package dgemm

import "../../"
import "../common"
import "../common/cblas"
import "core:mem"
import "core:log"
import "core:container/queue"
import "core:fmt"
import "core:time"

Matrix :: common.Matrix
Matrix_Tile :: common.Matrix_Tile

Tile_A :: distinct Matrix_Tile
Tile_B :: distinct Matrix_Tile
Tile_C :: distinct Matrix_Tile
Tile_P :: distinct Matrix_Tile

Product_Data :: struct {
    a, b, p: ^Matrix_Tile,
}

Sum_Data :: struct {
    c, p: ^Matrix_Tile,
}

Sum_Queue :: struct {
    c: ^Matrix_Tile,
    ps: queue.Queue(^Matrix_Tile),
}

Dgemm_Data :: struct {
    logger: log.Logger,
    A, B, C: Matrix,
    TM, TN, TK: uint,
    tile_pools: [4]imp.Pool(Matrix_Tile),
    tile_cols, tile_rows: uint,
    product_state: struct {
        a_tiles: [dynamic]^Matrix_Tile,
        b_tiles: [dynamic]^Matrix_Tile,
    },
    sum_state: struct {
        queues: [dynamic]Sum_Queue,
        progress_counter: uint,
    },
    comms: struct {
        product_state: imp.Comms(union { ^Tile_A, ^Tile_B }),
        sum_state: imp.Comms(union { ^Tile_C, Product_Data, Sum_Data }),
        product_task: imp.Comm(Product_Data),
        sum_task: imp.Comm(Sum_Data),
    }
}

init_p_tile :: proc(tile: ^Matrix_Tile, data: rawptr, allocator: mem.Allocator) {
    data := cast(^Dgemm_Data)data
    common.matrix_tile_init_alloc(tile, 0, 0, data.tile_cols, data.tile_rows, allocator)
}

dgemm_data_init :: proc(data: ^Dgemm_Data, A, B, C: Matrix, tile_cols, tile_rows: uint) {
    data.logger = log.create_console_logger(.Error, {.Level, .Short_File_Path, .Line, .Procedure, .Terminal_Color, .Thread_Id})
    data.TM = C.rows / tile_rows + (C.rows % tile_rows == 0 ? 0 : 1)
    data.TN = C.cols / tile_cols + (C.cols % tile_cols == 0 ? 0 : 1)
    data.TK = A.cols / tile_cols + (A.cols % tile_cols == 0 ? 0 : 1)
    data.A = A
    data.B = B
    data.C = C
    data.tile_cols = tile_cols
    data.tile_rows = tile_rows
    imp.pool_init(&data.tile_pools[0], data.TM * data.TK)
    imp.pool_init(&data.tile_pools[1], data.TK * data.TN)
    imp.pool_init(&data.tile_pools[2], data.TM * data.TM)
    imp.pool_init(&data.tile_pools[3], 2000, init_p_tile, data)
    imp.comms_init(&data.comms.product_state)
    imp.comms_init(&data.comms.sum_state)
    imp.comm_init(&data.comms.product_task)
    imp.comm_init(&data.comms.sum_task)

    data.product_state.a_tiles = make([dynamic]^Matrix_Tile, data.TM * data.TK)
    data.product_state.b_tiles = make([dynamic]^Matrix_Tile, data.TK * data.TN)
    data.sum_state.queues = make([dynamic]Sum_Queue, data.TM * data.TN)
}

dgemm_data_destroy :: proc(data: ^Dgemm_Data) {
    imp.pool_destroy(&data.tile_pools[0])
    imp.pool_destroy(&data.tile_pools[1])
    imp.pool_destroy(&data.tile_pools[2])
    imp.pool_destroy(&data.tile_pools[3])
    imp.comms_destroy(&data.comms.product_state)
    imp.comms_destroy(&data.comms.sum_state)
    imp.comm_destroy(&data.comms.product_task)
    imp.comm_destroy(&data.comms.sum_task)
    log.destroy_console_logger(data.logger)

    delete(data.sum_state.queues)
    delete(data.product_state.a_tiles)
    delete(data.product_state.b_tiles)
}

split_task :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    imp.prof_procedure(ctx)
    thread_index := imp.get_thread_index(ctx)

    m: Matrix
    switch thread_index {
    case 0: m = data.A
    case 1: m = data.B
    case 2: m = data.C
    }

    for row : uint = 0; row < m.rows; row += data.tile_rows {
        for col : uint = 0; col < m.cols; col += data.tile_cols {
            tile, ok := imp.pool_alloc(&data.tile_pools[thread_index])
            assert(ok)
            common.matrix_tile_init(tile, m, row, col, row / data.tile_rows, col / data.tile_cols,
                                    min(data.tile_rows, m.rows - row),
                                    min(data.tile_cols, m.cols - col))
            switch thread_index {
            case 0: imp.comms_send(&data.comms.product_state, cast(^Tile_A)tile)
            case 1: imp.comms_send(&data.comms.product_state, cast(^Tile_B)tile)
            case 2: imp.comms_send(&data.comms.sum_state, cast(^Tile_C)tile)
            }
        }
    }
    imp.barrier(ctx)
    if thread_index == 0 {
        imp.comms_set_closed(&data.comms.product_state)
    }
}

product_state :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    imp.prof_procedure(ctx)

    product := proc(ctx: imp.Ctx, data: ^Dgemm_Data, a, b: ^Matrix_Tile) {
        p, ok := imp.pool_alloc(&data.tile_pools[3], .Wait)
        assert(ok)
        p.rows = a.rows
        p.cols = b.rows
        p.row_idx = a.row_idx
        p.col_idx = b.col_idx
        imp.comm_send(&data.comms.product_task, Product_Data{a, b, p})
    }

    TM := data.TM
    TN := data.TN
    TK := data.TK

    for {
        imp.prof_region(ctx, "product_state_dequeue_compute")
        udata := imp.comms_recv(&data.comms.product_state) or_break

        imp.prof_region(ctx, "product_state_compute")
        switch value in udata {
        case ^Tile_A:
            a := cast(^Matrix_Tile)value
            log.debugf("product_state: A[{},{}]", a.row_idx, a.col_idx)
            assert(data.product_state.a_tiles[a.row_idx * TK + a.col_idx] == nil)
            data.product_state.a_tiles[a.row_idx * TK + a.col_idx] = a
            for col in 0..<TN {
                b := data.product_state.b_tiles[a.col_idx * TN + col]
                if b != nil do product(ctx, data, a, b)
            }
        case ^Tile_B:
            b := cast(^Matrix_Tile)value
            log.debugf("product_state: B[{},{}]", b.row_idx, b.col_idx)
            assert(data.product_state.b_tiles[b.row_idx * TN + b.col_idx] == nil)
            data.product_state.b_tiles[b.row_idx * TN + b.col_idx] = b
            for row in 0..<TM {
                a := data.product_state.a_tiles[row * TK + b.row_idx]
                if a != nil do product(ctx, data, a, b)
            }
        }
    }
}

product_task :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    imp.prof_procedure(ctx)

    for {
        imp.prof_region(ctx, "product_task_dequeue_compute")
        tiles := imp.comm_recv(&data.comms.product_task) or_break
        imp.prof_region(ctx, "product_task_compute")
        common.dot(tiles.a, tiles.b, tiles.p)
        imp.comms_send(&data.comms.sum_state, tiles)
    }
}

sum_state :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    imp.prof_procedure(ctx)

    TM := data.TM
    TN := data.TN
    TK := data.TK

    if imp.single(ctx) {
        data.sum_state.progress_counter = TM * TN * TK
    }
    imp.barrier(ctx)

    for {
        imp.prof_region(ctx, "sum_state_dequeue_compute")
        udata := imp.comms_recv(&data.comms.sum_state) or_break

        imp.prof_region(ctx, "sum_state_compute")
        switch value in udata {
        case ^Tile_C:
            c := cast(^Matrix_Tile)value

            log.debugf("sum_state: C[{},{}]", c.row_idx, c.col_idx)

            q := &data.sum_state.queues[c.row_idx * TN + c.col_idx]
            if p, ok := queue.pop_front_safe(&q.ps); ok {
                imp.comm_send(&data.comms.sum_task, Sum_Data{c = c, p = p})
            } else {
                q.c = c
            }
        case Product_Data:
            p := value.p

            log.debugf("sum_state: Product_Data(A[{},{}], B[{},{}], P[{},{}])",
                value.a.row_idx, value.a.col_idx,
                value.b.row_idx, value.b.col_idx,
                value.p.row_idx, value.p.col_idx)

            q := &data.sum_state.queues[p.row_idx * TN + p.col_idx]
            if q.c != nil {
                imp.comm_send(&data.comms.sum_task, Sum_Data{c = q.c, p = p})
            } else {
                queue.enqueue(&q.ps, p)
            }
        case Sum_Data:
            log.debugf("sum_state: Sum_Data(P[{},{}], C[{},{}]) (progress = {})",
                value.p.row_idx, value.p.col_idx,
                value.c.row_idx, value.c.col_idx,
                data.sum_state.progress_counter - 1)

            data.sum_state.progress_counter -= 1
            if data.sum_state.progress_counter == 0 {
                terminate(ctx, data)
                return
            }

            imp.pool_release(&data.tile_pools[3], value.p)

            q := &data.sum_state.queues[value.c.row_idx * TN + value.c.col_idx]
            if p, ok := queue.pop_front_safe(&q.ps); ok {
                imp.comm_send(&data.comms.sum_task, Sum_Data{c = value.c, p = p})
            } else {
                q.c = value.c
            }
        }
    }
}

sum_task :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    imp.prof_procedure(ctx)

    for {
        imp.prof_region(ctx, "sum_task_dequeue_compute")
        tiles := imp.comm_recv(&data.comms.sum_task) or_break
        log.debugf("sum_task: C[{},{}]", tiles.c.row_idx, tiles.c.col_idx)

        if imp.prof_region(ctx, "sum_task_compute") {
            c := tiles.c
            p := tiles.p
            for row in 0..<tiles.c.rows {
                for col in 0..<tiles.c.cols {
                    c.data[row * c.ld + col] += p.data[row * p.ld + col]
                }
            }
            if imp.prof_region(ctx, "sum_task_send") {
                imp.comms_send(&data.comms.sum_state, tiles)
            }
        }
    }
}

//
// close all communicators which will make the task leave when the queues are empty
//
terminate :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    fmt.println("terminate")
    imp.comms_set_closed(&data.comms.sum_state)
    imp.comm_set_closed(&data.comms.product_task)
    imp.comm_set_closed(&data.comms.sum_task)
}

dgemm_parallel :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    imp.prof_procedure(ctx)
    ensure(imp.get_thread_count(ctx) > (3 + 1 + 1 + 40))
    context.logger = data.logger

    run_sum_task := true

    local_ctx := imp.get_local_ctx(ctx)
    if imp.branch(ctx, 3) {
        split_task(ctx, data)
    } else if imp.branch(ctx, 1) {
        product_state(ctx, data)
    } else if imp.branch(ctx, 1) {
        run_sum_task = false
        sum_state(ctx, data)
    } else if imp.branch(ctx, 40) {
        run_sum_task = false
        product_task(ctx, data)
    }
    imp.join_to(ctx, local_ctx)
    if run_sum_task do sum_task(ctx, data)
}

dgemm :: proc(A, B, C: Matrix, tile_rows, tile_cols: uint) {
    data: Dgemm_Data
    dgemm_data_init(&data, A, B, C, tile_rows, tile_cols)
    defer dgemm_data_destroy(&data)

    context.logger = data.logger

    global_ctx: imp.Global_Ctx
    imp.global_ctx_init(&global_ctx, 55)
    defer imp.global_ctx_destroy(&global_ctx)
    imp.launch(&global_ctx, dgemm_parallel, &data)

    imp.prof_print_report_dot(global_ctx, "dgemm.dot")
}

commpare_matrices :: proc(R, E: Matrix, precision := 1e-8) {
    Data :: struct { R, E: Matrix, precision: f64, result: bool }
    data := Data{ R, E, precision, true }

    comp :: proc(ctx: imp.Ctx, data: ^Data) {
        assert(data.E.rows == data.R.rows || data.E.cols == data.R.cols)

        results: [dynamic]bool
        if imp.single(ctx, 0) {
            results = make([dynamic]bool, imp.get_thread_count(ctx))
            for &r in results do r = true
        }
        imp.sync_val(ctx, 0, &results)

        for r := imp.range_init(ctx, len(data.E.data)); imp.range_continue(r); r = imp.range_next(r) {
            e := data.E.data[r.it]
            el := e - data.precision
            er := e + data.precision
            res := data.R.data[r.it]
            if !(el <= res && res <= er) {
                fmt.printfln("[{}]: diff at [{},{}], {} not_in [{}, {}]",
                             imp.get_thread_index(ctx), uint(r.it) / data.E.cols, uint(r.it) % data.E.cols,
                             res, el, er)
                results[imp.get_thread_index(ctx)] = false
                break
            }
        }
        imp.barrier(ctx)

        if imp.single(ctx, 0) {
            data.result = true
            for result in results {
                data.result &= result
            }
            delete(results)
        }
    }

    global_ctx: imp.Global_Ctx
    imp.global_ctx_init(&global_ctx, 10)
    defer imp.global_ctx_destroy(&global_ctx)
    imp.launch(&global_ctx, comp, &data)

    if data.result == true {
        fmt.println("[SUCCESS]: matricies equal")
    } else {
        fmt.println("[FAIL]: matricies not equal")
    }
}

main :: proc() {
    MATRIX_SIZE :: 20000
    TILE_SIZE :: 1024
    A, B, C, E: Matrix
    common.matrix_init(&A, 0, MATRIX_SIZE, MATRIX_SIZE)
    defer common.matrix_destroy(&A)
    common.matrix_init(&B, 1, MATRIX_SIZE, MATRIX_SIZE)
    defer common.matrix_destroy(&B)
    common.matrix_init(&C, 2, MATRIX_SIZE, MATRIX_SIZE)
    defer common.matrix_destroy(&C)
    // common.matrix_init(&E, 3, MATRIX_SIZE, MATRIX_SIZE)
    // defer common.matrix_destroy(&E)

    common.matrix_build(&A, .Float)
    common.matrix_build(&B, .Float)


    fmt.printfln("MATRIX_SIZE = {}, TILE_SIZE = {}", MATRIX_SIZE, TILE_SIZE)

    // {
    //     sw: time.Stopwatch
    //     time.stopwatch_start(&sw)
    //     common.dot(A, B, E)
    //     time.stopwatch_stop(&sw)
    //     fmt.printfln("bcblas: {}", time.stopwatch_duration(sw))
    // }

    cblas.openblas_set_num_threads(1)

    {
        sw: time.Stopwatch
        time.stopwatch_start(&sw)
        dgemm(A, B, C, TILE_SIZE, TILE_SIZE)
        time.stopwatch_stop(&sw)
        fmt.printfln("dgemm: {}", time.stopwatch_duration(sw))
    }

    // commpare_matrices(C, E)

    if  MATRIX_SIZE < 16 {
        common.matrix_print(A, "A")
        common.matrix_print(B, "B")
        common.matrix_print(C, "C")
        common.matrix_print(E, "E")
    }
}
