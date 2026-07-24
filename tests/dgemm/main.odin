package dgemm

import "../../"
import "../../prof"
import "../common"
import "../common/cblas"
import "core:mem"
import "core:log"
import "core:container/queue"
import "core:fmt"
import "core:testing"

Matrix :: common.Matrix
Matrix_Tile :: common.Matrix_Tile

Tile_A :: distinct Matrix_Tile
Tile_B :: distinct Matrix_Tile
Tile_C :: distinct Matrix_Tile
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
        tasks: imp.Assembly_Line(union { Product_Data, Sum_Data }, 1024),
    }
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
    imp.pool_init(&data.tile_pools[2], data.TM * data.TN)

    imp.pool_init(&data.tile_pools[3], data.TM * data.TN,
        proc(tile: ^Matrix_Tile, data: rawptr) {
            data := cast(^Dgemm_Data)data
            common.matrix_tile_init_alloc(tile, 0, 0, data.tile_cols, data.tile_rows)
        }, data)

    imp.type_comms_init(&data.comms.product_state)
    imp.type_comms_init(&data.comms.sum_state)

    imp.assembly_line_init(&data.comms.tasks)

    data.product_state.a_tiles = make([dynamic]^Matrix_Tile, data.TM * data.TK)
    data.product_state.b_tiles = make([dynamic]^Matrix_Tile, data.TK * data.TN)
    data.sum_state.queues = make([dynamic]Sum_Queue, data.TM * data.TN)
}

dgemm_data_destroy :: proc(data: ^Dgemm_Data) {
    imp.pool_destroy(&data.tile_pools[0])
    imp.pool_destroy(&data.tile_pools[1])
    imp.pool_destroy(&data.tile_pools[2])
    imp.pool_destroy_with_item_destroy(&data.tile_pools[3], proc(tile: ^Matrix_Tile, data: rawptr) {
        common.matrix_tile_destroy(tile)
    })
    imp.comms_destroy(&data.comms.product_state)
    imp.comms_destroy(&data.comms.sum_state)

    log.destroy_console_logger(data.logger)

    for &q in data.sum_state.queues {
        queue.destroy(&q.ps)
    }
    delete(data.sum_state.queues)
    delete(data.product_state.a_tiles)
    delete(data.product_state.b_tiles)
}

split_task :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    prof.procedure()
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
            case 0: imp.type_comms_send(&data.comms.product_state, cast(^Tile_A)tile)
            case 1: imp.type_comms_send(&data.comms.product_state, cast(^Tile_B)tile)
            case 2: imp.type_comms_send(&data.comms.sum_state, cast(^Tile_C)tile)
            }
        }
    }
    imp.barrier(ctx)
    if thread_index == 0 {
        imp.comms_set_closed(&data.comms.product_state)
    }
}

product_state :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    prof.procedure()

    product := proc(ctx: imp.Ctx, data: ^Dgemm_Data, a, b: ^Matrix_Tile) {
        // wait version
        p, ok := imp.pool_alloc(&data.tile_pools[3], .Wait)
        // busy wait version
        // p: ^Matrix_Tile
        // ok: bool
        // for {
        //     p, ok = imp.pool_alloc(&data.tile_pools[3], .Fail)
        //     if ok do break
        //     tiles := imp.assembly_line_get(&data.comms.tasks) or_continue
        //     tasks_process(data, tiles)
        // }
        p.rows = a.rows
        p.cols = b.rows
        p.row_idx = a.row_idx
        p.col_idx = b.col_idx
        imp.assembly_line_put(&data.comms.tasks, Product_Data{a, b, p})
    }

    TM := data.TM
    TN := data.TN
    TK := data.TK

    for {
        prof.region("product_state_dequeue_compute")
        udata := imp.comms_recv(&data.comms.product_state) or_break

        prof.region("product_state_compute")
        #no_bounds_check switch value in udata {
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

sum_state :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    prof.procedure()

    TM := data.TM
    TN := data.TN
    TK := data.TK

    if imp.single(ctx) {
        data.sum_state.progress_counter = TM * TN * TK
    }
    imp.barrier(ctx)

    for {
        prof.region("sum_state_dequeue_compute")
        udata := imp.comms_recv(&data.comms.sum_state) or_break

        prof.region("sum_state_compute")
        switch value in udata {
        case ^Tile_C:
            c := cast(^Matrix_Tile)value

            log.debugf("sum_state: C[{},{}]", c.row_idx, c.col_idx)

            q := &data.sum_state.queues[c.row_idx * TN + c.col_idx]
            if p, ok := queue.pop_front_safe(&q.ps); ok {
                imp.assembly_line_put(&data.comms.tasks, Sum_Data{c = c, p = p})
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
                imp.assembly_line_put(&data.comms.tasks, Sum_Data{c = q.c, p = p})
                q.c = nil
            } else {
                queue.enqueue(&q.ps, p)
            }
        case Sum_Data:
            log.debugf("sum_state: Sum_Data(P[{},{}], C[{},{}]) (progress = {})",
                value.p.row_idx, value.p.col_idx,
                value.c.row_idx, value.c.col_idx,
                data.sum_state.progress_counter - 1)

            imp.pool_release(&data.tile_pools[3], value.p)

            data.sum_state.progress_counter -= 1
            if data.sum_state.progress_counter == 0 {
                terminate(ctx, data)
                return
            }

            q := &data.sum_state.queues[value.c.row_idx * TN + value.c.col_idx]
            if p, ok := queue.pop_front_safe(&q.ps); ok {
                imp.assembly_line_put(&data.comms.tasks, Sum_Data{c = value.c, p = p})
            } else {
                q.c = value.c
            }
        }
    }
}

tasks_process :: proc(data: ^Dgemm_Data, udata: union {Product_Data, Sum_Data}) {
    switch tiles in udata {
    case Product_Data:
        prof.region("product_task")
        common.dot(tiles.a, tiles.b, tiles.p)
        imp.type_comms_send(&data.comms.sum_state, tiles)
    case Sum_Data:
        prof.region("sum_task")
        c := tiles.c
        p := tiles.p

        c_ptr := c.data
        p_ptr := p.data
        c_ld := c.ld
        p_ld := p.ld
        rows := tiles.c.rows
        cols := tiles.c.cols
        #no_bounds_check for row in 0..<rows {
            cr := c_ptr[row * c_ld:]
            pr := p_ptr[row * p_ld:]
            for col in 0..<cols {
                cr[col] += pr[col]
            }
        }
        imp.type_comms_send(&data.comms.sum_state, tiles)
    }
}

tasks :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    prof.procedure()

    for {
        prof.region("tasks_dequeue_compute")
        udata := imp.assembly_line_get(&data.comms.tasks) or_break
        tasks_process(data, udata)
    }
}

//
// close all communicators which will make the task leave when the queues are empty
//
terminate :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    fmt.println("terminate")
    imp.comms_set_closed(&data.comms.sum_state)
    imp.assembly_line_set_stop(&data.comms.tasks)
}

dgemm_parallel :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    prof.procedure()
    context.logger = data.logger

    local_ctx := imp.get_local_ctx(ctx)
    if imp.branch(ctx, 3) {
        split_task(ctx, data)
    } else if imp.branch(ctx, 1) {
        product_state(ctx, data)
    } else if imp.branch(ctx, 1) {
        sum_state(ctx, data)
    }
    imp.join_to(ctx, local_ctx)
    tasks(ctx, data)
}

dgemm :: proc(A, B, C: Matrix, tile_rows, tile_cols: uint) {
    data: Dgemm_Data
    dgemm_data_init(&data, A, B, C, tile_rows, tile_cols)
    defer dgemm_data_destroy(&data)

    context.logger = data.logger

    global_ctx: imp.Global_Ctx
    imp.global_ctx_init(&global_ctx, 40)
    defer imp.global_ctx_destroy(&global_ctx)
    imp.launch(&global_ctx, dgemm_parallel, &data)
}

commpare_matrices :: proc(R, E: Matrix, precision := 1e-8) -> bool {
    Data :: struct { R, E: Matrix, precision: f64, result: bool }
    data := Data{ R, E, precision, true }

    comp :: proc(ctx: imp.Ctx, data: ^Data) {
        assert(data.E.rows == data.R.rows && data.E.cols == data.R.cols)

        results: [dynamic]bool
        if imp.single(ctx, 0) {
            results = make([dynamic]bool, imp.get_thread_count(ctx))
            for &r in results do r = true
        }
        imp.sync_val(ctx, 0, &results)

        for r := imp.range_init(ctx, int(data.E.size)); imp.range_continue(r); r = imp.range_next(r) {
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

        result := imp.reduce_builtins(ctx, results[:], .And)
        if imp.single(ctx, 0) {
            data.result = result
            delete(results)
        }
    }

    global_ctx: imp.Global_Ctx
    imp.global_ctx_init(&global_ctx, 10)
    defer imp.global_ctx_destroy(&global_ctx)
    imp.launch(&global_ctx, comp, &data)
    return data.result
}

@(test)
test :: proc(t: ^testing.T) {
    MATRIX_SIZE :: 512
    TILE_SIZE :: 32
    A, B, C, E: Matrix
    common.matrix_init(&A, 0, MATRIX_SIZE, MATRIX_SIZE)
    defer common.matrix_destroy(&A)
    common.matrix_init(&B, 1, MATRIX_SIZE, MATRIX_SIZE)
    defer common.matrix_destroy(&B)
    common.matrix_init(&C, 2, MATRIX_SIZE, MATRIX_SIZE)
    defer common.matrix_destroy(&C)
    common.matrix_init(&E, 3, MATRIX_SIZE, MATRIX_SIZE)
    defer common.matrix_destroy(&E)

    common.matrix_build(&A, .Int)
    common.matrix_build(&B, .Int)

    common.dot(A, B, E)
    cblas.openblas_set_num_threads(1)
    dgemm(A, B, C, TILE_SIZE, TILE_SIZE)
    testing.expect(t, commpare_matrices(C, E))
}

main :: proc() {
    prof.init()
    defer {
        prof.print_report_to_file("dgemm.dot", .Dot)
        prof.fini()
    }

    MATRIX_SIZE :: 20000
    TILE_SIZE :: 2048
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

    prof.reset()

    // if prof.region("cblas") {
    //     common.dot(A, B, E)
    // }

    cblas.openblas_set_num_threads(1)

    if prof.region("dgemm") {
        dgemm(A, B, C, TILE_SIZE, TILE_SIZE)
    }

    // if commpare_matrices(C, E) == true {
    //     fmt.println("[SUCCESS]: matricies equal")
    // } else {
    //     fmt.println("[FAIL]: matricies not equal")
    // }

    prof.report({"dgemm"})

    if  MATRIX_SIZE < 16 {
        common.matrix_print(A, "A")
        common.matrix_print(B, "B")
        common.matrix_print(C, "C")
        common.matrix_print(E, "E")
    }
}
