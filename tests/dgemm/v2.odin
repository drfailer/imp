package dgemm

import "../../"
import "../../prof"
import "../common"
import "../common/cblas"
import "core:log"
import "core:container/queue"

@(private="file")
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
            tile := alloc_tile(data, thread_index)
            common.matrix_tile_init(tile, m, row, col, row / data.tile_rows, col / data.tile_cols,
                                    min(data.tile_rows, m.rows - row),
                                    min(data.tile_cols, m.cols - col))
            switch thread_index {
            case 0: imp.type_comm_send(&data.comm.product_state, cast(^Tile_A)tile)
            case 1: imp.type_comm_send(&data.comm.product_state, cast(^Tile_B)tile)
            case 2: imp.type_comm_send(&data.comm.sum_state, cast(^Tile_C)tile)
            }
        }
    }
    imp.barrier(ctx)
    if thread_index == 0 {
        imp.comm_set_closed(&data.comm.product_state)
    }
}

@(private="file")
product_state :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    prof.procedure()

    product := proc(ctx: imp.Ctx, data: ^Dgemm_Data, a, b: ^Matrix_Tile) {
        p := alloc_tile_with_data(data, 3)
        p.rows = a.rows
        p.cols = b.rows
        p.row_idx = a.row_idx
        p.col_idx = b.col_idx
        imp.assembly_line_put(&data.comm.tasks, Product_Data{a, b, p})
    }

    TM := data.TM
    TN := data.TN
    TK := data.TK

    for {
        prof.region("product_state_dequeue_compute")
        udata := imp.comm_recv(&data.comm.product_state) or_break

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

@(private="file")
sum_state :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    prof.procedure()

    TM := data.TM
    TN := data.TN
    TK := data.TK
    data.sum_state.progress_counter = TM * TN * TK

    for {
        prof.region("sum_state_dequeue_compute")
        udata := imp.comm_recv(&data.comm.sum_state) or_break

        prof.region("sum_state_compute")
        switch value in udata {
        case ^Tile_C:
            c := cast(^Matrix_Tile)value

            q := &data.sum_state.queues[c.row_idx * TN + c.col_idx]
            if p, ok := queue.pop_front_safe(&q.ps); ok {
                imp.assembly_line_put(&data.comm.tasks, Sum_Data{c = c, p = p})
            } else {
                q.c = c
            }
        case ^Tile_P:
            p := cast(^Matrix_Tile)value

            q := &data.sum_state.queues[p.row_idx * TN + p.col_idx]
            if q.c != nil {
                imp.assembly_line_put(&data.comm.tasks, Sum_Data{c = q.c, p = p})
                q.c = nil
            } else {
                queue.enqueue(&q.ps, p)
            }
        case Sum_Data:
            free_tile_with_data(data, 3, value.p)

            q := &data.sum_state.queues[value.c.row_idx * TN + value.c.col_idx]
            if p, ok := queue.pop_front_safe(&q.ps); ok {
                imp.assembly_line_put(&data.comm.tasks, Sum_Data{c = value.c, p = p})
            } else {
                q.c = value.c
            }

            data.sum_state.progress_counter -= 1
            if data.sum_state.progress_counter == 0 {
                terminate(ctx, data)
                return
            }
        }
    }
}

@(private="file")
compute_sum_task :: #force_inline proc(#no_alias c_ptr: [^]f64, #no_alias p_ptr: [^]f64, rows, cols, c_ld, p_ld: uint) {
    #no_bounds_check for row in 0..<rows {
        cr := c_ptr[row * c_ld:]
        pr := p_ptr[row * p_ld:]
        for col in 0..<cols {
            cr[col] += pr[col]
        }
    }
}

@(private="file")
tasks_process :: proc(data: ^Dgemm_Data, udata: union {Product_Data, Sum_Data}) {
    switch tiles in udata {
    case Product_Data:
        prof.region("product_task")
        common.dot(tiles.a, tiles.b, tiles.p)
        imp.type_comm_send(&data.comm.sum_state, cast(^Tile_P)tiles.p)
    case Sum_Data:
        prof.region("sum_task")
        c := tiles.c
        p := tiles.p
        compute_sum_task(c.data, p.data, c.rows, c.cols, c.ld, p.ld)
        imp.type_comm_send(&data.comm.sum_state, tiles)
        // free_tile_with_data(data, 3, tiles.p)
    }
}

@(private="file")
tasks :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    prof.procedure()

    for {
        prof.region("tasks_dequeue_compute")
        udata := imp.assembly_line_get(&data.comm.tasks) or_break
        tasks_process(data, udata)
    }
}

//
// close all communicators which will make the task leave when the queues are empty
//
@(private="file")
terminate :: proc(ctx: imp.Ctx, data: ^Dgemm_Data) {
    prof.procedure()
    imp.comm_set_closed(&data.comm.sum_state)
    imp.assembly_line_set_stop(&data.comm.tasks)
    when !USE_TILE_POOL {
        for &q in data.sum_state.queues {
            free_tile(data, 2, q.c)
        }
        for a in data.product_state.a_tiles {
            free_tile(data, 0, a)
        }
        for b in data.product_state.b_tiles {
            free_tile(data, 1, b)
        }
    }
}

@(private="file")
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

dgemm_v2 :: proc(A, B, C: Matrix, tile_rows, tile_cols: uint) {
    data: Dgemm_Data
    dgemm_data_init(&data, A, B, C, tile_rows, tile_cols)
    defer dgemm_data_destroy(&data)

    context.logger = data.logger

    global_ctx: imp.Global_Ctx
    imp.global_ctx_init(&global_ctx, 40)
    defer imp.global_ctx_destroy(&global_ctx)
    imp.launch(&global_ctx, dgemm_parallel, &data)
}
