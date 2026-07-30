package dgemm

import "../../"
import "../../prof"
import "../common"
import "core:container/queue"
import "core:log"
import "base:runtime"

USE_TILE_POOL :: #config(USE_TILE_POOL, true)

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
    comm: struct {
        product_state: imp.Comm(union { ^Tile_A, ^Tile_B }),
        sum_state: imp.Comm(union { Sum_Data, ^Tile_P, ^Tile_C }),
        product_task: imp.Comm(Product_Data),
        sum_task: imp.Comm(Sum_Data),
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
    when USE_TILE_POOL {
        imp.pool_init(&data.tile_pools[0], data.TM * data.TK)
        imp.pool_init(&data.tile_pools[1], data.TK * data.TN)
        imp.pool_init(&data.tile_pools[2], data.TM * data.TN)
        imp.pool_init(&data.tile_pools[3], data.TM * data.TN,
            proc(tile: ^Matrix_Tile, data: rawptr) {
                data := cast(^Dgemm_Data)data
                common.matrix_tile_init_alloc(tile, 0, 0, data.tile_cols, data.tile_rows)
            },
            proc(tile: ^Matrix_Tile, data: rawptr) {
                common.matrix_tile_destroy(tile)
            },
            data,
            add_allocator = runtime.heap_allocator())
    }

    imp.type_comm_init(&data.comm.product_state)
    imp.type_comm_init(&data.comm.sum_state)
    imp.comm_init(&data.comm.product_task)
    imp.comm_init(&data.comm.sum_task)

    imp.assembly_line_init(&data.comm.tasks)

    data.product_state.a_tiles = make([dynamic]^Matrix_Tile, data.TM * data.TK)
    data.product_state.b_tiles = make([dynamic]^Matrix_Tile, data.TK * data.TN)
    data.sum_state.queues = make([dynamic]Sum_Queue, data.TM * data.TN)
    for &q in data.sum_state.queues {
        queue.init(&q.ps, int(data.TK))
    }
}

dgemm_data_destroy :: proc(data: ^Dgemm_Data) {
    when USE_TILE_POOL {
        imp.pool_destroy(&data.tile_pools[0])
        imp.pool_destroy(&data.tile_pools[1])
        imp.pool_destroy(&data.tile_pools[2])
        imp.pool_destroy(&data.tile_pools[3])
    }
    imp.comm_destroy(&data.comm.product_state)
    imp.comm_destroy(&data.comm.sum_state)
    imp.comm_destroy(&data.comm.product_task)
    imp.comm_destroy(&data.comm.sum_task)

    log.destroy_console_logger(data.logger)

    for &q in data.sum_state.queues {
        queue.destroy(&q.ps)
    }
    delete(data.sum_state.queues)
    delete(data.product_state.a_tiles)
    delete(data.product_state.b_tiles)
}

when USE_TILE_POOL {

alloc_tile_with_data :: proc(data: ^Dgemm_Data, index: int) -> ^Matrix_Tile {
    tile, ok := imp.pool_alloc(&data.tile_pools[index], .Wait)
    assert(ok)
    return tile
}

alloc_tile :: proc(data: ^Dgemm_Data, index: int) -> ^Matrix_Tile {
    tile, ok := imp.pool_alloc(&data.tile_pools[index])
    assert(ok)
    return tile
}

free_tile :: proc(data: ^Dgemm_Data, index: int, tile: ^Matrix_Tile, loc := #caller_location) {
    imp.pool_release(&data.tile_pools[index], tile, loc = loc)
}

free_tile_with_data :: proc(data: ^Dgemm_Data, index: int, tile: ^Matrix_Tile, loc := #caller_location) {
    imp.pool_release(&data.tile_pools[index], tile, loc = loc)
}

} else {

alloc_tile_with_data :: proc(data: ^Dgemm_Data, index: int) -> ^Matrix_Tile {
    tile := new(Matrix_Tile)
    common.matrix_tile_init_alloc(tile, 0, 0, data.tile_cols, data.tile_rows)
    return tile
}

alloc_tile :: proc(data: ^Dgemm_Data, index: int, loc := #caller_location) -> ^Matrix_Tile {
    return new(Matrix_Tile, loc = loc)
}

free_tile :: proc(data: ^Dgemm_Data, index: int, tile: ^Matrix_Tile) {
    free(tile)
}

free_tile_with_data :: proc(data: ^Dgemm_Data, index: int, tile: ^Matrix_Tile) {
    free(tile.data)
    free(tile)
}

}
