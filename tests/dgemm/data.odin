package dgemm

import "../../"
import "../../prof"
import "../common"
import "core:container/queue"
import "core:log"

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
    imp.pool_init(&data.tile_pools[0], data.TM * data.TK)
    imp.pool_init(&data.tile_pools[1], data.TK * data.TN)
    imp.pool_init(&data.tile_pools[2], data.TM * data.TN)

    imp.pool_init(&data.tile_pools[3], data.TM * data.TN * data.TK,
        proc(tile: ^Matrix_Tile, data: rawptr) {
            data := cast(^Dgemm_Data)data
            common.matrix_tile_init_alloc(tile, 0, 0, data.tile_cols, data.tile_rows)
        }, data)

    imp.type_comms_init(&data.comms.product_state)
    imp.type_comms_init(&data.comms.sum_state)
    imp.comm_init(&data.comms.product_task)
    imp.comm_init(&data.comms.sum_task)

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
    imp.comm_destroy(&data.comms.product_task)
    imp.comm_destroy(&data.comms.sum_task)

    log.destroy_console_logger(data.logger)

    for &q in data.sum_state.queues {
        queue.destroy(&q.ps)
    }
    delete(data.sum_state.queues)
    delete(data.product_state.a_tiles)
    delete(data.product_state.b_tiles)
}
