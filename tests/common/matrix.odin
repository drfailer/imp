package common

import "cblas"
import "core:fmt"

Matrix :: struct {
    id: int,
    rows, cols, ld: uint,
    data: []f64,
}

matrix_init :: proc(m: ^Matrix, id: int, rows, cols: uint, allocator := context.allocator) {
    m.id = id
    m.rows = rows
    m.cols = cols
    m.ld = cols
    m.data = make([]f64, rows * cols, allocator)
}

matrix_destroy :: proc(m: ^Matrix) {
    delete(m.data)
}

dot :: proc(A, B, C: Matrix) {
    assert(C.rows == A.rows)
    assert(C.cols == B.rows)
    assert(A.cols == B.rows)
    M := C.rows
    N := C.cols
    K := A.cols
    cblas.dgemm(.NoTrans, .NoTrans, M, N, K, 1.0, A.data, A.ld, B.data, B.ld, 0,
                C.data, C.ld)
}

Matrix_Build_Kind :: enum { Zero, Int, Float }

matrix_build :: proc(m: ^Matrix, kind: Matrix_Build_Kind) {
    switch kind {
    case .Zero: for &data in m.data do data = 0
    case .Int:
        v := f64(1)
        for i in 0..<m.rows {
            for j in 0..<m.cols {
                m.data[i * m.cols + j] = v
                v += 1
            }
        }
    case .Float:
        for i in 0..<m.rows {
            for j in 0..<m.cols {
                m.data[i * m.cols + j] = 1 / f64(m.rows * m.cols)
            }
        }
    }
}

matrix_print :: proc(m: Matrix, name: string) {
    MAX_ROWS :: 6
    MAX_COLS :: 6

    rows := min(MAX_ROWS, m.rows)
    cols := min(MAX_COLS, m.cols)

    fmt.printfln("{} = ", name)
    for row in 0..<rows {
        for col in 0..<cols {
            fmt.printf("  {: 12.3f}", m.data[row * m.ld + col])
        }
        if m.cols > MAX_COLS do fmt.println("  ⋯")
        fmt.println()
    }
    if m.rows > MAX_ROWS {
        for col in 0..<cols {
            fmt.printf("          ⠇   ")
        }
        if m.cols > MAX_COLS do fmt.println("  ⋱")
    }
}

Matrix_Tile :: struct {
    using m: Matrix,
    row_idx, col_idx: uint,
}

matrix_tile_init_from_matrix :: proc(tile: ^Matrix_Tile, m: Matrix, row, col, row_idx, col_idx, rows, cols: uint) {
    tile.id = m.id
    tile.row_idx = row_idx
    tile.col_idx = col_idx
    tile.rows = rows
    tile.cols = cols
    tile.ld = m.ld
    tile.data = m.data[row * m.ld + col:]
}

matrix_tile_init_alloc :: proc(tile: ^Matrix_Tile, row_idx, col_idx, rows, cols: uint, allocator := context.allocator) {
    tile.row_idx = row_idx
    tile.col_idx = col_idx
    tile.rows = rows
    tile.cols = cols
    tile.ld = cols
    tile.data = make([]f64, rows * cols, allocator)
}

matrix_tile_init :: proc{
    matrix_tile_init_from_matrix,
    matrix_tile_init_alloc,
}

matrix_tile_destroy :: proc(tile: ^Matrix_Tile) {
    delete(tile.data)
}
