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

    // compute ground true
    common.dot(A, B, E)
    cblas.openblas_set_num_threads(1)

    // v1
    dgemm_v1(A, B, C, TILE_SIZE, TILE_SIZE)
    testing.expect(t, commpare_matrices(C, E))
    common.matrix_build(&C, .Zero)

    // v2
    dgemm_v2(A, B, C, TILE_SIZE, TILE_SIZE)
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
        dgemm_v1(A, B, C, TILE_SIZE, TILE_SIZE)
    }


    prof.report({"dgemm"})

    if  MATRIX_SIZE < 16 {
        common.matrix_print(A, "A")
        common.matrix_print(B, "B")
        common.matrix_print(C, "C")
        common.matrix_print(E, "E")
    }
}
