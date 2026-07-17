package imp

import "core:fmt"

exec :: proc(ctx: Parallel_Ctx, i: int) {
    fmt.printfln("[{}/{}]: i = {}", get_thread_index(ctx) + 1, get_thread_count(ctx), i)

    if branch(ctx, 2) {
        fmt.printfln("right_branch(1, 2)[{}/{}]", get_thread_index(ctx) + 1, get_thread_count(ctx))
    } else {
        fmt.printfln("left_branch(1, 2)[{}/{}]", get_thread_index(ctx) + 1, get_thread_count(ctx))
    }
    join(ctx)

    fmt.printfln("[{}/{}] done", get_thread_index(ctx) + 1, get_thread_count(ctx))
}

exec2 :: proc(ctx: Parallel_Ctx, i: int) {
    fmt.printfln("[{}/{}]: i = {}", get_thread_index(ctx) + 1, get_thread_count(ctx), i)

    if branch(ctx, 2) {
        fmt.printfln("right_branch(1, 2)[{}/{}]", get_thread_index(ctx) + 1, get_thread_count(ctx))
    } else {
        fmt.printfln("left_branch(1, 2)[{}/{}]", get_thread_index(ctx) + 1, get_thread_count(ctx))
        if branch(ctx, 1) {
            fmt.printfln("right_branch(3, 4)[{}/{}]", get_thread_index(ctx) + 1, get_thread_count(ctx))
        } else {
            fmt.printfln("left_branch(3, 4)[{}/{}]", get_thread_index(ctx) + 1, get_thread_count(ctx))
        }
        join(ctx)
    }
    join(ctx)

    fmt.printfln("[{}/{}] done", get_thread_index(ctx) + 1, get_thread_count(ctx))
}

main :: proc() {
    line: Parallel_Line
    parallel_line_init(&line, 4, exec2, 30)
    defer parallel_line_destroy(&line)
}
