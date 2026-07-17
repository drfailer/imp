package playground

import "core:fmt"
import "../../"

exec_branch :: proc(ctx: imp.Ctx, i: int) {
    fmt.printfln("[{}/{}]: i = {}", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), i)

    if imp.branch(ctx, 2) {
        fmt.printfln("branch0(1, 2)[{}/{}]", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
    } else {
        fmt.printfln("branch1(1, 2)[{}/{}]", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
    }
    imp.join(ctx)

    fmt.printfln("[{}/{}] done", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
}

exec_nested_branches :: proc(ctx: imp.Ctx, i: int) {
    fmt.printfln("[{}/{}]: i = {}", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), i)

    if imp.branch(ctx, 2) {
        fmt.printfln("branch0(1, 2)[{}/{}]", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
    } else {
        fmt.printfln("branch1(1, 2)[{}/{}]", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
        if imp.branch(ctx, 1) {
            fmt.printfln("branch0(3, 4)[{}/{}]", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
        } else {
            fmt.printfln("branch1(3, 4)[{}/{}]", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
        }
        imp.join(ctx)
    }
    imp.join(ctx)

    fmt.printfln("[{}/{}] done", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
}

exec_messages :: proc(ctx: imp.Ctx, i: int) {
    ensure(imp.get_thread_count(ctx) == 4)
    buf0: [2][100]u8
    buf1: [2][100]u8
    fmt.printfln("[{}/{}]: i = {}", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), i)

    branch_ctx: imp.Branch_Ctx
    if imp.branch(ctx, 2, &branch_ctx) {
        ensure(imp.get_thread_count(ctx) == 2)

        text := fmt.bprintf(buf0[imp.get_thread_index(ctx)][:], "imp.branch 1 thread {}", imp.get_thread_index(ctx))
        imp.send_data(ctx, 1 - imp.get_thread_index(ctx), imp.make_data(&text))
        imp.send_data(ctx, branch_ctx[1], imp.get_thread_index(ctx), imp.make_data(&text))

        for i in 0..<2 {
            data, sender := imp.recv_data(ctx)
            if sender >= 0 {
                fmt.printfln("branch0[{}/{}]: {} (local from {})",
                    imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx),
                    imp.data_ptr(data, string)^, sender)
            } else {
                fmt.printfln("branch0[{}/{}]: {} (remote from {})",
                    imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx),
                    imp.data_ptr(data, string)^, ~sender)
                imp.send_data(ctx, sender, imp.make_data(&text))
            }
        }
        data, sender := imp.recv_data(ctx)
        fmt.printfln("branch0[{}/{}]: {} (global from {})",
            imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx),
            imp.data_ptr(data, string)^, ~sender)
        imp.barrier(ctx)
    } else {
        ensure(imp.get_thread_count(ctx) == 2)

        text := fmt.bprintf(buf1[imp.get_thread_index(ctx)][:], "imp.branch 2 thread {}", imp.get_thread_index(ctx))
        imp.send_data(ctx, 1 - imp.get_thread_index(ctx), imp.make_data(&text))
        imp.send_data(ctx, branch_ctx[0], imp.get_thread_index(ctx), imp.make_data(&text))

        for _ in 0..<2 {
            data, sender := imp.recv_data(ctx)
            if sender >= 0 {
                fmt.printfln("branch1[{}/{}]: {} (local from {})",
                    imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx),
                    imp.data_ptr(data, string)^, sender)
            } else {
                fmt.printfln("branch1[{}/{}]: {} (remote from {})",
                    imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx),
                    imp.data_ptr(data, string)^, ~sender)
                imp.send_data(ctx, sender, imp.make_data(&text))
            }
        }
        data, sender := imp.recv_data(ctx)
        fmt.printfln("branch1[{}/{}]: {} (global from {})",
            imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx),
            imp.data_ptr(data, string)^, ~sender)
        imp.barrier(ctx)
    }
    imp.join(ctx)
    fmt.printfln("[{}/{}] done", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
    imp.barrier(ctx)
}

run_test :: proc(thread_count: int, exec: proc(ctx: imp.Ctx, data: $I), data: I) {
    fmt.println("--------------")
    ctx: imp.Global_Ctx
    imp.global_ctx_init(&ctx, 4)
    defer imp.global_ctx_destroy(&ctx)
    imp.lauch(&ctx, exec, data)
}

main :: proc() {
    run_test(4, exec_branch, 1)
    run_test(4, exec_nested_branches, 2)
    run_test(4, exec_messages, 3)
}
