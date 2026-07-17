package imp

import "core:fmt"

exec_branch :: proc(ctx: Parallel_Ctx, i: int) {
    fmt.printfln("[{}/{}]: i = {}", get_thread_index(ctx) + 1, get_thread_count(ctx), i)

    if branch(ctx, 2) {
        fmt.printfln("branch0(1, 2)[{}/{}]", get_thread_index(ctx) + 1, get_thread_count(ctx))
    } else {
        fmt.printfln("branch1(1, 2)[{}/{}]", get_thread_index(ctx) + 1, get_thread_count(ctx))
    }
    join(ctx)

    fmt.printfln("[{}/{}] done", get_thread_index(ctx) + 1, get_thread_count(ctx))
}

exec_nested_branches :: proc(ctx: Parallel_Ctx, i: int) {
    fmt.printfln("[{}/{}]: i = {}", get_thread_index(ctx) + 1, get_thread_count(ctx), i)

    if branch(ctx, 2) {
        fmt.printfln("branch0(1, 2)[{}/{}]", get_thread_index(ctx) + 1, get_thread_count(ctx))
    } else {
        fmt.printfln("branch1(1, 2)[{}/{}]", get_thread_index(ctx) + 1, get_thread_count(ctx))
        if branch(ctx, 1) {
            fmt.printfln("branch0(3, 4)[{}/{}]", get_thread_index(ctx) + 1, get_thread_count(ctx))
        } else {
            fmt.printfln("branch1(3, 4)[{}/{}]", get_thread_index(ctx) + 1, get_thread_count(ctx))
        }
        join(ctx)
    }
    join(ctx)

    fmt.printfln("[{}/{}] done", get_thread_index(ctx) + 1, get_thread_count(ctx))
}

exec_messages :: proc(ctx: Parallel_Ctx, i: int) {
    ensure(get_thread_count(ctx) == 4)
    buf0: [2][100]u8
    buf1: [2][100]u8
    fmt.printfln("[{}/{}]: i = {}", get_thread_index(ctx) + 1, get_thread_count(ctx), i)

    branch_ctx: Branch_Ctx
    if branch(ctx, 2, &branch_ctx) {
        ensure(get_thread_count(ctx) == 2)

        text := fmt.bprintf(buf0[get_thread_index(ctx)][:], "branch 1 thread {}", get_thread_index(ctx))
        send_message(ctx, 1 - get_thread_index(ctx), make_data(&text))
        send_message(ctx, branch_ctx[1], get_thread_index(ctx), make_data(&text))

        for i in 0..<2 {
            received_msg := recv_message(ctx)
            received_data := received_msg.content.(Data)
            if received_msg.sender_index >= 0 {
                fmt.printfln("branch0[{}/{}]: {} (local from {})",
                    get_thread_index(ctx) + 1, get_thread_count(ctx),
                    data_ptr(received_data, string)^, received_msg.sender_index)
            } else {
                fmt.printfln("branch0[{}/{}]: {} (remote from {})",
                    get_thread_index(ctx) + 1, get_thread_count(ctx),
                    data_ptr(received_data, string)^, ~received_msg.sender_index)
                send_message(ctx, received_msg.sender_index, make_data(&text))
            }
        }
        new_msg := recv_message(ctx)
        new_data := new_msg.content.(Data)
        fmt.printfln("branch0[{}/{}]: {} (global from {})",
            get_thread_index(ctx) + 1, get_thread_count(ctx),
            data_ptr(new_data, string)^, ~new_msg.sender_index)
        barrier(ctx)
    } else {
        ensure(get_thread_count(ctx) == 2)

        text := fmt.bprintf(buf1[get_thread_index(ctx)][:], "branch 2 thread {}", get_thread_index(ctx))
        send_message(ctx, 1 - get_thread_index(ctx), make_data(&text))
        send_message(ctx, branch_ctx[0], get_thread_index(ctx), make_data(&text))

        for _ in 0..<2 {
            received_msg := recv_message(ctx)
            received_data := received_msg.content.(Data)
            if received_msg.sender_index >= 0 {
                fmt.printfln("branch1[{}/{}]: {} (local from {})",
                    get_thread_index(ctx) + 1, get_thread_count(ctx),
                    data_ptr(received_data, string)^, received_msg.sender_index)
            } else {
                fmt.printfln("branch1[{}/{}]: {} (remote from {})",
                    get_thread_index(ctx) + 1, get_thread_count(ctx),
                    data_ptr(received_data, string)^, ~received_msg.sender_index)
                send_message(ctx, received_msg.sender_index, make_data(&text))
            }
        }
        new_msg := recv_message(ctx)
        new_data := new_msg.content.(Data)
        fmt.printfln("branch1[{}/{}]: {} (global from {})",
            get_thread_index(ctx) + 1, get_thread_count(ctx),
            data_ptr(new_data, string)^, ~new_msg.sender_index)
        barrier(ctx)
    }
    join(ctx)
    fmt.printfln("[{}/{}] done", get_thread_index(ctx) + 1, get_thread_count(ctx))
    barrier(ctx)
}

run_test :: proc(thread_count: int, exec: proc(ctx: Parallel_Ctx, data: $I), data: I) {
    fmt.println("--------------")
    line: Parallel_Line
    parallel_line_init(&line, thread_count, exec, data)
    defer parallel_line_destroy(&line)
}

main :: proc() {
    run_test(4, exec_branch, 1)
    run_test(4, exec_nested_branches, 2)
    run_test(4, exec_messages, 3)
}
