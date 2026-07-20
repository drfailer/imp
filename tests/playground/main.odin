package playground

import "core:fmt"
import "../../"

exec_branch :: proc(ctx: imp.Ctx, i: int) {
    fmt.printfln("[{}/{}]: i = {}", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), i)

    if imp.branch(ctx, imp.get_thread_count(ctx) / 2) {
        fmt.printfln("branch0(1, 2)[{}/{}]", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
    } else {
        fmt.printfln("branch1(1, 2)[{}/{}]", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
    }
    imp.join(ctx)

    fmt.printfln("[{}/{}] done", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
}

exec_nested_branches :: proc(ctx: imp.Ctx, i: int) {
    imp.prof_procedure(ctx)
    fmt.printfln("[{}/{}]: i = {}", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), i)

    if imp.branch(ctx, imp.get_thread_count(ctx) / 2) {
        imp.prof_region(ctx, "branch0")
        fmt.printfln("branch0(1, 2)[{}/{}]", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
    } else {
        imp.prof_region(ctx, "branch1")
        fmt.printfln("branch1(1, 2)[{}/{}]", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
        if imp.branch(ctx, imp.get_thread_count(ctx) / 2) {
            imp.prof_region(ctx, "branch10")
            fmt.printfln("branch0(3, 4)[{}/{}]", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
        } else {
            imp.prof_region(ctx, "branch11")
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

exec_sync :: proc(ctx: imp.Ctx, i: int) {
    val := imp.get_thread_index(ctx)
    val1, val2, val3 := val, val * 2, val * 3
    vals := []int{val1, val2, val3}
    fmt.printfln("[{}/{}]: i = {}", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), i)

    imp.barrier(ctx)
    fmt.printfln("[{}/{}]: val before sync = {}", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), val)
    imp.sync_val(ctx, 1, &val)
    fmt.printfln("[{}/{}]: val after sync = {}", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), val)
    imp.barrier(ctx)
    fmt.printfln("[{}/{}]: before sync = val1 = {}, val2 = {}, val3 = {}",
        imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), val1, val2, val3)
    imp.sync_vals_variadic(ctx, 2, int, &val1, &val2, &val3)
    fmt.printfln("[{}/{}]: after sync = val1 = {}, val2 = {}, val3 = {}",
        imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), val1, val2, val3)
    imp.barrier(ctx)
    fmt.printfln("[{}/{}]: before sync = {}", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), vals)
    imp.sync_vals_slice(ctx, 3, vals)
    fmt.printfln("[{}/{}]: after sync = {}", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), vals)
    imp.barrier(ctx)
}

exec_range :: proc(ctx: imp.Ctx, i: int) {
    vals: [dynamic]int
    if imp.get_thread_index(ctx) == 0 {
        vals = make([dynamic]int, 22)
        for &val, idx in vals {
            val = idx
        }
    }
    imp.sync_val(ctx, 0, &vals)
    fmt.printfln("[{}/{}]: before = {}", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), vals)
    imp.barrier(ctx)


    for r := imp.range_init(ctx, len(vals)); imp.range_continue(r); r = imp.range_next(r) {
        if imp.get_thread_index(ctx) == 0 {
            fmt.println(r)
        }
        vals[r.it] *= 2
    }
    imp.barrier(ctx)
    fmt.printfln("[{}/{}]: after = {}", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx), vals)
}

exec_job :: proc(ctx: imp.Ctx, i: int) {
    ensure(imp.get_thread_count(ctx) == 4)
    job: ^imp.CommJob(int)

    if imp.get_thread_index(ctx) == 0 {
        job = new(imp.CommJob(int))
        imp.comm_job_init(job, 2)
    }
    imp.sync_val(ctx, 0, &job)

    if imp.branch(ctx, 2) {
        // producer
        imp.comm_job_send(job, imp.get_thread_index(ctx))
        imp.job_wait_completion(job)
    } else {
        // consumer
        data := imp.comm_job_recv(job)
        fmt.println("received data through comm job:", data)
        imp.job_complete_step(job)
    }
    imp.join(ctx)
    imp.barrier(ctx)

    if imp.get_thread_index(ctx) == 0 {
        imp.comm_job_destroy(job)
        free(job)
    }
    fmt.printfln("[{}/{}] done", imp.get_thread_index(ctx) + 1, imp.get_thread_count(ctx))
}

run_test :: proc(thread_count: int, exec: proc(ctx: imp.Ctx, data: $I), data: I) {
    fmt.println("--------------")
    ctx: imp.Global_Ctx
    imp.global_ctx_init(&ctx, thread_count)
    defer imp.global_ctx_destroy(&ctx)
    imp.lauch(&ctx, exec, data)
    if data == 2 {
        imp.prof_print_report_dot(ctx, "test.dot")
    }
}

main :: proc() {
    run_test(40, exec_branch, 1)
    run_test(40, exec_nested_branches, 2)
    run_test(4, exec_messages, 3)
    run_test(4, exec_sync, 4)
    run_test(4, exec_range, 5)
    run_test(4, exec_job, 6)
}
