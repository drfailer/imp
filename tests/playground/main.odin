package playground

import "core:fmt"
import "../../"

exec_branch :: proc(i: int) {
    fmt.printfln("[{}/{}]: i = {}", imp.get_thread_index() + 1, imp.get_thread_count(), i)

    if imp.branch(imp.get_thread_count() / 2) {
        fmt.printfln("branch0(1, 2)[{}/{}]", imp.get_thread_index() + 1, imp.get_thread_count())
    } else {
        fmt.printfln("branch1(1, 2)[{}/{}]", imp.get_thread_index() + 1, imp.get_thread_count())
    }
    imp.join()

    fmt.printfln("[{}/{}] done", imp.get_thread_index() + 1, imp.get_thread_count())
}

exec_nested_branches :: proc(i: int) {
    fmt.printfln("[{}/{}]: i = {}", imp.get_thread_index() + 1, imp.get_thread_count(), i)

    if imp.branch(imp.get_thread_count() / 2) {
        fmt.printfln("branch0(1, 2)[{}/{}]", imp.get_thread_index() + 1, imp.get_thread_count())
    } else {
        fmt.printfln("branch1(1, 2)[{}/{}]", imp.get_thread_index() + 1, imp.get_thread_count())
        if imp.branch(imp.get_thread_count() / 2) {
            fmt.printfln("branch0(3, 4)[{}/{}]", imp.get_thread_index() + 1, imp.get_thread_count())
        } else {
            fmt.printfln("branch1(3, 4)[{}/{}]", imp.get_thread_index() + 1, imp.get_thread_count())
        }
        imp.join()
    }
    imp.join()

    fmt.printfln("[{}/{}] done", imp.get_thread_index() + 1, imp.get_thread_count())
}

exec_join_to :: proc(i: int) {
    ensure(imp.get_thread_count() == 8)
    shared_ctx := imp.get_shared_ctx()
    local_ctx := imp.get_local_ctx()

    if imp.branch(2) {
        fmt.printfln("branch1[{}/{}]: {}", imp.get_thread_index() + 1, imp.get_thread_count(), imp.get_thread_id())
    } else if imp.branch(2) {
        fmt.printfln("branch2[{}/{}]: {}", imp.get_thread_index() + 1, imp.get_thread_count(), imp.get_thread_id())
    } else if imp.branch(2) {
        fmt.printfln("branch3[{}/{}]: {}", imp.get_thread_index() + 1, imp.get_thread_count(), imp.get_thread_id())
    } else {
        fmt.printfln("branch4[{}/{}]: {}", imp.get_thread_index() + 1, imp.get_thread_count(), imp.get_thread_id())
    }
    imp.join_to(local_ctx)
    ensure(local_ctx == imp.get_local_ctx())
    ensure(shared_ctx == imp.get_shared_ctx())
}

exec_messages :: proc(i: int) {
    ensure(imp.get_thread_count() == 4)
    buf0: [2][100]u8
    buf1: [2][100]u8
    fmt.printfln("[{}/{}]: i = {}", imp.get_thread_index() + 1, imp.get_thread_count(), i)

    branch_ctx: imp.Branch_Ctx
    if imp.branch(2, &branch_ctx) {
        ensure(imp.get_thread_count() == 2)

        text := fmt.bprintf(buf0[imp.get_thread_index()][:], "imp.branch 1 thread {}", imp.get_thread_index())
        imp.send_data(1 - imp.get_thread_index(), &text)
        imp.send_data(branch_ctx[1], imp.get_thread_index(), &text)

        for i in 0..<2 {
            data, sender, ok := imp.recv_data()
            ensure(ok)
            if sender >= 0 {
                fmt.printfln("branch0[{}/{}]: {} (local from {})",
                    imp.get_thread_index() + 1, imp.get_thread_count(),
                    imp.data_ptr(data, string)^, sender)
            } else {
                fmt.printfln("branch0[{}/{}]: {} (remote from {})",
                    imp.get_thread_index() + 1, imp.get_thread_count(),
                    imp.data_ptr(data, string)^, ~sender)
                imp.send_data(sender, &text)
            }
        }
        data, sender, ok := imp.recv_data()
        ensure(ok)
        fmt.printfln("branch0[{}/{}]: {} (global from {})",
            imp.get_thread_index() + 1, imp.get_thread_count(),
            imp.data_ptr(data, string)^, ~sender)
        imp.barrier()
    } else {
        ensure(imp.get_thread_count() == 2)

        text := fmt.bprintf(buf1[imp.get_thread_index()][:], "imp.branch 2 thread {}", imp.get_thread_index())
        imp.send_data(1 - imp.get_thread_index(), &text)
        imp.send_data(branch_ctx[0], imp.get_thread_index(), &text)

        for _ in 0..<2 {
            data, sender, ok := imp.recv_data()
            ensure(ok)
            if sender >= 0 {
                fmt.printfln("branch1[{}/{}]: {} (local from {})",
                    imp.get_thread_index() + 1, imp.get_thread_count(),
                    imp.data_ptr(data, string)^, sender)
            } else {
                fmt.printfln("branch1[{}/{}]: {} (remote from {})",
                    imp.get_thread_index() + 1, imp.get_thread_count(),
                    imp.data_ptr(data, string)^, ~sender)
                imp.send_data(sender, &text)
            }
        }
        data, sender, ok := imp.recv_data()
        ensure(ok)
        fmt.printfln("branch1[{}/{}]: {} (global from {})",
            imp.get_thread_index() + 1, imp.get_thread_count(),
            imp.data_ptr(data, string)^, ~sender)
        imp.barrier()
    }
    imp.join()
    fmt.printfln("[{}/{}] done", imp.get_thread_index() + 1, imp.get_thread_count())
    imp.barrier()
}

exec_sync :: proc(i: int) {
    val := imp.get_thread_index()
    val1, val2, val3 := val, val * 2, val * 3
    vals := []int{val1, val2, val3}
    fmt.printfln("[{}/{}]: i = {}", imp.get_thread_index() + 1, imp.get_thread_count(), i)

    imp.barrier()
    fmt.printfln("[{}/{}]: val before sync = {}", imp.get_thread_index() + 1, imp.get_thread_count(), val)
    imp.sync_val(1, &val)
    fmt.printfln("[{}/{}]: val after sync = {}", imp.get_thread_index() + 1, imp.get_thread_count(), val)
    imp.barrier()
    fmt.printfln("[{}/{}]: before sync = val1 = {}, val2 = {}, val3 = {}",
        imp.get_thread_index() + 1, imp.get_thread_count(), val1, val2, val3)
    imp.sync_vals_variadic(2, int, &val1, &val2, &val3)
    fmt.printfln("[{}/{}]: after sync = val1 = {}, val2 = {}, val3 = {}",
        imp.get_thread_index() + 1, imp.get_thread_count(), val1, val2, val3)
    imp.barrier()
    fmt.printfln("[{}/{}]: before sync = {}", imp.get_thread_index() + 1, imp.get_thread_count(), vals)
    imp.sync_vals_slice(3, vals)
    fmt.printfln("[{}/{}]: after sync = {}", imp.get_thread_index() + 1, imp.get_thread_count(), vals)
    imp.barrier()
}

exec_range :: proc(i: int) {
    vals: [dynamic]int
    if imp.get_thread_index() == 0 {
        vals = make([dynamic]int, 22)
        for &val, idx in vals {
            val = idx
        }
    }
    imp.sync_val(0, &vals)
    fmt.printfln("[{}/{}]: before = {}", imp.get_thread_index() + 1, imp.get_thread_count(), vals)
    imp.barrier()


    for r := imp.range_init(len(vals)); imp.range_continue(r); r = imp.range_next(r) {
        if imp.get_thread_index() == 0 {
            fmt.println(r)
        }
        vals[r.it] *= 2
    }
    imp.barrier()
    fmt.printfln("[{}/{}]: after = {}", imp.get_thread_index() + 1, imp.get_thread_count(), vals)
}

exec_loop :: proc(i: int) {
    loop: ^imp.Index_Loop

    if imp.single() {
        loop = new(imp.Index_Loop)
    }
    imp.sync_val(0, &loop)

    if imp.branch(1) {
        for _ in 0..<10 {
            imp.index_loop_inc(loop)
        }
        if imp.single() {
            imp.index_loop_done(loop)
        }
    } else {
        index := 0
        for imp.index_loop_step(loop, &index) {
            fmt.printfln("[{}/{}]: {}", imp.get_thread_index() + 1, imp.get_thread_count(), index)
        }
    }
    imp.join()
    imp.barrier()

    if imp.single() {
        free(loop)
    }
}

run_test :: proc(thread_count: int, exec: proc(data: $I), data: I) {
    fmt.println("--------------")
    imp.launch(thread_count, exec, data)
}

main :: proc() {
    run_test(40, exec_branch, 1)
    run_test(40, exec_nested_branches, 2)
    run_test(8, exec_join_to, 3)
    run_test(4, exec_messages, 4)
    run_test(4, exec_sync, 5)
    run_test(4, exec_range, 6)
    run_test(4, exec_loop, 8)
}
