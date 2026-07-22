package imp

import "core:thread"

launch :: proc(ctx: ^Global_Ctx, exec: proc(ctx: Ctx, data: $I), data: I) {
    thread_count := len(ctx.thread_ctxs)
    threads := make([dynamic]^thread.Thread, thread_count - 1)
    defer delete(threads)

    for &t, idx in threads {
        thread_ctx := &ctx.thread_ctxs[idx + 1]
        t = thread.create_and_start_with_poly_data2(Ctx{ctx, thread_ctx},
                                                    data, exec,
                                                    init_context = context)
    }
    exec(Ctx{ctx, &ctx.thread_ctxs[0]}, data)
    for &t in threads {
        thread.join(t)
        thread.destroy(t)
    }
}
