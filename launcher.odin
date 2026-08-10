package imp

import "core:thread"
import "prof"

Worker_Data :: struct($T: typeid) {
    ctx: Ctx,
    data: T,
    exec: proc(ctx: Ctx, data: T),
    parent_path: string,
}

launch_ctx :: proc(ctx: ^Global_Ctx, exec: proc(ctx: Ctx, data: $I), data: I) {
    thread_count := len(ctx.thread_ctxs)
    threads := make([]^thread.Thread, thread_count - 1, context.temp_allocator)
    parent_path := prof.get_parent_path()

    for &t, idx in threads {
        thread_ctx := &ctx.thread_ctxs[idx + 1]
        wd := Worker_Data(I){Ctx{ctx, thread_ctx}, data, exec, parent_path}
        t = thread.create_and_start_with_poly_data(wd, proc(wd: Worker_Data(I)) {
            prof.new_thread(wd.parent_path)
            wd.exec(wd.ctx, wd.data)
        }, init_context = context)
    }
    exec(Ctx{ctx, &ctx.thread_ctxs[0]}, data)
    for &t in threads {
        thread.join(t)
        thread.destroy(t)
    }
}

launch_threads :: proc(
    thread_count: int,
    exec: proc(ctx: Ctx, data: $I),
    data: I,
    comm_channel_count := 1,
    shared_ctx_pool_capacity := DEFAULT_SHARED_CTX_POOL_SIZE,
    thread_ctx_stack_capacity := DEFAULT_CONTEXT_CAPACITY,
    thread_scratch_memory_size := DEFAULT_THREAD_SCRATCH_MEMORY_SIZE,
) {
    ctx: Imp
    init(&ctx, thread_count, comm_channel_count, shared_ctx_pool_capacity,
         thread_ctx_stack_capacity, thread_scratch_memory_size)
    launch_ctx(&ctx, exec, data)
    destroy(&ctx)
}

launch :: proc{ launch_ctx, launch_threads }
