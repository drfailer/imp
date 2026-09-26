package imp

import "core:sync"
import "core:mem"
import "base:runtime"

DEFAULT_CONTEXT_CAPACITY           :: #config(IMP_DEFAULT_CONTEXT_CAPACITY, 64)
DEFAULT_SHARED_CTX_POOL_SIZE       :: #config(IMP_DEFAULT_SHARED_CTX_POOL_SIZE, 16)
DEFAULT_THREAD_SCRATCH_MEMORY_SIZE :: #config(IMP_DEFAULT_THREAD_SCRATCH_MEMORY_SIZE, 1024*4)

SENTINEL_CTX :: cast(^Shared_Ctx)uintptr(0xDEADBEEF)

// Global Context //////////////////////////////////////////////////////////////

//
// Context global to a group of thread.
//

Global_Ctx :: struct {
    thread_ctxs: [dynamic]Thread_Ctx,
    arena: mem.Dynamic_Arena,
    mutex: sync.Mutex,
    shared: struct {
        root: ^Shared_Ctx,
        free_list: ^Shared_Ctx,
    },
    comm_channel_count: int,
}

global_ctx_init :: proc(ctx: ^Global_Ctx, thread_count: int,
                        comm_channel_count         := 1,
                        shared_ctx_pool_capacity   := DEFAULT_SHARED_CTX_POOL_SIZE,
                        thread_ctx_stack_capacity  := DEFAULT_CONTEXT_CAPACITY,
                        thread_scratch_memory_size := DEFAULT_THREAD_SCRATCH_MEMORY_SIZE) {
    mem.dynamic_arena_init(&ctx.arena)
    allocator := mem.dynamic_arena_allocator(&ctx.arena)

    ctx.comm_channel_count = comm_channel_count

    // Setup Root Context
    ctx.shared.root = new(Shared_Ctx, allocator)
    shared_ctx_init(ctx.shared.root, thread_count, comm_channel_count)
    ctx.shared.free_list = nil
    for _ in 0..<DEFAULT_SHARED_CTX_POOL_SIZE {
        shared_ctx := new(Shared_Ctx, allocator)
        shared_ctx_init(shared_ctx, thread_count - 1, comm_channel_count)
        release_shared_ctx(ctx, shared_ctx) // release add to the pool
    }

    // Create Threads
    ctx.thread_ctxs = make([dynamic]Thread_Ctx, thread_count, allocator)
    for &tctx, idx in ctx.thread_ctxs {
        thread_ctx_init(&tctx, idx, thread_ctx_stack_capacity, comm_channel_count,
                        thread_scratch_memory_size, ctx.shared.root, allocator)
    }
}

global_ctx_destroy :: proc(ctx: ^Global_Ctx) {
    allocator := mem.dynamic_arena_allocator(&ctx.arena)
    for &tctx in ctx.thread_ctxs {
        thread_ctx_destroy(&tctx)
    }
    shared_ctx_destroy(ctx.shared.root)
    curr := ctx.shared.free_list
    for curr != nil {
        shared_ctx_destroy(curr)
        free(curr, allocator)
        curr = curr.parent
    }
    mem.dynamic_arena_destroy(&ctx.arena)
}

@(private)
alloc_shared_ctx :: proc(ctx: ^Global_Ctx) -> ^Shared_Ctx {
    sync.mutex_lock(&ctx.mutex)
    defer sync.mutex_unlock(&ctx.mutex)

    if ctx.shared.free_list != nil {
        shared_ctx := ctx.shared.free_list
        ctx.shared.free_list = shared_ctx.parent
        return shared_ctx
    }
    allocator := mem.dynamic_arena_allocator(&ctx.arena)
    shared_ctx := new(Shared_Ctx, allocator)
    return shared_ctx
}

@(private)
release_shared_ctx :: proc(ctx: ^Global_Ctx, shared_ctx: ^Shared_Ctx) {
    shared_ctx.branch.ctxs = {nil, nil}
    shared_ctx.branch.arrival_counter = 0

    sync.mutex_lock(&ctx.mutex)
    defer sync.mutex_unlock(&ctx.mutex)
    shared_ctx.parent = ctx.shared.free_list
    ctx.shared.free_list = shared_ctx
}

// Thread Context //////////////////////////////////////////////////////////////

//
// Context local to a thread within a branch. Each new branch stacks a new
// local context.
//

Local_Ctx :: struct {
    shared_ctx: ^Shared_Ctx,
    thread_index: int,
    branch_generation: int, // Preserved perfectly by the context stack
}

//
// Context usique to the thread.
//

Thread_Ctx :: struct {
    id: int,
    comm: Comm(Message(Data)),
    ctx_stack: [dynamic]Local_Ctx,
    scratch_memory: mem.Scratch,
}

thread_ctx_init :: proc(ctx: ^Thread_Ctx, index, ctx_stack_capacity, comm_channel_count: int,
                        scratch_memory_size: int, shared_ctx: ^Shared_Ctx, allocator: mem.Allocator) {
    ctx.id = index
    comm_init(&ctx.comm, comm_channel_count, allocator)
    ctx.ctx_stack = make([dynamic]Local_Ctx, 1, ctx_stack_capacity + 1, allocator)
    ctx.ctx_stack[0] = Local_Ctx{ shared_ctx = shared_ctx, thread_index = index }
    mem.scratch_init(&ctx.scratch_memory, scratch_memory_size, allocator)
}

thread_ctx_destroy :: proc(ctx: ^Thread_Ctx) {
    comm_destroy(&ctx.comm)
    delete(ctx.ctx_stack)
    mem.scratch_destroy(&ctx.scratch_memory)
}

// Shared Context //////////////////////////////////////////////////////////////

//
// Context shared between a group of thread (creating a new branch creates a
// new shared context).
//

Shared_Ctx :: struct {
    parent: ^Shared_Ctx,
    thread_count: int,
    thread_index_offset: int,
    cond: sync.Cond,
    mutex: sync.Mutex,
    branch: struct #align(64) {
        generation: int,      // Solves the fast-laps-slow hazard
        ctxs: [2]^Shared_Ctx, // used to shared new context with threads in left and right branch
        fini_counter: int,    // Exit reference count
        arrival_counter: int, // ASAP reset counter
        join_sema: sync.Sema, // wakes branch-closing threads in join
    },
    sync: union { // use for synchronizing values
        rawptr,
        runtime.Raw_Slice,
    },
    barrier: Barrier,
}

shared_ctx_init :: proc(ctx: ^Shared_Ctx, thread_count, comm_channel_count: int) {
    ctx.thread_count = thread_count
    barrier_init(&ctx.barrier, thread_count)
}

shared_ctx_destroy :: proc(ctx: ^Shared_Ctx) {
}
