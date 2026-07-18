package imp

import "core:sync"
import "core:mem"
import "base:intrinsics"
import q "core:container/queue"
import "core:fmt"

SENTINEL_CTX :: cast(^Shared_Ctx)uintptr(0xDEADBEEF)

// Contexts ////////////////////////////////////////////////////////////////////

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
    }
}

DEFAULT_CONTEXT_CAPACITY :: #config(IMP_DEFAULT_CONTEXT_CAPACITY, 64)

global_ctx_init :: proc(ctx: ^Global_Ctx, thread_count: int, thread_ctx_stack_capacity := DEFAULT_CONTEXT_CAPACITY) {
    mem.dynamic_arena_init(&ctx.arena)
    allocator := mem.dynamic_arena_allocator(&ctx.arena)

    // Setup Root Context
    ctx.shared.root = new(Shared_Ctx, allocator)
    shared_ctx_init(ctx.shared.root, thread_count)
    ctx.shared.free_list = nil

    // Create Threads
    ctx.thread_ctxs = make([dynamic]Thread_Ctx, thread_count, allocator)
    for &tctx, idx in ctx.thread_ctxs {
        thread_ctx_init(&tctx, idx, thread_ctx_stack_capacity, ctx.shared.root, allocator)
    }
}

global_ctx_destroy :: proc(ctx: ^Global_Ctx) {
    for &tctx in ctx.thread_ctxs {
        thread_ctx_destroy(&tctx)
    }
    shared_ctx_destroy(ctx.shared.root)
    curr := ctx.shared.free_list
    for curr != nil {
        shared_ctx_destroy(curr)
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
    message_boxes_init(&shared_ctx.message_boxes)
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

//
// Context usique to the thread.
//

Thread_Ctx :: struct {
    id: int,
    message_boxes: MessageBoxes,
    ctx_stack: [dynamic]Local_Ctx,
}

thread_ctx_init :: proc(ctx: ^Thread_Ctx, index, ctx_stack_capacity: int, shared_ctx: ^Shared_Ctx, allocator: mem.Allocator) {
    ctx.id = index
    ctx.ctx_stack = make([dynamic]Local_Ctx, 1, ctx_stack_capacity + 1, allocator)
    ctx.ctx_stack[0] = Local_Ctx{ shared_ctx = shared_ctx, thread_index = index }
}

thread_ctx_destroy :: proc(ctx: ^Thread_Ctx) {
    message_boxes_destroy(&ctx.message_boxes)
    delete(ctx.ctx_stack)
}

//
// Context shared between a group of thread (creating a new branch creates a
// new shared context).
//

Shared_Ctx :: struct {
    parent: ^Shared_Ctx,
    thread_count: int,
    cond: sync.Cond,
    mutex: sync.Mutex,
    thread_index_map: [dynamic]^Thread_Ctx, // map local index to thread data (used by recv/send API)
    barrier: Barrier,
    message_boxes: MessageBoxes,
    branch: struct {
        init_counter: int,    // Slot claiming
        fini_counter: int,    // Exit reference count
        arrival_counter: int, // ASAP reset counter
        generation: int,      // Solves the fast-laps-slow hazard
        ctxs: [2]^Shared_Ctx, // used to shared new context with threads in left and right branch
    },
    sync: union { // use for synchronizing values
        u64,
    },
}

shared_ctx_init :: proc(ctx: ^Shared_Ctx, thread_count: int) {
    ctx.thread_count = thread_count
    barrier_init(&ctx.barrier, thread_count)
    message_boxes_init(&ctx.message_boxes)
}

shared_ctx_destroy :: proc(ctx: ^Shared_Ctx) {
    message_boxes_destroy(&ctx.message_boxes)
    delete(ctx.thread_index_map)
}

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
// Context used in the API.
//

Ctx :: struct {
    global_ctx: ^Global_Ctx,
    thread_ctx: ^Thread_Ctx,
}

@(private)
get_local_ctx :: proc(ctx: Ctx) -> ^Local_Ctx {
    return &ctx.thread_ctx.ctx_stack[len(ctx.thread_ctx.ctx_stack) - 1]
}

@(private)
get_shared_ctx :: proc(ctx: Ctx) -> ^Shared_Ctx {
    return get_local_ctx(ctx).shared_ctx
}

// Data ////////////////////////////////////////////////////////////////////////

Data :: struct {
    type: typeid,
    ptr: rawptr,
}

data_ptr :: proc(data: Data, $T: typeid) -> ^T {
    return cast(^T)data.ptr
}

data_type :: proc(data: Data) -> typeid {
    return data.type
}

make_data :: proc(ptr: ^$T) -> Data {
    return Data{T, ptr}
}

// Parallel API ////////////////////////////////////////////////////////////////

// accessors ///////////////////////////

get_thread_index :: proc(ctx: Ctx) -> int {
    return get_local_ctx(ctx).thread_index
}

get_thread_id :: proc(ctx: Ctx) -> int {
    return ctx.thread_ctx.id
}


get_thread_count :: proc(ctx: Ctx) -> int {
    return get_shared_ctx(ctx).thread_count
}

// barrier /////////////////////////////

barrier :: proc(ctx: Ctx, kind := BarrierKind.Spin) {
    barrier_wait(&get_shared_ctx(ctx).barrier, kind)
}

// sync values /////////////////////////

sync_val64 :: proc(ctx: Ctx, val: ^$T, master_index: int) where size_of(T) == 8 {
    shared_ctx := get_shared_ctx(ctx)

    if get_thread_index(ctx) == master_index {
        shared_ctx.sync.(u64) = val^
    }
    barrier(ctx, .Spin)
    val^ = shared_ctx.sync.(u64)
    barrier(ctx, .Spin)
}

sync_val :: proc{
    sync_val64,
}

// reduce //////////////////////////////

// TODO: we will need more reduction functions depending on the situation

reduce :: proc(ctx: Ctx, values: []$T, op: proc(val, acc: T) -> T) -> T {
    // TODO
}

// branch //////////////////////////////

Branch_Ctx :: distinct [2]^Shared_Ctx

branch :: proc(ctx: Ctx, right_thread_count: int, branch_ctx: ^Branch_Ctx = nil) -> bool {
    parent_local := get_local_ctx(ctx)
    parent_ctx := parent_local.shared_ctx

    // The exact generation we need for THIS branch at THIS nesting level
    my_expected_gen := parent_local.branch_generation + 1

    // =========================================================================
    // LOOP 1: Wait for Reset
    // If the generation is older than expected AND pointers aren't nil,
    // the previous branch hasn't finished its ASAP reset yet.
    // =========================================================================
    if sync.atomic_load_explicit(&parent_ctx.branch.generation, .Acquire) < my_expected_gen &&
       sync.atomic_load_explicit(&parent_ctx.branch.ctxs[1], .Acquire) != nil
    {
        sync.mutex_lock(&parent_ctx.mutex)
        for sync.atomic_load_explicit(&parent_ctx.branch.generation, .Acquire) < my_expected_gen &&
            sync.atomic_load_explicit(&parent_ctx.branch.ctxs[1], .Acquire) != nil
        {
            sync.cond_wait(&parent_ctx.cond, &parent_ctx.mutex)
        }
        sync.mutex_unlock(&parent_ctx.mutex)
    }

    // =========================================================================
    // ELECTION: Try to become the initializer
    // =========================================================================
    if sync.atomic_load_explicit(&parent_ctx.branch.generation, .Acquire) < my_expected_gen {
        expected: ^Shared_Ctx = nil
        if _, ok := sync.atomic_compare_exchange_strong_explicit(
            &parent_ctx.branch.ctxs[1], expected, SENTINEL_CTX,
            .Acquire, .Acquire); ok
        {
            node0 := alloc_shared_ctx(ctx.global_ctx)
            node1 := alloc_shared_ctx(ctx.global_ctx)

            node0.thread_count = right_thread_count
            clear(&node0.thread_index_map)
            resize(&node0.thread_index_map, node0.thread_count)
            barrier_init(&node0.barrier, node0.thread_count)
            node0.branch.init_counter = 0
            node0.branch.fini_counter = node0.thread_count
            node0.parent = parent_ctx

            node1.thread_count = parent_ctx.thread_count - right_thread_count
            clear(&node1.thread_index_map)
            resize(&node1.thread_index_map, node1.thread_count)
            barrier_init(&node1.barrier, node1.thread_count)
            node1.branch.init_counter = 0
            node1.branch.fini_counter = node1.thread_count
            node1.parent = parent_ctx

            // Protect state mutation to prevent lost wakeups for Loop 2
            sync.mutex_lock(&parent_ctx.mutex)
            sync.atomic_store_explicit(&parent_ctx.branch.ctxs[0], node0, .Release)
            sync.atomic_store_explicit(&parent_ctx.branch.ctxs[1], node1, .Release)
            // Bump generation LAST to signal readiness
            sync.atomic_store_explicit(&parent_ctx.branch.generation, my_expected_gen, .Release)
            sync.mutex_unlock(&parent_ctx.mutex)

            sync.cond_broadcast(&parent_ctx.cond)
        }
    }

    // =========================================================================
    // LOOP 2: Wait for Init
    // =========================================================================
    if sync.atomic_load_explicit(&parent_ctx.branch.generation, .Acquire) < my_expected_gen {
        sync.mutex_lock(&parent_ctx.mutex)
        for sync.atomic_load_explicit(&parent_ctx.branch.generation, .Acquire) < my_expected_gen {
            sync.cond_wait(&parent_ctx.cond, &parent_ctx.mutex)
        }
        sync.mutex_unlock(&parent_ctx.mutex)
    }

    // =========================================================================
    // CLAIM: Slots and prepare local context
    // =========================================================================
    ctx0 := sync.atomic_load_explicit(&parent_ctx.branch.ctxs[0], .Acquire)
    ctx1 := sync.atomic_load_explicit(&parent_ctx.branch.ctxs[1], .Acquire)

    if branch_ctx != nil do branch_ctx^ = {ctx0, ctx1}

    idx0 := sync.atomic_add_explicit(&ctx0.branch.init_counter, 1, .Relaxed)

    new_local: Local_Ctx
    if idx0 < ctx0.thread_count {
        // Fresh generation for the nested scope
        new_local = Local_Ctx{ shared_ctx = ctx0, thread_index = idx0, branch_generation = 0 }
        sync.atomic_store_explicit(&ctx0.thread_index_map[idx0], ctx.thread_ctx, .Release)
    } else {
        idx1 := sync.atomic_add_explicit(&ctx1.branch.init_counter, 1, .Relaxed)
        new_local = Local_Ctx{ shared_ctx = ctx1, thread_index = idx1, branch_generation = 0 }
        sync.atomic_store_explicit(&ctx1.thread_index_map[idx1], ctx.thread_ctx, .Release)
    }

    // Advance our local generation tracker for the parent context
    parent_local.branch_generation = my_expected_gen

    // =========================================================================
    // ASAP RESET: Last thread to arrive cleans the slate
    // =========================================================================
    arrivals := sync.atomic_add_explicit(&parent_ctx.branch.arrival_counter, 1, .Relaxed)
    if arrivals == parent_ctx.thread_count - 1 {
        sync.atomic_store_explicit(&parent_ctx.branch.arrival_counter, 0, .Relaxed)

        sync.mutex_lock(&parent_ctx.mutex)
        sync.atomic_store_explicit(&parent_ctx.branch.ctxs[0], nil, .Release)
        sync.atomic_store_explicit(&parent_ctx.branch.ctxs[1], nil, .Release)
        sync.mutex_unlock(&parent_ctx.mutex)

        sync.cond_broadcast(&parent_ctx.cond)
    }

    append(&ctx.thread_ctx.ctx_stack, new_local)
    return new_local.shared_ctx == ctx0
}

join :: proc(ctx: Ctx) {
    cur_ctx := get_shared_ctx(ctx)
    prev_count := sync.atomic_sub_explicit(&cur_ctx.branch.fini_counter, 1, .Relaxed)
    if prev_count == 1 {
        release_shared_ctx(ctx.global_ctx, cur_ctx)
    }
    pop(&ctx.thread_ctx.ctx_stack)
}

// messages ////////////////////////////

//
// send: send a message to another thread (use the message box from the target thread data)
// recv: recv a message from this thread message box (we do not allow
//       specifying the sender, the first message in the queue should be
//       received and process in priority)
// put: put a message in the current context message box.
// get: get a message from the current context message box.
//
// try_recv & try_put are non blocking (return a bool).
//
// A negative thread index will be treated as a ~global thread id. When threads
// communicate outside of the current current context, they use their thread id
// as identifier and make it negative so that the receiver can know.
//

// data ///////////

send_data_parallel_ctx :: proc(ctx: Ctx, thread_index: int, data: Data) {
    send_message_parallel_ctx(ctx, thread_index, data)
}

send_data_shared_ctx :: proc(ctx: Ctx, shared_ctx: ^Shared_Ctx, thread_index: int, data: Data) {
    send_message_shared_ctx(ctx, shared_ctx, thread_index, data)
}

send_data :: proc{ send_data_parallel_ctx, send_data_shared_ctx }

recv_data :: proc(ctx: Ctx) -> (Data, int) {
    return recv_message(ctx, Data)
}

try_recv_data :: proc(ctx: Ctx) -> (Data, int, bool) {
    return try_get_message(ctx, Data)
}

put_data_parallel_ctx :: proc(ctx: Ctx, data: Data) {
    put_message_parallel_ctx(ctx, data)
}

put_data_shared_ctx :: proc(ctx: Ctx, shared_ctx: ^Shared_Ctx, data: Data) {
    put_message_shared_ctx(ctx, shared_ctx, data)
}

put_data :: proc{ put_data_parallel_ctx, put_data_shared_ctx }

get_data :: proc(ctx: Ctx) -> (Data, int) {
    return get_message(ctx, Data)
}

try_get_data :: proc(ctx: Ctx) -> (Data, int, bool) {
    return try_get_message(ctx, Data)
}

// job ////////////

send_job_parallel_ctx :: proc(ctx: Ctx, thread_index: int, job: ^Job) {
    send_message_parallel_ctx(ctx, thread_index, job)
}

send_job_shared_ctx :: proc(ctx: Ctx, shared_ctx: ^Shared_Ctx, thread_index: int, job: ^Job) {
    send_message_shared_ctx(ctx, shared_ctx, thread_index, job)
}

send_job :: proc{ send_job_parallel_ctx, send_job_shared_ctx }

recv_job :: proc(ctx: Ctx) -> (^Job, int) {
    return recv_message(ctx, ^Job)
}

try_recv_job :: proc(ctx: Ctx) -> (^Job, int, bool) {
    return try_get_message(ctx, ^Job)
}

put_job_parallel_ctx :: proc(ctx: Ctx, job: ^Job) {
    put_message_parallel_ctx(ctx, job)
}

put_job_shared_ctx :: proc(ctx: Ctx, shared_ctx: ^Shared_Ctx, job: ^Job) {
    put_message_shared_ctx(ctx, shared_ctx, job)
}

put_job :: proc{ put_job_parallel_ctx, put_job_shared_ctx }

get_job :: proc(ctx: Ctx) -> (^Job, int) {
    return get_message(ctx, ^Job)
}

try_get_job :: proc(ctx: Ctx) -> (^Job, int, bool) {
    return try_get_message(ctx, ^Job)
}
