package imp

import "core:thread"
import "core:sync"
import "core:mem"
import "base:intrinsics"
import q "core:container/queue"
import "core:fmt"

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

// Contexts ////////////////////////////////////////////////////////////////////

SENTINEL_CTX :: cast(^Shared_Ctx)uintptr(0xDEADBEEF)

Shared_Ctx :: struct {
    branch: struct {
        init_counter: int,    // Slot claiming
        fini_counter: int,    // Exit reference count
        arrival_counter: int, // ASAP reset counter
        generation: int,      // Solves the fast-laps-slow hazard
        ctxs: [2]^Shared_Ctx, // used to shared new context with threads in left and right branch
    },
    parent: ^Shared_Ctx,
    thread_count: int,
    thread_index_map: [dynamic]^Thread_Data, // map local index to thread data (used by recv/send API)
    barrier: sync.Barrier,
    message_boxes: MessageBoxes,
    mutex: sync.Mutex,
    cond: sync.Cond,
}

shared_ctx_init :: proc(ctx: ^Shared_Ctx, thread_count: int) {
    sync.barrier_init(&ctx.barrier, thread_count)
}

Local_Ctx :: struct {
    shared_ctx: ^Shared_Ctx,
    thread_index: int,
    branch_generation: int, // Preserved perfectly by the context stack
}

Parallel_Ctx :: struct {
    line: ^Parallel_Line,
    thread_data: ^Thread_Data,
}

@(private)
get_local_ctx :: proc(ctx: Parallel_Ctx) -> ^Local_Ctx {
    return &ctx.thread_data.context_stack[len(ctx.thread_data.context_stack) - 1]
}

// Parallel lines //////////////////////////////////////////////////////////////

Thread_Data :: struct {
    id: int,
    thread: ^thread.Thread,
    message_boxes: MessageBoxes,
    context_stack: [dynamic]Local_Ctx,
}

Parallel_Line :: struct {
    threads: [dynamic]Thread_Data,
    arena: mem.Dynamic_Arena,
    mutex: sync.Mutex,
    root_ctx: ^Shared_Ctx,
    shared_ctx_free_list: ^Shared_Ctx,
}

@(private)
pop_free_ctx :: proc(line: ^Parallel_Line) -> ^Shared_Ctx {
    sync.mutex_lock(&line.mutex)
    defer sync.mutex_unlock(&line.mutex)

    if line.shared_ctx_free_list != nil {
        ctx := line.shared_ctx_free_list
        line.shared_ctx_free_list = ctx.parent
        return ctx
    }
    allocator := mem.dynamic_arena_allocator(&line.arena)
    ctx := new(Shared_Ctx, allocator)
    message_boxes_init(&ctx.message_boxes)
    return ctx
}

@(private)
push_free_ctx :: proc(line: ^Parallel_Line, ctx: ^Shared_Ctx) {
    ctx.branch.ctxs = {nil, nil}
    ctx.branch.arrival_counter = 0

    sync.mutex_lock(&line.mutex)
    defer sync.mutex_unlock(&line.mutex)
    ctx.parent = line.shared_ctx_free_list
    line.shared_ctx_free_list = ctx
}

DEFAULT_CONTEXT_CAPACITY :: #config(IMP_DEFAULT_CONTEXT_CAPACITY, 64)

parallel_line_init :: proc(line: ^Parallel_Line, thread_count: int, exec: proc(ctx: Parallel_Ctx, data: $I), data: I, context_capacity := DEFAULT_CONTEXT_CAPACITY) {
    mem.dynamic_arena_init(&line.arena)
    allocator := mem.dynamic_arena_allocator(&line.arena)

    // Setup Root Context
    line.root_ctx = new(Shared_Ctx, allocator)
    line.root_ctx.thread_count = thread_count
    sync.barrier_init(&line.root_ctx.barrier, thread_count)
    message_boxes_init(&line.root_ctx.message_boxes)

    line.shared_ctx_free_list = nil

    // Create Threads
    line.threads = make([dynamic]Thread_Data, thread_count, allocator)
    for &td, idx in line.threads {
        td.id = idx
        td.context_stack = make([dynamic]Local_Ctx, 1, context_capacity + 1, allocator)
        td.context_stack[0] = Local_Ctx{ shared_ctx = line.root_ctx, thread_index = idx }
        td.thread = thread.create_and_start_with_poly_data2(Parallel_Ctx{line, &td}, data, exec)
    }
}

parallel_line_destroy :: proc(line: ^Parallel_Line) {
    for &td in line.threads {
        thread.join(td.thread)
        thread.destroy(td.thread)
        message_boxes_destroy(&td.message_boxes)
    }
    message_boxes_destroy(&line.root_ctx.message_boxes)
    curr := line.shared_ctx_free_list
    for curr != nil {
        delete(curr.thread_index_map)
        message_boxes_destroy(&curr.message_boxes)
        curr = curr.parent
    }
    mem.dynamic_arena_destroy(&line.arena)
}

// Parallel API ////////////////////////////////////////////////////////////////

// accessors ///////////////////////////

get_thread_index :: proc(ctx: Parallel_Ctx) -> int {
    return get_local_ctx(ctx).thread_index
}

get_thread_id :: proc(ctx: Parallel_Ctx) -> int {
    return ctx.thread_data.id
}


get_thread_count :: proc(ctx: Parallel_Ctx) -> int {
    return get_local_ctx(ctx).shared_ctx.thread_count
}

// barrier /////////////////////////////

barrier :: proc(ctx: Parallel_Ctx) {
    sync.barrier_wait(&get_local_ctx(ctx).shared_ctx.barrier)
}

// branch //////////////////////////////

Branch_Ctx :: distinct [2]^Shared_Ctx

branch :: proc(ctx: Parallel_Ctx, right_thread_count: int, branch_ctx: ^Branch_Ctx = nil) -> bool {
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
            node0 := pop_free_ctx(ctx.line)
            node1 := pop_free_ctx(ctx.line)

            node0.thread_count = right_thread_count
            clear(&node0.thread_index_map)
            resize(&node0.thread_index_map, node0.thread_count)
            node0.branch.init_counter = 0
            node0.branch.fini_counter = node0.thread_count
            node0.parent = parent_ctx

            node1.thread_count = parent_ctx.thread_count - right_thread_count
            clear(&node1.thread_index_map)
            resize(&node1.thread_index_map, node1.thread_count)
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
        sync.atomic_store_explicit(&ctx0.thread_index_map[idx0], ctx.thread_data, .Release)
    } else {
        idx1 := sync.atomic_add_explicit(&ctx1.branch.init_counter, 1, .Relaxed)
        new_local = Local_Ctx{ shared_ctx = ctx1, thread_index = idx1, branch_generation = 0 }
        sync.atomic_store_explicit(&ctx1.thread_index_map[idx1], ctx.thread_data, .Release)
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

    append(&ctx.thread_data.context_stack, new_local)
    return new_local.shared_ctx == ctx0
}

join :: proc(ctx: Parallel_Ctx) {
    cur_ctx := get_local_ctx(ctx).shared_ctx
    prev_count := sync.atomic_sub_explicit(&cur_ctx.branch.fini_counter, 1, .Relaxed)
    if prev_count == 1 {
        push_free_ctx(ctx.line, cur_ctx)
    }
    pop(&ctx.thread_data.context_stack)
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

send_data_parallel_ctx :: proc(ctx: Parallel_Ctx, thread_index: int, data: Data) {
    send_message_parallel_ctx(ctx, thread_index, data)
}

send_data_shared_ctx :: proc(ctx: Parallel_Ctx, shared_ctx: ^Shared_Ctx, thread_index: int, data: Data) {
    send_message_shared_ctx(ctx, shared_ctx, thread_index, data)
}

send_data :: proc{ send_data_parallel_ctx, send_data_shared_ctx }

recv_data :: proc(ctx: Parallel_Ctx) -> (Data, int) {
    return recv_message(ctx, Data)
}

try_recv_data :: proc(ctx: Parallel_Ctx) -> (Data, int, bool) {
    return try_get_message(ctx, Data)
}

put_data_parallel_ctx :: proc(ctx: Parallel_Ctx, data: Data) {
    put_message_parallel_ctx(ctx, data)
}

put_data_shared_ctx :: proc(ctx: Parallel_Ctx, shared_ctx: ^Shared_Ctx, data: Data) {
    put_message_shared_ctx(ctx, shared_ctx, data)
}

put_data :: proc{ put_data_parallel_ctx, put_data_shared_ctx }

get_data :: proc(ctx: Parallel_Ctx) -> (Data, int) {
    return get_message(ctx, Data)
}

try_get_data :: proc(ctx: Parallel_Ctx) -> (Data, int, bool) {
    return try_get_message(ctx, Data)
}

// job ////////////

send_job_parallel_ctx :: proc(ctx: Parallel_Ctx, thread_index: int, job: ^Job) {
    send_message_parallel_ctx(ctx, thread_index, job)
}

send_job_shared_ctx :: proc(ctx: Parallel_Ctx, shared_ctx: ^Shared_Ctx, thread_index: int, job: ^Job) {
    send_message_shared_ctx(ctx, shared_ctx, thread_index, job)
}

send_job :: proc{ send_job_parallel_ctx, send_job_shared_ctx }

recv_job :: proc(ctx: Parallel_Ctx) -> (^Job, int) {
    return recv_message(ctx, ^Job)
}

try_recv_job :: proc(ctx: Parallel_Ctx) -> (^Job, int, bool) {
    return try_get_message(ctx, ^Job)
}

put_job_parallel_ctx :: proc(ctx: Parallel_Ctx, job: ^Job) {
    put_message_parallel_ctx(ctx, job)
}

put_job_shared_ctx :: proc(ctx: Parallel_Ctx, shared_ctx: ^Shared_Ctx, job: ^Job) {
    put_message_shared_ctx(ctx, shared_ctx, job)
}

put_job :: proc{ put_job_parallel_ctx, put_job_shared_ctx }

get_job :: proc(ctx: Parallel_Ctx) -> (^Job, int) {
    return get_message(ctx, ^Job)
}

try_get_job :: proc(ctx: Parallel_Ctx) -> (^Job, int, bool) {
    return try_get_message(ctx, ^Job)
}
