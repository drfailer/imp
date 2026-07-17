package imp

import "core:thread"
import "core:sync"
import "core:mem"
import q "core:container/queue"
import "core:fmt"

// Job /////////////////////////////////////////////////////////////////////////

// FIXME: how to handle the reuse, do we need a generation counter?
Job :: struct {
    mutex: sync.Mutex,
    cond: sync.Cond,
    steps: int,
    queue: LockQueue(Data),
}

job_create :: proc(step_count := 1) -> ^Job {
    job := new(Job)
    queue_init(&job.queue)
    job.steps = step_count
    return job
}

job_destroy :: proc(job: ^Job) {
    queue_destroy(&job.queue)
    free(job)
}

job_reset :: proc(job: ^Job, step_count := 1) {
    job.steps = step_count
}

job_done :: proc(job: ^Job, mdata: Maybe(Data) = nil) {
    steps := sync.atomic_sub(&job.steps, 1)
    if data, ok := mdata.?; ok {
        queue_push(&job.queue, data)
        sync.cond_broadcast(&job.cond)
    } else if steps <= 1 {
        sync.cond_broadcast(&job.cond)
    }
}

job_result :: proc(job: ^Job, data: Data) {
    queue_push(&job.queue, data)
    sync.cond_broadcast(&job.cond)
}

job_is_done :: proc(job: ^Job) -> bool {
    sync.lock(&job.mutex)
    defer sync.unlock(&job.mutex)
    return job.steps == 0
}

job_has_data :: proc(job: ^Job) -> bool {
    return queue_size(&job.queue) > 0
}

job_pop_data :: proc(job: ^Job) -> (Data, bool) {
    return queue_pop(&job.queue)
}

job_wait :: proc(job: ^Job) {
    sync.lock(&job.mutex)
    defer sync.unlock(&job.mutex)
    for {
        if sync.atomic_load(&job.steps) <= 0 do break
        sync.cond_wait(&job.cond, &job.mutex)
    }
}

job_wait_data :: proc(job: ^Job) -> (data: Data, has_data: bool) {
    sync.lock(&job.mutex)
    defer sync.unlock(&job.mutex)
    for {
        if queue_size(&job.queue) > 0 do return queue_pop(&job.queue)
        if sync.atomic_load(&job.steps) <= 0 do break
        sync.cond_wait(&job.cond, &job.mutex)
    }
    return data, false
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

// Messages ////////////////////////////////////////////////////////////////////

Message :: union {
    Data,
    ^Shared_Ctx,
    ^Job,
}

MessageBox :: struct {
    queue: LockQueue(Message),
    cond: sync.Cond,
    mutex: sync.Mutex,
}

message_box_init :: proc(mb: ^MessageBox) {
    queue_init(&mb.queue)
}

message_box_destroy :: proc(mb: ^MessageBox) {
    queue_destroy(&mb.queue)
}

message_box_send :: proc(mb: ^MessageBox, m: Message) {
    queue_push(&mb.queue, m)
    sync.cond_signal(&mb.cond)
}

message_box_recv :: proc(mb: ^MessageBox) -> Message {
    sync.lock(&mb.mutex)
    defer sync.unlock(&mb.mutex)
    for {
        if m, ok := queue_pop(&mb.queue); ok {
            return m
        }
        sync.cond_wait(&mb.cond, &mb.mutex)
    }
}

message_box_try_recv :: proc(mb: ^MessageBox) -> (m: Message, received: bool) {
    return queue_pop(&mb.queue)
}

message_box_get_size :: proc(mb: ^MessageBox) -> int {
    return queue_size(&mb.queue)
}

// Contexts ////////////////////////////////////////////////////////////////////

SENTINEL_CTX := cast(^Shared_Ctx)uintptr(0xDEADBEEF)

Shared_Ctx :: struct {
    branch: struct {
        init_counter: int,        // Slot claiming
        fini_counter: int,        // Exit reference count
        arrival_counter: int,     // ASAP reset counter
        generation: int,          // Solves the fast-laps-slow hazard
        left, right: ^Shared_Ctx, // used to shared new context with threads in left and right branch
    },
    parent: ^Shared_Ctx,
    thread_count: int,
    barrier: sync.Barrier,
    message_box: MessageBox,
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

// Parallel lines //////////////////////////////////////////////////////////////

Thread_Data :: struct {
    id: int,
    thread: ^thread.Thread,
    message_box: MessageBox,
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
    message_box_init(&ctx.message_box)
    return ctx
}

@(private)
push_free_ctx :: proc(line: ^Parallel_Line, ctx: ^Shared_Ctx) {
    ctx.branch.left = nil
    ctx.branch.right = nil
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
    message_box_init(&line.root_ctx.message_box)

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
        message_box_destroy(&td.message_box)
    }
    message_box_destroy(&line.root_ctx.message_box)
    curr := line.shared_ctx_free_list
    for curr != nil {
        message_box_destroy(&curr.message_box)
        curr = curr.parent
    }
    mem.dynamic_arena_destroy(&line.arena)
}

// Parallel API ////////////////////////////////////////////////////////////////

@(private)
get_local_context :: proc(ctx: Parallel_Ctx) -> ^Local_Ctx {
    return &ctx.thread_data.context_stack[len(ctx.thread_data.context_stack) - 1]
}

get_thread_index :: proc(ctx: Parallel_Ctx) -> int {
    return get_local_context(ctx).thread_index
}

get_thread_count :: proc(ctx: Parallel_Ctx) -> int {
    return get_local_context(ctx).shared_ctx.thread_count
}

barrier :: proc(ctx: Parallel_Ctx) {
    sync.barrier_wait(&get_local_context(ctx).shared_ctx.barrier)
}

branch :: proc(ctx: Parallel_Ctx, right_thread_count: int) -> bool {
    parent_local := get_local_context(ctx)
    parent_ctx := parent_local.shared_ctx

    // The exact generation we need for THIS branch at THIS nesting level
    my_expected_gen := parent_local.branch_generation + 1

    // =========================================================================
    // LOOP 1: Wait for Reset
    // If the generation is older than expected AND pointers aren't nil,
    // the previous branch hasn't finished its ASAP reset yet.
    // =========================================================================
    if sync.atomic_load_explicit(&parent_ctx.branch.generation, .Acquire) < my_expected_gen &&
       sync.atomic_load_explicit(&parent_ctx.branch.left, .Acquire) != nil
    {
        sync.mutex_lock(&parent_ctx.mutex)
        for sync.atomic_load_explicit(&parent_ctx.branch.generation, .Acquire) < my_expected_gen &&
            sync.atomic_load_explicit(&parent_ctx.branch.left, .Acquire) != nil
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
            &parent_ctx.branch.left, expected, SENTINEL_CTX,
            .Acquire, .Acquire); ok
        {
            left_node := pop_free_ctx(ctx.line)
            right_node := pop_free_ctx(ctx.line)

            left_node.thread_count = parent_ctx.thread_count - right_thread_count
            sync.atomic_store_explicit(&left_node.branch.init_counter, 0, .Relaxed)
            sync.atomic_store_explicit(&left_node.branch.fini_counter, left_node.thread_count, .Relaxed)
            left_node.parent = parent_ctx

            right_node.thread_count = right_thread_count
            sync.atomic_store_explicit(&right_node.branch.init_counter, 0, .Relaxed)
            sync.atomic_store_explicit(&right_node.branch.fini_counter, right_node.thread_count, .Relaxed)
            right_node.parent = parent_ctx

            // Protect state mutation to prevent lost wakeups for Loop 2
            sync.mutex_lock(&parent_ctx.mutex)
            sync.atomic_store_explicit(&parent_ctx.branch.right, right_node, .Release)
            sync.atomic_store_explicit(&parent_ctx.branch.left, left_node, .Release)
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
    right_ctx := sync.atomic_load_explicit(&parent_ctx.branch.right, .Acquire)
    left_ctx := sync.atomic_load_explicit(&parent_ctx.branch.left, .Acquire)

    idx := sync.atomic_add_explicit(&right_ctx.branch.init_counter, 1, .Relaxed)

    new_local: Local_Ctx
    if idx < right_ctx.thread_count {
        // Fresh generation for the nested scope
        new_local = Local_Ctx{ shared_ctx = right_ctx, thread_index = idx, branch_generation = 0 }
    } else {
        idx_left := sync.atomic_add_explicit(&left_ctx.branch.init_counter, 1, .Relaxed)
        new_local = Local_Ctx{ shared_ctx = left_ctx, thread_index = idx_left, branch_generation = 0 }
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
        sync.atomic_store_explicit(&parent_ctx.branch.right, nil, .Release)
        sync.atomic_store_explicit(&parent_ctx.branch.left, nil, .Release)
        sync.mutex_unlock(&parent_ctx.mutex)

        sync.cond_broadcast(&parent_ctx.cond)
    }

    append(&ctx.thread_data.context_stack, new_local)
    return new_local.shared_ctx == right_ctx
}

join :: proc(ctx: Parallel_Ctx) {
    cur_ctx := get_local_context(ctx).shared_ctx
    prev_count := sync.atomic_sub_explicit(&cur_ctx.branch.fini_counter, 1, .Relaxed)
    if prev_count == 1 {
        push_free_ctx(ctx.line, cur_ctx)
    }
    pop(&ctx.thread_data.context_stack)
}
