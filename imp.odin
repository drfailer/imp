package imp

import "core:sync"
import "core:mem"
import "base:intrinsics"
import q "core:container/queue"
import "base:runtime"
import "core:time"
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
    },
    comm_channel_count: int,
    profilers: Profilers,
}

DEFAULT_CONTEXT_CAPACITY :: #config(IMP_DEFAULT_CONTEXT_CAPACITY, 64)
DEFAULT_SHARED_CTX_POOL_SIZE :: #config(IMP_DEFAULT_SHARED_CTX_POOL_SIZE, 16)

global_ctx_init :: proc(ctx: ^Global_Ctx, thread_count: int,
                        comm_channel_count := 1,
                        shared_ctx_pool_capacity := DEFAULT_SHARED_CTX_POOL_SIZE,
                        thread_ctx_stack_capacity := DEFAULT_CONTEXT_CAPACITY) {
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
        thread_ctx_init(&tctx, idx, thread_ctx_stack_capacity, comm_channel_count, ctx.shared.root, allocator)
    }

    when PROFILER_ENABLED {
        // TODO: the profiler should use the thread allocator
        ctx.profilers.profilers = make([]Profiler, thread_count, allocator)
        time.stopwatch_start(&ctx.profilers.stopwatch)
        for &tctx, idx in ctx.thread_ctxs {
            profiler_init(&ctx.profilers.profilers[idx])
            tctx.profiler = &ctx.profilers.profilers[idx]
        }
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

//
// Context usique to the thread.
//

Thread_Ctx :: struct {
    id: int,
    comms: Comms,
    ctx_stack: [dynamic]Local_Ctx,
    profiler: ^Profiler,
}

thread_ctx_init :: proc(ctx: ^Thread_Ctx, index,
                        ctx_stack_capacity, comm_channel_count: int,
                        shared_ctx: ^Shared_Ctx, allocator: mem.Allocator) {
    ctx.id = index
    comms_init(&ctx.comms, comm_channel_count, allocator)
    ctx.ctx_stack = make([dynamic]Local_Ctx, 1, ctx_stack_capacity + 1, allocator)
    ctx.ctx_stack[0] = Local_Ctx{ shared_ctx = shared_ctx, thread_index = index }
}

thread_ctx_destroy :: proc(ctx: ^Thread_Ctx) {
    comms_destroy(&ctx.comms)
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
    branch: struct {
        init_counter: int,    // Slot claiming
        fini_counter: int,    // Exit reference count
        arrival_counter: int, // ASAP reset counter
        generation: int,      // Solves the fast-laps-slow hazard
        ctxs: [2]^Shared_Ctx, // used to shared new context with threads in left and right branch
    },
    sync: union { // use for synchronizing values
        rawptr,
        runtime.Raw_Slice,
    },
}

shared_ctx_init :: proc(ctx: ^Shared_Ctx, thread_count, comm_channel_count: int) {
    ctx.thread_count = thread_count
    barrier_init(&ctx.barrier, thread_count)
}

shared_ctx_destroy :: proc(ctx: ^Shared_Ctx) {
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

get_local_ctx :: proc(ctx: Ctx) -> ^Local_Ctx {
    return &ctx.thread_ctx.ctx_stack[len(ctx.thread_ctx.ctx_stack) - 1]
}

get_shared_ctx :: proc(ctx: Ctx) -> ^Shared_Ctx {
    return get_local_ctx(ctx).shared_ctx
}

// single //////////////////////////////

single :: proc(ctx: Ctx, index := 0) -> bool {
    return get_thread_index(ctx) == index
}

// barrier /////////////////////////////

barrier :: proc(ctx: Ctx, kind := BarrierKind.Spin) {
    barrier_wait(&get_shared_ctx(ctx).barrier, kind)
}

// sync values /////////////////////////

sync_vals_slice :: proc(ctx: Ctx, master_index: int, vals: []$T) {
    if vals == nil do return

    shared_ctx := get_shared_ctx(ctx)

    if get_thread_index(ctx) == master_index {
        shared_ctx.sync = runtime.Raw_Slice{raw_data(vals), len(vals)}
    }
    barrier(ctx, .Spin)
    if get_thread_index(ctx) != master_index {
        master_vals := transmute([]T)shared_ctx.sync.(runtime.Raw_Slice)
        assert(len(vals) == len(master_vals))
        for i in 0..<len(vals) {
            vals[i] = master_vals[i]
        }
    }
    barrier(ctx, .Spin)
}

sync_vals_variadic :: proc(ctx: Ctx, master_index: int, $T: typeid, vals: ..^T) {
    vals_array := make([dynamic]T, len(vals), context.temp_allocator)
    defer delete(vals_array)

    if get_thread_index(ctx) == master_index {
        for val, idx in vals {
            vals_array[idx] = val^
        }
    }
    sync_vals_slice(ctx, master_index, vals_array[:])
    if get_thread_index(ctx) != master_index {
        for val, idx in vals {
            val^ = vals_array[idx]
        }
    }
}

sync_vals :: proc{
    sync_vals_slice,
    sync_vals_variadic,
}

sync_val :: proc(ctx: Ctx, master_index: int, val: ^$T) {
    shared_ctx := get_shared_ctx(ctx)

    if get_thread_index(ctx) == master_index {
        shared_ctx.sync = cast(rawptr)val
    }
    barrier(ctx, .Spin)
    if get_thread_index(ctx) != master_index {
        val^ = (cast(^T)shared_ctx.sync.(rawptr))^
    }
    barrier(ctx, .Spin)
}

// range ///////////////////////////////

Range :: struct {
    thread_count: int,
    init, step, max: int,
    it_out, it_in, it: int,
}

range_init :: proc(ctx: Ctx, count: int) -> Range {
    thread_count := get_thread_count(ctx)
    thread_index := get_thread_index(ctx)

    if thread_count >= count {
        return Range{thread_count, thread_index, 1, count, thread_index, 0, thread_index}
    }

    step := count / thread_count
    start_idx := thread_index * step
    return Range{thread_count, start_idx, step, count, thread_index, 0, thread_index}
}

range_continue :: proc(range: Range) -> bool {
    return range.it < range.max
}

range_next_mut :: proc(range: ^Range) {
    range.it_in += 1
    if range.it_in == range.step {
        range.it_in = 0
        range.it_out += range.thread_count
        range.it = range.it_out * range.step
    } else {
        range.it += 1
    }
}

range_next_imut :: proc(range: Range) -> Range {
    range := range
    range_next_mut(&range)
    return range
}

range_next :: proc{
    range_next_mut,
    range_next_imut,
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

join_to :: proc(ctx: Ctx, local_ctx: ^Local_Ctx) {
    for get_local_ctx(ctx) != local_ctx {
        join(ctx)
    }
}

// messages ////////////////////////////

//
// A negative thread index will be treated as a ~global thread id. When threads
// communicate outside of the current current context, they use their thread id
// as identifier and make it negative so that the receiver can know.
//

@(private)
get_thread_data_from_index_and_wait_if_not_available :: proc(ctx: ^Shared_Ctx, index: int) -> ^Thread_Ctx {
    if index < 0 || index >= ctx.thread_count {
        panic("the target thread does not exist")
    }

    // threads going into a branch do not wait for the other threads so we need
    // to wait for the thread_data to be available if the initialization is not
    // done yet (most of the time, thread won't wait).
    for {
        thread_data := sync.atomic_load_explicit(&ctx.thread_index_map[index], .Acquire)
        if thread_data != nil do return thread_data
        intrinsics.cpu_relax()
    }
    panic("unreachable")
}

send_data_parallel_ctx :: proc(ctx: Ctx, thread_index: int, data: Data, channel := 0) {
    shared_ctx := get_local_ctx(ctx).shared_ctx
    if thread_index >= 0 {
        // local send
        receiver_data := get_thread_data_from_index_and_wait_if_not_available(shared_ctx, thread_index)
        comms_send(&receiver_data.comms, Message(Data){get_thread_index(ctx), data}, channel)
    } else {
        // global send
        receiver_data := &ctx.global_ctx.thread_ctxs[~thread_index]
        comms_send(&receiver_data.comms, Message(Data){~get_thread_id(ctx), data}, channel)
    }
}

send_data_shared_ctx :: proc(ctx: Ctx, shared_ctx: ^Shared_Ctx, thread_index: int, data: Data, channel := 0) {
    assert(shared_ctx != get_local_ctx(ctx).shared_ctx)
    assert(thread_index >= 0)
    receiver_data := get_thread_data_from_index_and_wait_if_not_available(shared_ctx, thread_index)
    comms_send(&receiver_data.comms, Message(Data){~get_thread_id(ctx), data}, channel)
}

send_data :: proc{ send_data_parallel_ctx, send_data_shared_ctx }

recv_data :: proc(ctx: Ctx, channel := ANY_CHANNEL) -> (Data, int, bool) {
    msg, ok := comms_recv(&ctx.thread_ctx.comms, Data, channel)
    return msg.content, msg.sender_index, ok
}

try_recv_data :: proc(ctx: Ctx, channel := ANY_CHANNEL) -> (Data, int, bool) {
    msg, ok := comms_try_recv(&ctx.thread_ctx.comms, Data, channel)
    return msg.content, msg.sender_index, ok
}
