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
}

DEFAULT_CONTEXT_CAPACITY :: #config(IMP_DEFAULT_CONTEXT_CAPACITY, 64)
DEFAULT_SHARED_CTX_POOL_SIZE :: #config(IMP_DEFAULT_SHARED_CTX_POOL_SIZE, 16)
DEFAULT_THREAD_SCRATCH_MEMORY_SIZE :: #config(IMP_DEFAULT_THREAD_SCRATCH_MEMORY_SIZE, 1024*4)

global_ctx_init :: proc(ctx: ^Global_Ctx, thread_count: int,
                        comm_channel_count := 1,
                        shared_ctx_pool_capacity := DEFAULT_SHARED_CTX_POOL_SIZE,
                        thread_ctx_stack_capacity := DEFAULT_CONTEXT_CAPACITY,
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

//
// Context usique to the thread.
//

Thread_Ctx :: struct {
    id: int,
    comm: Comm(Message(Data)),
    ctx_stack: [dynamic]Local_Ctx,
    scratch_memory: mem.Scratch,
}

thread_ctx_init :: proc(ctx: ^Thread_Ctx, index,
                        ctx_stack_capacity, comm_channel_count: int,
                        scratch_memory_size: int,
                        shared_ctx: ^Shared_Ctx, allocator: mem.Allocator) {
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
        _pad_gen: [64 - size_of(int)]u8,
        ctxs: [2]^Shared_Ctx, // used to shared new context with threads in left and right branch
        _pad0: [64 - size_of([2]^Shared_Ctx)]u8,
        fini_counter: int,    // Exit reference count
        _pad1: [64 - size_of(int)]u8,
        arrival_counter: int, // ASAP reset counter
        _pad2: [64 - size_of(int)]u8,
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

data_ptr :: #force_inline proc(data: Data, $T: typeid) -> ^T {
    when ODIN_DEBUG {
        if data.type != T do panic("tried to unpack data from the wrong type")
    }
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

get_local_ctx :: #force_inline proc(ctx: Ctx) -> ^Local_Ctx {
    #no_bounds_check {
        return &ctx.thread_ctx.ctx_stack[len(ctx.thread_ctx.ctx_stack) - 1]
    }
}

get_shared_ctx :: proc(ctx: Ctx) -> ^Shared_Ctx {
    return get_local_ctx(ctx).shared_ctx
}

get_scratch_allocator :: proc(ctx: Ctx) -> mem.Allocator {
    return mem.scratch_allocator(&ctx.thread_ctx.scratch_memory)
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
    thread_index := get_thread_index(ctx)

    if thread_index == master_index {
        shared_ctx.sync = runtime.Raw_Slice{raw_data(vals), len(vals)}
    }
    barrier(ctx, .Spin)
    if thread_index != master_index {
        master_vals := transmute([]T)shared_ctx.sync.(runtime.Raw_Slice)
        when ODIN_DEBUG {
            assert(len(vals) == len(master_vals))
        }
        mem.copy(raw_data(vals), raw_data(master_vals), len(vals) * size_of(T))
    }
    barrier(ctx, .Spin)
}

sync_vals_variadic :: proc(ctx: Ctx, master_index: int, $T: typeid, vals: ..^T) {
    shared_ctx := get_shared_ctx(ctx)
    thread_index := get_thread_index(ctx)

    if thread_index == master_index {
        vals_array := make([]T, len(vals), context.temp_allocator)
        for val, idx in vals {
            vals_array[idx] = val^
        }
        shared_ctx.sync = runtime.Raw_Slice{raw_data(vals_array), len(vals_array)}
    }
    barrier(ctx, .Spin)
    if thread_index != master_index {
        master_vals := transmute([]T)shared_ctx.sync.(runtime.Raw_Slice)
        for val, idx in vals {
            val^ = master_vals[idx]
        }
    }
    barrier(ctx, .Spin)
}

sync_vals :: proc{
    sync_vals_slice,
    sync_vals_variadic,
}

sync_val :: proc(ctx: Ctx, master_index: int, val: ^$T) {
    shared_ctx := get_shared_ctx(ctx)
    thread_index := get_thread_index(ctx)

    if thread_index == master_index {
        shared_ctx.sync = cast(rawptr)val
    }
    barrier(ctx, .Spin)
    if thread_index != master_index {
        val^ = (cast(^T)shared_ctx.sync.(rawptr))^
    }
    barrier(ctx, .Spin)
}

// range ///////////////////////////////

Range :: struct {
    it, max: int,
}

range_init :: proc(ctx: Ctx, count: int) -> Range {
    thread_count := get_thread_count(ctx)
    thread_index := get_thread_index(ctx)

    if thread_count >= count {
        return Range{thread_index, min(count, thread_index + 1)}
    }
    step := count / thread_count + (count % thread_count == 0 ? 0 : 1)
    start_idx := thread_index * step
    return Range{start_idx, min(count, start_idx + step)}
}

range_continue :: proc(range: Range) -> bool {
    return range.it < range.max
}

range_next_mut :: proc(range: ^Range) {
    range.it += 1
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

reduce_imut :: proc(ctx: Ctx, values: []$T, op: proc(val, acc: T) -> T) -> T {
    when ODIN_DEBUG { assert(len(values) > 0) }

    shared_ctx := get_shared_ctx(ctx)
    thread_index := get_thread_index(ctx)
    thread_count := get_thread_count(ctx)
    count := len(values)

    r := range_init(ctx, count)
    local_result: T
    if r.it < r.max {
        local_result = values[r.it]
        for i in r.it + 1 ..< r.max {
            local_result = op(values[i], local_result)
        }
    }

    if thread_count == 1 do return local_result

    if thread_index == 0 {
        partials := make([]T, thread_count, get_scratch_allocator(ctx))
        shared_ctx.sync = runtime.Raw_Slice{raw_data(partials), len(partials)}
    }
    barrier(ctx)
    partials := transmute([]T)shared_ctx.sync.(runtime.Raw_Slice)
    if r.it < r.max do partials[thread_index] = local_result
    barrier(ctx)

    effective_count: int
    if thread_count >= count {
        effective_count = count
    } else {
        step := count / thread_count + (count % thread_count == 0 ? 0 : 1)
        effective_count = (count - 1) / step + 1
    }
    result := partials[0]
    for i in 1 ..< effective_count {
        result = op(partials[i], result)
    }
    barrier(ctx)
    return result
}

reduce_mut :: proc(ctx: Ctx, values: []$T, op: proc(val: T, acc: ^T)) -> T {
    when ODIN_DEBUG { assert(len(values) > 0) }

    shared_ctx := get_shared_ctx(ctx)
    thread_index := get_thread_index(ctx)
    thread_count := get_thread_count(ctx)
    count := len(values)

    r := range_init(ctx, count)
    local_result: T
    if r.it < r.max {
        local_result = values[r.it]
        for i in r.it + 1 ..< r.max {
            op(values[i], &local_result)
        }
    }

    if thread_count == 1 do return local_result

    if thread_index == 0 {
        partials := make([]T, thread_count, get_scratch_allocator(ctx))
        shared_ctx.sync = runtime.Raw_Slice{raw_data(partials), len(partials)}
    }
    barrier(ctx)
    partials := transmute([]T)shared_ctx.sync.(runtime.Raw_Slice)
    if r.it < r.max do partials[thread_index] = local_result
    barrier(ctx)

    effective_count: int
    if thread_count >= count {
        effective_count = count
    } else {
        step := count / thread_count + (count % thread_count == 0 ? 0 : 1)
        effective_count = (count - 1) / step + 1
    }
    result := partials[0]
    for i in 1 ..< effective_count {
        op(partials[i], &result)
    }
    barrier(ctx)
    return result
}

Reduce_Op :: enum {
    And,
    Or,
    Add,
    Sub,
    Mul,
    Mut_And,
    Mut_Or,
    Mut_Add,
    Mut_Sub,
    Mut_Mul,
}

reduce_builtins :: proc(ctx: Ctx, values: []$T, $op: Reduce_Op) -> T {
    when op == .And {
        return reduce_imut(ctx, values, proc(val, acc: T) -> T { return val && acc })
    } else when op == .Or {
        return reduce_imut(ctx, values, proc(val, acc: T) -> T { return val || acc })
    } else when op == .Add {
        return reduce_imut(ctx, values, proc(val, acc: T) -> T { return val + acc })
    } else when op == .Sub {
        return reduce_imut(ctx, values, proc(val, acc: T) -> T { return val - acc })
    } else when op == .Mul {
        return reduce_imut(ctx, values, proc(val, acc: T) -> T { return val * acc })
    } else when op == .Mut_And {
        return reduce_mut(ctx, values, proc(val: T, acc: ^T) { acc^ &= val })
    } else when op == .Mut_Or {
        return reduce_mut(ctx, values, proc(val: T, acc: ^T) { acc^ |= val })
    } else when op == .Mut_Add {
        return reduce_mut(ctx, values, proc(val: T, acc: ^T) { acc^ += val })
    } else when op == .Mut_Sub {
        return reduce_mut(ctx, values, proc(val: T, acc: ^T) { acc^ -= val })
    } else when op == .Mut_Mul {
        return reduce_mut(ctx, values, proc(val: T, acc: ^T) { acc^ *= val })
    }
    panic("unreachable")
}

reduce :: proc{
    reduce_imut,
    reduce_mut,
    reduce_builtins,
}

// branch //////////////////////////////

Branch_Ctx :: distinct [2]^Shared_Ctx

branch :: proc(ctx: Ctx, thread_count: int, branch_ctx: ^Branch_Ctx = nil) -> bool {
    parent_local := get_local_ctx(ctx)
    parent_ctx := parent_local.shared_ctx

    my_expected_gen := parent_local.branch_generation + 1

    // =========================================================================
    // LOOP 1: Wait for Reset
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

            node0.thread_count = thread_count
            node0.thread_index_offset = parent_ctx.thread_index_offset
            barrier_init(&node0.barrier, node0.thread_count)
            node0.branch.fini_counter = node0.thread_count
            node0.parent = parent_ctx

            node1.thread_count = parent_ctx.thread_count - thread_count
            node1.thread_index_offset = parent_ctx.thread_index_offset + thread_count
            barrier_init(&node1.barrier, node1.thread_count)
            node1.branch.fini_counter = node1.thread_count
            node1.parent = parent_ctx

            sync.mutex_lock(&parent_ctx.mutex)
            sync.atomic_store_explicit(&parent_ctx.branch.ctxs[0], node0, .Release)
            sync.atomic_store_explicit(&parent_ctx.branch.ctxs[1], node1, .Release)
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
    // CLAIM: Deterministic assignment by parent thread_index
    // =========================================================================
    ctx0 := sync.atomic_load_explicit(&parent_ctx.branch.ctxs[0], .Acquire)
    ctx1 := sync.atomic_load_explicit(&parent_ctx.branch.ctxs[1], .Acquire)

    if branch_ctx != nil do branch_ctx^ = {ctx0, ctx1}

    new_local: Local_Ctx
    if parent_local.thread_index < thread_count {
        new_local = Local_Ctx{ shared_ctx = ctx0, thread_index = parent_local.thread_index, branch_generation = 0 }
    } else {
        new_local = Local_Ctx{ shared_ctx = ctx1, thread_index = parent_local.thread_index - thread_count, branch_generation = 0 }
    }

    parent_local.branch_generation = my_expected_gen

    // =========================================================================
    // ASAP RESET: Last thread to arrive cleans the state and wakes join waiters
    // =========================================================================
    arrivals := sync.atomic_add_explicit(&parent_ctx.branch.arrival_counter, 1, .Relaxed)
    if arrivals == parent_ctx.thread_count - 1 {
        sync.atomic_store_explicit(&parent_ctx.branch.arrival_counter, 0, .Relaxed)

        sync.mutex_lock(&parent_ctx.mutex)
        sync.atomic_store_explicit(&parent_ctx.branch.ctxs[0], nil, .Release)
        sync.atomic_store_explicit(&parent_ctx.branch.ctxs[1], nil, .Release)
        sync.mutex_unlock(&parent_ctx.mutex)

        sync.cond_broadcast(&parent_ctx.cond)
        sync.sema_post(&parent_ctx.branch.join_sema, 2)
    }

    append(&ctx.thread_ctx.ctx_stack, new_local)
    return new_local.shared_ctx == ctx0
}

join :: proc(ctx: Ctx) {
    cur_ctx := get_shared_ctx(ctx)
    parent_ctx := cur_ctx.parent

    prev_count := sync.atomic_sub_explicit(&cur_ctx.branch.fini_counter, 1, .Relaxed)
    if prev_count == 1 {
        sync.sema_wait(&parent_ctx.branch.join_sema)
        release_shared_ctx(ctx.global_ctx, cur_ctx)
    }
    pop(&ctx.thread_ctx.ctx_stack)
}

join_to :: proc(ctx: Ctx, local_ctx: ^Local_Ctx) {
    for get_local_ctx(ctx) != local_ctx {
        join(ctx)
    }
}

// fancy auto-join synctax

Branches_Result :: struct { run: bool, local_ctx: ^Local_Ctx }

branches_end :: proc(ctx: Ctx, br: Branches_Result) {
    join_to(ctx, br.local_ctx)
}

@(deferred_in_out=branches_end)
branches :: proc(ctx: Ctx) -> Branches_Result {
    return Branches_Result{ true, get_local_ctx(ctx) }
}

// task ////////////////////////////////

task :: proc(ctx: Ctx, thread_count: int, comm: ^Comm($I), self: $T, exec: proc(ctx: Ctx, self: T, input: I)) -> (thread_continue: bool) {
    if branch(ctx, thread_count) {
        for {
            data := type_comm_recv(comm) or_break
            exec(ctx, self, data)
        }
        return false
    }
    return true
}

task_shutdown :: proc(ctx: Ctx, comm: ^Comm($I)) {
    if single(ctx) {
        comm_set_closed(comm)
    }
}

tasks :: branches
task_send :: comm_send

// messages ////////////////////////////

//
// A negative thread index will be treated as a ~global thread id. When threads
// communicate outside of the current current context, they use their thread id
// as identifier and make it negative so that the receiver can know.
//

@(private)
get_thread_ctx_by_local_index :: #force_inline proc(ctx: Ctx, shared_ctx: ^Shared_Ctx, index: int) -> ^Thread_Ctx {
    return &ctx.global_ctx.thread_ctxs[shared_ctx.thread_index_offset + index]
}

send_data_parallel_ctx_data :: proc(ctx: Ctx, thread_index: int, data: Data, channel := 0) {
    shared_ctx := get_local_ctx(ctx).shared_ctx
    if thread_index >= 0 {
        receiver_data := get_thread_ctx_by_local_index(ctx, shared_ctx, thread_index)
        comm_send(&receiver_data.comm, Message(Data){get_thread_index(ctx), data}, channel)
    } else {
        receiver_data := &ctx.global_ctx.thread_ctxs[~thread_index]
        comm_send(&receiver_data.comm, Message(Data){~get_thread_id(ctx), data}, channel)
    }
}

send_data_parallel_ctx_poly :: proc(ctx: Ctx, thread_index: int, data: ^$T, channel := 0) {
    send_data_parallel_ctx_data(ctx, thread_index, make_data(data), channel)
}

send_data_shared_ctx_data :: proc(ctx: Ctx, shared_ctx: ^Shared_Ctx, thread_index: int, data: Data, channel := 0) {
    assert(shared_ctx != get_local_ctx(ctx).shared_ctx)
    assert(thread_index >= 0)
    receiver_data := get_thread_ctx_by_local_index(ctx, shared_ctx, thread_index)
    comm_send(&receiver_data.comm, Message(Data){~get_thread_id(ctx), data}, channel)
}

send_data_shared_ctx_poly :: proc(ctx: Ctx, shared_ctx: ^Shared_Ctx, thread_index: int, data: ^$T, channel := 0) {
    send_data_shared_ctx_data(ctx, shared_ctx, thread_index, make_data(data), channel)
}

send_data :: proc{
    send_data_parallel_ctx_data,
    send_data_parallel_ctx_poly,
    send_data_shared_ctx_data,
    send_data_shared_ctx_poly,
}

recv_data_data :: proc(ctx: Ctx, channel := ANY_CHANNEL) -> (Data, int, bool) {
    msg, ok := comm_recv(&ctx.thread_ctx.comm, channel)
    return msg.content, msg.sender_index, ok
}

recv_data_poly :: proc(ctx: Ctx, $T: typeid, channel := ANY_CHANNEL) -> (^T, int, bool) {
    if data, sender_index, ok := recv_data_data(ctx, channel); ok {
        return data_ptr(data, T), sender_index, ok
    }
    return nil, 0, false
}

recv_data :: proc{ recv_data_data, recv_data_poly }

try_recv_data_data :: proc(ctx: Ctx, channel := ANY_CHANNEL) -> (Data, int, bool) {
    msg, ok := comm_try_recv(&ctx.thread_ctx.comm, channel)
    return msg.content, msg.sender_index, ok
}

try_recv_data_poly :: proc(ctx: Ctx, $T: typeid, channel := ANY_CHANNEL) -> (^T, int, bool) {
    if data, sender_index, ok := try_recv_data_data(ctx, channel); ok {
        return data_ptr(data, T), sender_index, ok
    }
    return nil, 0, false
}

try_recv_data :: proc{ try_recv_data_data, try_recv_data_poly }
