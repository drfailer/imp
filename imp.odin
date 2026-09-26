package imp

import "core:thread"
import "core:sync"
import "core:mem"
import "base:intrinsics"
import "base:runtime"
import "prof"

// Globals /////////////////////////////////////////////////////////////////////

GLOBAL_CTX                 : Global_Ctx
@(thread_local) THREAD_CTX : ^Thread_Ctx

init :: proc(thread_count: int,
             comm_channel_count         := 1,
             shared_ctx_pool_capacity   := DEFAULT_SHARED_CTX_POOL_SIZE,
             thread_ctx_stack_capacity  := DEFAULT_CONTEXT_CAPACITY,
             thread_scratch_memory_size := DEFAULT_THREAD_SCRATCH_MEMORY_SIZE) {
    global_ctx_init(&GLOBAL_CTX, thread_count, comm_channel_count, shared_ctx_pool_capacity,
                    thread_ctx_stack_capacity, thread_scratch_memory_size);
}

fini :: proc() {
    global_ctx_destroy(&GLOBAL_CTX);
}

init_thread :: proc(index: int) {
    THREAD_CTX = &GLOBAL_CTX.thread_ctxs[index]
}

Worker_Data :: struct($T: typeid) {
    thread_index: int,
    data: T,
    exec: proc(data: T),
    parent_path: string,
}

//
// Launch the threads and initialize the contexts using the given configuration.
//
launch :: proc(
    thread_count: int,
    exec: proc(data: $I),
    data: I,
    comm_channel_count         := 1,
    shared_ctx_pool_capacity   := DEFAULT_SHARED_CTX_POOL_SIZE,
    thread_ctx_stack_capacity  := DEFAULT_CONTEXT_CAPACITY,
    thread_scratch_memory_size := DEFAULT_THREAD_SCRATCH_MEMORY_SIZE,
) {
    init(thread_count, comm_channel_count, shared_ctx_pool_capacity,
         thread_ctx_stack_capacity, thread_scratch_memory_size)
    defer fini()

    thread_count := len(GLOBAL_CTX.thread_ctxs)
    threads := make([]^thread.Thread, thread_count - 1, context.temp_allocator)
    parent_path := prof.get_parent_path()

    //
    // launch the threads
    //
    for &t, idx in threads {
        wd := Worker_Data(I){idx + 1, data, exec, parent_path}
        t = thread.create_and_start_with_poly_data(wd, proc(wd: Worker_Data(I)) {
            init_thread(wd.thread_index)
            prof.new_thread(wd.parent_path)
            wd.exec(wd.data)
        }, init_context = context)
    }

    //
    // the main thread executes the function too
    //
    init_thread(0)
    exec(data)

    //
    // join the threads
    //
    for &t in threads {
        thread.join(t)
        thread.destroy(t)
    }
}

// Accessors ///////////////////////////////////////////////////////////////////

get_thread_index :: proc() -> int {
    return get_local_ctx().thread_index
}

get_thread_id :: proc() -> int {
    return THREAD_CTX.id
}

get_thread_count :: proc() -> int {
    return get_shared_ctx().thread_count
}

get_local_ctx :: proc() -> ^Local_Ctx {
    return &THREAD_CTX.ctx_stack[len(THREAD_CTX.ctx_stack) - 1]
}

get_shared_ctx :: proc() -> ^Shared_Ctx {
    return get_local_ctx().shared_ctx
}

get_scratch_allocator :: proc() -> mem.Allocator {
    return mem.scratch_allocator(&THREAD_CTX.scratch_memory)
}

// single //////////////////////////////////////////////////////////////////////

single :: proc(index := 0) -> bool {
    return get_thread_index() == index
}

// barrier /////////////////////////////////////////////////////////////////////

barrier :: proc(kind := BarrierKind.Spin) {
    barrier_wait(&get_shared_ctx().barrier, kind)
}

// sync values /////////////////////////////////////////////////////////////////

sync_vals_slice :: proc(master_index: int, vals: []$T) {
    if vals == nil do return

    shared_ctx := get_shared_ctx()
    thread_index := get_thread_index()

    if thread_index == master_index {
        shared_ctx.sync = runtime.Raw_Slice{raw_data(vals), len(vals)}
    }
    barrier(.Spin)
    if thread_index != master_index {
        master_vals := transmute([]T)shared_ctx.sync.(runtime.Raw_Slice)
        when ODIN_DEBUG {
            assert(len(vals) == len(master_vals))
        }
        mem.copy(raw_data(vals), raw_data(master_vals), len(vals) * size_of(T))
    }
    barrier(.Spin)
}

sync_vals_variadic :: proc(master_index: int, $T: typeid, vals: ..^T) {
    shared_ctx := get_shared_ctx()
    thread_index := get_thread_index()

    if thread_index == master_index {
        vals_array := make([]T, len(vals), context.temp_allocator)
        for val, idx in vals {
            vals_array[idx] = val^
        }
        shared_ctx.sync = runtime.Raw_Slice{raw_data(vals_array), len(vals_array)}
    }
    barrier(.Spin)
    if thread_index != master_index {
        master_vals := transmute([]T)shared_ctx.sync.(runtime.Raw_Slice)
        for val, idx in vals {
            val^ = master_vals[idx]
        }
    }
    barrier(.Spin)
}

sync_vals :: proc{
    sync_vals_slice,
    sync_vals_variadic,
}

sync_val :: proc(master_index: int, val: ^$T) {
    shared_ctx := get_shared_ctx()
    thread_index := get_thread_index()

    if thread_index == master_index {
        shared_ctx.sync = cast(rawptr)val
    }
    barrier(.Spin)
    if thread_index != master_index {
        val^ = (cast(^T)shared_ctx.sync.(rawptr))^
    }
    barrier(.Spin)
}

// range ///////////////////////////////////////////////////////////////////////

Range :: struct {
    it, max: int,
}

range_init :: proc(count: int) -> Range {
    thread_count := get_thread_count()
    thread_index := get_thread_index()

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

// reduce //////////////////////////////////////////////////////////////////////

reduce_imut :: proc(values: []$T, op: proc(val, acc: T) -> T) -> T {
    when ODIN_DEBUG { assert(len(values) > 0) }

    shared_ctx := get_shared_ctx()
    thread_index := get_thread_index()
    thread_count := get_thread_count()
    count := len(values)

    r := range_init(count)
    local_result: T
    if r.it < r.max {
        local_result = values[r.it]
        for i in r.it + 1 ..< r.max {
            local_result = op(values[i], local_result)
        }
    }

    if thread_count == 1 do return local_result

    if thread_index == 0 {
        partials := make([]T, thread_count, get_scratch_allocator())
        shared_ctx.sync = runtime.Raw_Slice{raw_data(partials), len(partials)}
    }
    barrier()
    partials := transmute([]T)shared_ctx.sync.(runtime.Raw_Slice)
    if r.it < r.max do partials[thread_index] = local_result
    barrier()

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
    barrier()
    return result
}

reduce_mut :: proc(values: []$T, op: proc(val: T, acc: ^T)) -> T {
    when ODIN_DEBUG { assert(len(values) > 0) }

    shared_ctx := get_shared_ctx()
    thread_index := get_thread_index()
    thread_count := get_thread_count()
    count := len(values)

    r := range_init(count)
    local_result: T
    if r.it < r.max {
        local_result = values[r.it]
        for i in r.it + 1 ..< r.max {
            op(values[i], &local_result)
        }
    }

    if thread_count == 1 do return local_result

    if thread_index == 0 {
        partials := make([]T, thread_count, get_scratch_allocator())
        shared_ctx.sync = runtime.Raw_Slice{raw_data(partials), len(partials)}
    }
    barrier()
    partials := transmute([]T)shared_ctx.sync.(runtime.Raw_Slice)
    if r.it < r.max do partials[thread_index] = local_result
    barrier()

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
    barrier()
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

reduce_builtins :: proc(values: []$T, $op: Reduce_Op) -> T {
    when op == .And {
        return reduce_imut(values, proc(val, acc: T) -> T { return val && acc })
    } else when op == .Or {
        return reduce_imut(values, proc(val, acc: T) -> T { return val || acc })
    } else when op == .Add {
        return reduce_imut(values, proc(val, acc: T) -> T { return val + acc })
    } else when op == .Sub {
        return reduce_imut(values, proc(val, acc: T) -> T { return val - acc })
    } else when op == .Mul {
        return reduce_imut(values, proc(val, acc: T) -> T { return val * acc })
    } else when op == .Mut_And {
        return reduce_mut(values, proc(val: T, acc: ^T) { acc^ &= val })
    } else when op == .Mut_Or {
        return reduce_mut(values, proc(val: T, acc: ^T) { acc^ |= val })
    } else when op == .Mut_Add {
        return reduce_mut(values, proc(val: T, acc: ^T) { acc^ += val })
    } else when op == .Mut_Sub {
        return reduce_mut(values, proc(val: T, acc: ^T) { acc^ -= val })
    } else when op == .Mut_Mul {
        return reduce_mut(values, proc(val: T, acc: ^T) { acc^ *= val })
    }
    panic("unreachable")
}

reduce :: proc{
    reduce_imut,
    reduce_mut,
    reduce_builtins,
}

// branch //////////////////////////////////////////////////////////////////////

Branch_Ctx :: distinct [2]^Shared_Ctx

branch :: proc(thread_count: int, branch_ctx: ^Branch_Ctx = nil) -> bool {
    parent_local := get_local_ctx()
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
            node0 := alloc_shared_ctx(&GLOBAL_CTX)
            node1 := alloc_shared_ctx(&GLOBAL_CTX)

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

    append(&THREAD_CTX.ctx_stack, new_local)
    return new_local.shared_ctx == ctx0
}

join :: proc() {
    cur_ctx := get_shared_ctx()
    parent_ctx := cur_ctx.parent

    prev_count := sync.atomic_sub_explicit(&cur_ctx.branch.fini_counter, 1, .Relaxed)
    if prev_count == 1 {
        sync.sema_wait(&parent_ctx.branch.join_sema)
        release_shared_ctx(&GLOBAL_CTX, cur_ctx)
    }
    pop(&THREAD_CTX.ctx_stack)
}

join_to :: proc(local_ctx: ^Local_Ctx) {
    for get_local_ctx() != local_ctx {
        join()
    }
}

// fancy auto-join synctax

Branches_Result :: struct { run: bool, local_ctx: ^Local_Ctx }

branches_end :: proc(br: Branches_Result) {
    join_to(br.local_ctx)
}

@(deferred_in_out=branches_end)
branches :: proc() -> Branches_Result {
    return Branches_Result{ true, get_local_ctx() }
}

// task ////////////////////////////////////////////////////////////////////////

task :: proc(thread_count: int, comm: ^Comm($I), self: $T, exec: proc(self: T, input: I)) -> (thread_continue: bool) {
    if branch(thread_count) {
        for {
            data := type_comm_recv(comm) or_break
            exec(self, data)
        }
        return false
    }
    return true
}

task_shutdown :: proc(comm: ^Comm($I)) {
    if single() {
        comm_set_closed(comm)
    }
}

tasks :: branches
task_send :: comm_send

// messages ////////////////////////////////////////////////////////////////////

//
// A negative thread index will be treated as a ~global thread id. When threads
// communicate outside of the current current context, they use their thread id
// as identifier and make it negative so that the receiver can know.
//

@(private)
get_thread_ctx_by_local_index :: proc(shared_ctx: ^Shared_Ctx, index: int) -> ^Thread_Ctx {
    return &GLOBAL_CTX.thread_ctxs[shared_ctx.thread_index_offset + index]
}

send_data_parallel_ctx_data :: proc(thread_index: int, data: Data, channel := 0) {
    shared_ctx := get_local_ctx().shared_ctx
    if thread_index >= 0 {
        receiver_data := get_thread_ctx_by_local_index(shared_ctx, thread_index)
        comm_send(&receiver_data.comm, Message(Data){get_thread_index(), data}, channel)
    } else {
        receiver_data := &GLOBAL_CTX.thread_ctxs[~thread_index]
        comm_send(&receiver_data.comm, Message(Data){~get_thread_id(), data}, channel)
    }
}

send_data_parallel_ctx_poly :: proc(thread_index: int, data: $T, channel := 0) {
    send_data_parallel_ctx_data(thread_index, make_data(data), channel)
}

send_data_shared_ctx_data :: proc(shared_ctx: ^Shared_Ctx, thread_index: int, data: Data, channel := 0) {
    assert(shared_ctx != get_local_ctx().shared_ctx)
    assert(thread_index >= 0)
    receiver_data := get_thread_ctx_by_local_index(shared_ctx, thread_index)
    comm_send(&receiver_data.comm, Message(Data){~get_thread_id(), data}, channel)
}

send_data_shared_ctx_poly :: proc(shared_ctx: ^Shared_Ctx, thread_index: int, data: $T, channel := 0) {
    send_data_shared_ctx_data(shared_ctx, thread_index, make_data(data), channel)
}

send_data :: proc{
    send_data_parallel_ctx_data,
    send_data_parallel_ctx_poly,
    send_data_shared_ctx_data,
    send_data_shared_ctx_poly,
}

recv_data_data :: proc(channel := ANY_CHANNEL) -> (Data, int, bool) {
    msg, ok := comm_recv(&THREAD_CTX.comm, channel)
    return msg.content, msg.sender_index, ok
}

recv_data_poly :: proc($T: typeid, channel := ANY_CHANNEL) -> (^T, int, bool) {
    if data, sender_index, ok := recv_data_data(channel); ok {
        return data_ptr(data, T), sender_index, ok
    }
    return nil, 0, false
}

recv_data :: proc{ recv_data_data, recv_data_poly }

try_recv_data_data :: proc(channel := ANY_CHANNEL) -> (Data, int, bool) {
    msg, ok := comm_try_recv(&THREAD_CTX.comm, channel)
    return msg.content, msg.sender_index, ok
}

try_recv_data_poly :: proc($T: typeid, channel := ANY_CHANNEL) -> (^T, int, bool) {
    if data, sender_index, ok := try_recv_data_data(channel); ok {
        return data_ptr(data, T), sender_index, ok
    }
    return nil, 0, false
}

try_recv_data :: proc{ try_recv_data_data, try_recv_data_poly }

// Data ////////////////////////////////////////////////////////////////////////

Data :: struct {
    type: typeid,
    val: rawptr,
}

data_val :: proc(data: Data, $T: typeid) -> T {
    when ODIN_DEBUG {
        if data.type != T do panic("tried to unpack data from the wrong type")
    }
    return cast(T)data.val
}

data_ptr :: proc(data: Data, $T: typeid) -> ^T {
    return data_val(data, ^T)
}

data_type :: proc(data: Data) -> typeid {
    return data.type
}

make_data :: proc(data: $T) -> Data {
    return Data{T, cast(rawptr)data}
}
