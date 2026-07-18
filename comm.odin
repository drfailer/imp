package imp

import "core:sync"
import "core:mem"
import "base:intrinsics"

// Messages ////////////////////////////////////////////////////////////////////

Message :: struct($T: typeid) {
    sender_index: int,
    content: T,
}

Comm :: struct($T: typeid) {
    queue: LockQueue(Message(T)),
    cond: sync.Atomic_Cond,
    mutex: sync.Atomic_Mutex,
}

comm_init :: proc(comm: ^Comm($T)) {
    queue_init(&comm.queue)
}

comm_destroy :: proc(comm: ^Comm($T)) {
    queue_destroy(&comm.queue)
}

comm_send :: proc(comm: ^Comm($T), m: Message(T)) {
    queue_push(&comm.queue, m)
    sync.signal(&comm.cond)
}

comm_recv :: proc(comm: ^Comm($T)) -> Message(T) {
    sync.lock(&comm.mutex)
    defer sync.unlock(&comm.mutex)
    for {
        if m, ok := queue_pop(&comm.queue); ok {
            return m
        }
        sync.wait(&comm.cond, &comm.mutex)
    }
}

comm_try_recv :: proc(comm: ^Comm($T)) -> (m: Message(T), received: bool) {
    return queue_pop(&comm.queue)
}

comm_get_size :: proc(comm: ^Comm($T)) -> int {
    return queue_size(&comm.queue)
}

// Core utilities //////////////////////////////////////////////////////////////

// utility types for supported messages types within the library

@(private)
Comms :: struct {
    datas: Comm(Data),
    jobs: Comm(^Job),
}

@(private)
comms_init :: proc(comms: ^Comms) {
    comm_init(&comms.datas)
    comm_init(&comms.jobs)
}

@(private)
comms_destroy :: proc(comms: ^Comms) {
    comm_destroy(&comms.datas)
    comm_destroy(&comms.jobs)
}

@(private)
comms_send :: proc(comms: ^Comms, m: Message($T)) {
    when T == Data {
        comm_send(&comms.datas, m)
    } else {
        comm_send(&comms.jobs, m)
    }
}

@(private)
comms_recv :: proc(comms: ^Comms, $T: typeid) -> Message(T) {
    when T == Data {
        return comm_recv(&comms.datas)
    } else {
        return comm_recv(&comms.jobs)
    }
}

@(private)
comms_try_recv :: proc(comms: ^Comms, $T: typeid) -> (m: Message(T), received: bool) {
    when T == Data {
        return comm_try_recv(&comms.datas)
    } else {
        return comm_try_recv(&comms.jobs)
    }
}

// API utilities ///////////////////////////////////////////////////////////////

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

//
// Sends a message either to another in the local branch, or to another thread
// globally if the given thread index is negative.
//
@(private)
send_message_parallel_ctx :: proc(ctx: Ctx, thread_index: int, content: $T) {
    shared_ctx := get_local_ctx(ctx).shared_ctx
    if thread_index >= 0 {
        // local send
        receiver_data := get_thread_data_from_index_and_wait_if_not_available(shared_ctx, thread_index)
        comms_send(&receiver_data.comms, Message(T){get_thread_index(ctx), content})
    } else {
        // global send
        receiver_data := &ctx.global_ctx.thread_ctxs[~thread_index]
        comms_send(&receiver_data.comms, Message(T){~get_thread_id(ctx), content})
    }
}

//
// Sends a message to a thread in another branch (using its local index within
// the banch context), The receiver will receive a message with a negative
// thread id allowing for a global response.
//
@(private)
send_message_shared_ctx :: proc(ctx: Ctx, shared_ctx: ^Shared_Ctx, thread_index: int, content: $T) {
    assert(shared_ctx != get_local_ctx(ctx).shared_ctx)
    assert(thread_index >= 0)
    receiver_data := get_thread_data_from_index_and_wait_if_not_available(shared_ctx, thread_index)
    comms_send(&receiver_data.comms, Message(T){~get_thread_id(ctx), content})
}

@(private)
recv_message :: proc(ctx: Ctx, $T: typeid) -> (T, int) {
    msg := comms_recv(&ctx.thread_ctx.comms, T)
    return msg.content, msg.sender_index
}

@(private)
try_recv_message :: proc(ctx: Ctx, $T: typeid) -> (T, int, bool) {
    msg, ok := comms_try_recv(&ctx.thread_data.comms, T)
    if !ok do return {}, 0, false
    return msg.content, msg.sender_index, true
}

//
// Put a message in the message box of the current branch context.
//
@(private)
put_message_parallel_ctx :: proc(ctx: Ctx, content: $T) {
    comms_send(&get_local_ctx(ctx).shared_ctx.comms, Message(T){get_thread_index(ctx), content})
}

//
// Put a message in the message box of another branch context.
//
@(private)
put_message_shared_ctx :: proc(ctx: Ctx, shared_ctx: ^Shared_Ctx, content: $T) {
    assert(get_local_ctx(ctx).shared_ctx != shared_ctx)
    comms_send(&shared_ctx.comms, Message(T){~get_thread_id(ctx), content})
}

@(private)
get_message :: proc(ctx: Ctx, $T: typeid) -> (T, int) {
    msg := comms_recv(&get_local_ctx(ctx).shared_ctx.comms, T)
    return msg.content, msg.sender_index
}

@(private)
try_get_message :: proc(ctx: Ctx, $T: typeid) -> (T, int, bool) {
    msg, ok := comms_try_recv(&get_local_ctx(ctx).shared_ctx.comms, T)
    if !ok do return {}, 0, false
    return msg.content, msg.sender_index, true
}
