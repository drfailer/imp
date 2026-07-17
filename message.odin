package imp

import "core:sync"
import "core:mem"
import "base:intrinsics"

// Messages ////////////////////////////////////////////////////////////////////

Message :: struct($T: typeid) {
    sender_index: int,
    content: T,
}

MessageBox :: struct($T: typeid) {
    queue: LockQueue(Message(T)),
    cond: sync.Cond,
    mutex: sync.Mutex,
}

message_box_init :: proc(mb: ^MessageBox($T)) {
    queue_init(&mb.queue)
}

message_box_destroy :: proc(mb: ^MessageBox($T)) {
    queue_destroy(&mb.queue)
}

message_box_send :: proc(mb: ^MessageBox($T), m: Message(T)) {
    queue_push(&mb.queue, m)
    sync.cond_signal(&mb.cond)
}

message_box_recv :: proc(mb: ^MessageBox($T)) -> Message(T) {
    sync.lock(&mb.mutex)
    defer sync.unlock(&mb.mutex)
    for {
        if m, ok := queue_pop(&mb.queue); ok {
            return m
        }
        sync.cond_wait(&mb.cond, &mb.mutex)
    }
}

message_box_try_recv :: proc(mb: ^MessageBox($T)) -> (m: Message(T), received: bool) {
    return queue_pop(&mb.queue)
}

message_box_get_size :: proc(mb: ^MessageBox($T)) -> int {
    return queue_size(&mb.queue)
}

// Core utilities //////////////////////////////////////////////////////////////

// utility types for supported messages types within the library

@(private)
MessageBoxes :: struct {
    datas: MessageBox(Data),
    jobs: MessageBox(^Job),
}

@(private)
message_boxes_init :: proc(mbs: ^MessageBoxes) {
    message_box_init(&mbs.datas)
    message_box_init(&mbs.jobs)
}

@(private)
message_boxes_destroy :: proc(mbs: ^MessageBoxes) {
    message_box_destroy(&mbs.datas)
    message_box_destroy(&mbs.jobs)
}

@(private)
message_boxes_send :: proc(mbs: ^MessageBoxes, m: Message($T)) {
    when T == Data {
        message_box_send(&mbs.datas, m)
    } else {
        message_box_send(&mbs.jobs, m)
    }
}

@(private)
message_boxes_recv :: proc(mbs: ^MessageBoxes, $T: typeid) -> Message(T) {
    when T == Data {
        return message_box_recv(&mbs.datas)
    } else {
        return message_box_recv(&mbs.jobs)
    }
}

@(private)
message_boxes_try_recv :: proc(mbs: ^MessageBoxes, $T: typeid) -> (m: Message(T), received: bool) {
    when T == Data {
        return message_box_try_recv(&mbs.datas)
    } else {
        return message_box_try_recv(&mbs.jobs)
    }
}

// API utilities ///////////////////////////////////////////////////////////////

@(private)
get_thread_data_from_index_and_wait_if_not_available :: proc(ctx: ^Shared_Ctx, index: int) -> ^Thread_Data {
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
send_message_parallel_ctx :: proc(ctx: Parallel_Ctx, thread_index: int, content: $T) {
    shared_ctx := get_local_ctx(ctx).shared_ctx
    if thread_index >= 0 {
        // local send
        receiver_data := get_thread_data_from_index_and_wait_if_not_available(shared_ctx, thread_index)
        message_boxes_send(&receiver_data.message_boxes, Message(T){get_thread_index(ctx), content})
    } else {
        // global send
        receiver_data := &ctx.line.threads[~thread_index]
        message_boxes_send(&receiver_data.message_boxes, Message(T){~get_thread_id(ctx), content})
    }
}

//
// Sends a message to a thread in another branch (using its local index within
// the banch context), The receiver will receive a message with a negative
// thread id allowing for a global response.
//
@(private)
send_message_shared_ctx :: proc(ctx: Parallel_Ctx, shared_ctx: ^Shared_Ctx, thread_index: int, content: $T) {
    assert(shared_ctx != get_local_ctx(ctx).shared_ctx)
    assert(thread_index >= 0)
    receiver_data := get_thread_data_from_index_and_wait_if_not_available(shared_ctx, thread_index)
    message_boxes_send(&receiver_data.message_boxes, Message(T){~get_thread_id(ctx), content})
}

@(private)
recv_message :: proc(ctx: Parallel_Ctx, $T: typeid) -> (T, int) {
    msg := message_boxes_recv(&ctx.thread_data.message_boxes, T)
    return msg.content, msg.sender_index
}

@(private)
try_recv_message :: proc(ctx: Parallel_Ctx, $T: typeid) -> (T, int, bool) {
    msg, ok := message_boxes_try_recv(&ctx.thread_data.message_boxes, T)
    if !ok do return {}, 0, false
    return msg.content, msg.sender_index, true
}

//
// Put a message in the message box of the current branch context.
//
@(private)
put_message_parallel_ctx :: proc(ctx: Parallel_Ctx, content: $T) {
    message_boxes_send(&get_local_ctx(ctx).shared_ctx.message_boxes, Message(T){get_thread_index(ctx), content})
}

//
// Put a message in the message box of another branch context.
//
@(private)
put_message_shared_ctx :: proc(ctx: Parallel_Ctx, shared_ctx: ^Shared_Ctx, content: $T) {
    assert(get_local_ctx(ctx).shared_ctx != shared_ctx)
    message_boxes_send(&shared_ctx.message_boxes, Message(T){~get_thread_id(ctx), content})
}

@(private)
get_message :: proc(ctx: Parallel_Ctx, $T: typeid) -> (T, int) {
    msg := message_boxes_recv(&get_local_ctx(ctx).shared_ctx.message_boxes, T)
    return msg.content, msg.sender_index
}

@(private)
try_get_message :: proc(ctx: Parallel_Ctx, $T: typeid) -> (T, int, bool) {
    msg, ok := message_boxes_try_recv(&get_local_ctx(ctx).shared_ctx.message_boxes, T)
    if !ok do return {}, 0, false
    return msg.content, msg.sender_index, true
}
