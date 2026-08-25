package imp

import "prof"
import "core:sync"
import "core:mem"
import "base:intrinsics"
import "base:runtime"

COMM_PROFILING_ENABLED :: #config(IMP_COMM_PROFILING_ENABLED, false)

// Messages ////////////////////////////////////////////////////////////////////

Message :: struct($T: typeid) {
    sender_index: int,
    content: T,
}

// Comm ////////////////////////////////////////////////////////////////////////

ANY_CHANNEL :: -1

Comm :: struct($T: typeid) #align(64) {
    closed:   bool,
    waiters:  i32,
    channels: [dynamic]Lock_Queue(T),
    // TODO: try to replace this with a semaphore and use a group wakeup strategy
    mutex:    sync.Mutex,
    cond:     sync.Cond,
}

comm_init :: proc(comm: ^Comm($T), channel_count := 1, allocator := context.allocator) {
    comm.channels = make([dynamic]Lock_Queue(T), channel_count, allocator)
    for &channel in comm.channels {
        queue_init(&channel, allocator)
    }
}

type_comm_init :: proc(comm: ^Comm($U), allocator := context.allocator) {
    when intrinsics.type_is_union(U) {
        comm_init(comm, intrinsics.type_union_variant_count(U), allocator)
    } else {
        comm_init(comm, 1, allocator)
    }
}

comm_destroy :: proc(comm: ^Comm($T)) {
    for &channel in comm.channels {
        queue_destroy(&channel)
    }
    delete(comm.channels)
}

comm_set_closed :: proc(comm: ^Comm($T), closed := true) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    intrinsics.atomic_store_explicit(&comm.closed, closed, .Release)
    if intrinsics.atomic_load_explicit(&comm.waiters, .Acquire) > 0 {
        sync.guard(&comm.mutex)
        sync.broadcast(&comm.cond)
    }
}

comm_is_closed :: proc(comm: ^Comm($T)) -> bool {
    return intrinsics.atomic_load_explicit(&comm.closed, .Acquire)
}

comm_wait_open :: proc(comm: ^Comm($T)) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    if intrinsics.atomic_load_explicit(&comm.closed, .Acquire) do return
    if sync.guard(&comm.mutex) {
        intrinsics.atomic_add_explicit(&comm.waiters, 1, .Release)
        for intrinsics.atomic_load_explicit(&comm.closed, .Acquire) {
            sync.wait(&comm.cond, &comm.mutex)
        }
        intrinsics.atomic_sub_explicit(&comm.waiters, 1, .Release)
    }
}

comm_send :: proc(comm: ^Comm($T), data: T, channel := 0) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    queue_push(&comm.channels[channel], data)
    if intrinsics.atomic_load(&comm.waiters) > 0 {
        sync.guard(&comm.mutex)
        sync.signal(&comm.cond)
    }
}

type_comm_send :: proc(comm: ^Comm($U), data: $T) {
    when intrinsics.type_is_union(U) {
        comm_send(comm, data, intrinsics.type_variant_index_of(U, T))
    } else {
        comm_send(comm, data, 0)
    }
}

comm_recv :: proc(comm: ^Comm($T), channel := ANY_CHANNEL, thread_index := 0) -> (data: T, received: bool) {
    channel_count := len(comm.channels)
    channel := channel if channel_count > 1 else 0
    if channel != ANY_CHANNEL {
        when COMM_PROFILING_ENABLED do prof.region("comm_recv")
        if data, ok := queue_pop(&comm.channels[channel]); ok do return data, true
    } else {
        when COMM_PROFILING_ENABLED do prof.region("comm_recv")
        start_idx := thread_index % channel_count
        for i in 0..<channel_count {
            idx := (start_idx + i) % channel_count
            if data, ok := queue_pop(&comm.channels[idx]); ok do return data, true
        }
    }
    if intrinsics.atomic_load_explicit(&comm.closed, .Acquire) do return data, false

    return comm_recv_wait(comm, channel, thread_index)
}

@(private)
comm_recv_wait :: proc(comm: ^Comm($T), channel, thread_index: int) -> (data: T, ok: bool) {
    channel_count := len(comm.channels)
    intrinsics.atomic_add_explicit(&comm.waiters, 1, .Release)
    defer intrinsics.atomic_sub_explicit(&comm.waiters, 1, .Release)
    if sync.guard(&comm.mutex) {
        for {
            when COMM_PROFILING_ENABLED do prof.region("comm_recv_wait")
            if channel != ANY_CHANNEL {
                if data, ok = queue_pop(&comm.channels[channel]); ok do return data, true
            } else {
                start_idx := thread_index % channel_count
                for i in 0..<channel_count {
                    idx := (start_idx + i) % channel_count
                    if data, ok = queue_pop(&comm.channels[idx]); ok do return data, true
                }
            }
            if intrinsics.atomic_load_explicit(&comm.closed, .Acquire) do return data, false
            sync.wait(&comm.cond, &comm.mutex)
        }
    }
    return data, false
}

type_comm_recv :: proc(comm: ^Comm($U), $T: typeid) -> (data: T, received: bool) {
    when intrinsics.type_is_union(U) {
        udata := comm_recv(comm, intrinsics.type_variant_index_of(U, T)) or_return
        return udata.(T), true
    } else {
        udata := comm_recv(comm, 0) or_return
        return udata.(T), true
    }
}

comm_try_recv :: proc(comm: ^Comm($T), channel := ANY_CHANNEL, thread_index := 0) -> (data: T, received: bool) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    if channel != ANY_CHANNEL {
        return queue_pop(&comm.channels[channel])
    }
    channel_count := len(comm.channels)
    start_idx := thread_index % channel_count
    for i in 0..<channel_count {
        idx := (start_idx + i) % channel_count
        if data, ok := queue_pop(&comm.channels[idx]); ok {
            return data, true
        }
    }
    return {}, false
}

type_comm_try_recv :: proc(comm: ^Comm($U), $T: typeid) -> (data: T, received: bool) {
    when intrinsics.type_is_union(U) {
        udata := comm_try_recv(comm, intrinsics.type_variant_index_of(U, T)) or_return
        return udata.(T), true
    } else {
        udata := comm_try_recv(comm, 0) or_return
        return udata.(T), true
    }
}

// assembly line ///////////////////////////////////////////////////////////////

Assembly_Line_Slot :: struct($T: typeid) {
    value: T,
    index: int,
}

Assembly_Line :: struct($T: typeid, $S: int) #align(64) {
    head: int,
    _pad1: [64 - size_of(int)]u8,
    tail: int,
    _pad2: [64 - size_of(int)]u8,
    stop: bool,
    _pad3: [64 - size_of(bool)]u8,
    data: [S]Assembly_Line_Slot(T),
}

assembly_line_init :: proc(line: ^Assembly_Line($T, $S)) {
    #assert(((S - 1) & S) == 0)
    for &d in line.data {
        d.index = -1
    }
}

assembly_line_set_stop :: proc(line: ^Assembly_Line($T, $S), stop := true) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    sync.atomic_store_explicit(&line.stop, stop, .Release)
}

assembly_line_put :: proc(line: ^Assembly_Line($T, $S), data: T) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    head := sync.atomic_add_explicit(&line.head, 1, .Relaxed)
    slot := &line.data[head & (S - 1)]

    // wait for the slot to be free
    backoff := SPIN_BACKOFF_INIT
    for sync.atomic_load_explicit(&slot.index, .Acquire) != -1 {
        spin_backoff(&backoff)
    }
    slot.value = data
    sync.atomic_store_explicit(&slot.index, head, .Release)
}

assembly_line_get :: proc(line: ^Assembly_Line($T, $S)) -> (T, bool) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    tail := sync.atomic_add_explicit(&line.tail, 1, .Relaxed)
    slot := &line.data[tail & (S - 1)]

    // wait for the slot to be ready
    backoff := SPIN_BACKOFF_INIT
    for sync.atomic_load_explicit(&slot.index, .Acquire) != tail {
        if sync.atomic_load_explicit(&line.stop, .Acquire) {
            // Check if our ticket was never claimed by a writer.
            // If tail >= head, no writer is coming for this slot.
            curr_head := sync.atomic_load_explicit(&line.head, .Acquire)
            if tail >= curr_head {
                return T{}, false
            }
        }
        spin_backoff(&backoff)
    }
    value := slot.value
    sync.atomic_store_explicit(&slot.index, -1, .Release)
    return value, true
}
