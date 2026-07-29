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

Comm :: struct($T: typeid) #align(64) {
    closed:  bool,
    waiters: i32,
    queue:   Lock_Queue(T),
    cond:    sync.Cond,
    mutex:   sync.Mutex,
}

comm_init :: proc(comm: ^Comm($T), allocator := context.allocator) {
    queue_init(&comm.queue, allocator)
}

comm_destroy :: proc(comm: ^Comm($T)) {
    queue_destroy(&comm.queue)
}

comm_set_closed :: proc(comm: ^Comm($T), closed := true) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    intrinsics.atomic_store_explicit(&comm.closed, closed, .Release)
    if intrinsics.atomic_load_explicit(&comm.waiters, .Acquire) > 0 {
        sync.lock(&comm.mutex)
        sync.broadcast(&comm.cond)
        sync.unlock(&comm.mutex)
    }
}

comm_closed :: proc(comm: ^Comm($T)) -> bool {
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

comm_send :: proc(comm: ^Comm($T), data: T) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    queue_push(&comm.queue, data)
    if intrinsics.atomic_load_explicit(&comm.waiters, .Acquire) > 0 {
        sync.guard(&comm.mutex) // signal under lock to prevent lost wakeups
        sync.signal(&comm.cond)
    }
}

comm_recv :: proc(comm: ^Comm($T)) -> (data: T, ok: bool) {
    when COMM_PROFILING_ENABLED do prof.region("comm_recv")
    if data, ok = queue_pop(&comm.queue); ok do return data, true
    if intrinsics.atomic_load_explicit(&comm.closed, .Acquire) do return data, false
    return comm_recv_wait(&comm.queue, &comm.closed, &comm.waiters, &comm.mutex, &comm.cond)
}

@(private)
comm_recv_wait :: proc(
    queue: ^Lock_Queue($T),
    closed: ^bool,
    waiters: ^i32,
    mutex: ^sync.Mutex,
    cond: ^sync.Cond,
) -> (data: T, ok: bool) {
    intrinsics.atomic_add_explicit(waiters, 1, .Release)
    defer intrinsics.atomic_sub_explicit(waiters, 1, .Release)
    if sync.guard(mutex) {
        for {
            when COMM_PROFILING_ENABLED do prof.region("comm_recv_wait")
            if data, ok = queue_pop(queue); ok do return data, true
            if intrinsics.atomic_load_explicit(closed, .Acquire) do return data, false
            sync.wait(cond, mutex)
        }
    }
    return data, false
}

comm_try_recv :: proc(comm: ^Comm($T)) -> (data: T, received: bool) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    return queue_pop(&comm.queue)
}


// Core utilities //////////////////////////////////////////////////////////////

ANY_CHANNEL :: -1

Comms :: struct($T: typeid) #align(64) {
    closed:   bool,
    waiters:  i32,
    channels: [dynamic]Comm(T),
    mutex:    sync.Mutex,
    cond:     sync.Cond,
}

comms_init :: proc(comms: ^Comms($T), channel_count: int, allocator := context.allocator) {
    comms.channels = make([dynamic]Comm(T), channel_count, allocator)
    for &channel in comms.channels {
        comm_init(&channel, allocator)
    }
}

type_comms_init :: proc(comms: ^Comms($U), allocator := context.allocator) where intrinsics.type_is_union(U) {
    comms_init(comms, intrinsics.type_union_variant_count(U), allocator)
}

comms_destroy :: proc(comms: ^Comms($T)) {
    for &channel in comms.channels {
        comm_destroy(&channel)
    }
    delete(comms.channels)
}

comms_set_closed :: proc(comms: ^Comms($T), closed := true) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    for &channel in comms.channels {
        comm_set_closed(&channel, closed)
    }
    intrinsics.atomic_store_explicit(&comms.closed, closed, .Release)
    if intrinsics.atomic_load_explicit(&comms.waiters, .Acquire) > 0 {
        sync.guard(&comms.mutex)
        sync.broadcast(&comms.cond)
    }
}

comms_is_closed :: proc(comms: ^Comms($T)) -> bool {
    return intrinsics.atomic_load_explicit(&comms.closed, .Acquire)
}

comms_wait_open :: proc(comms: ^Comms($T)) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    if intrinsics.atomic_load_explicit(&comms.closed, .Acquire) do return
    if sync.guard(&comms.mutex) {
        intrinsics.atomic_add_explicit(&comms.waiters, 1, .Release)
        for intrinsics.atomic_load_explicit(&comms.closed, .Acquire) {
            sync.wait(&comms.cond, &comms.mutex)
        }
        intrinsics.atomic_sub_explicit(&comms.waiters, 1, .Release)
    }
}

comms_send :: proc(comms: ^Comms($T), data: T, channel := 0) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    comm_send(&comms.channels[channel], data)
    // this is for global waiters
    if intrinsics.atomic_load(&comms.waiters) > 0 {
        sync.guard(&comms.mutex)
        sync.signal(&comms.cond)
    }
}

type_comms_send :: proc(comms: ^Comms($U), data: $T) where intrinsics.type_is_union(U) {
    comms_send(comms, data, intrinsics.type_variant_index_of(U, T))
}

comms_recv :: proc(comms: ^Comms($T), channel := ANY_CHANNEL, thread_index := 0) -> (data: T, received: bool) {
    if channel != ANY_CHANNEL {
        when COMM_PROFILING_ENABLED do prof.region("comms_recv")
        return comm_recv(&comms.channels[channel])
    }

    channel_count := len(comms.channels)
    when COMM_PROFILING_ENABLED do prof.region("comms_recv")
    start_idx := thread_index % channel_count
    for i in 0..<channel_count {
        idx := (start_idx + i) % channel_count
        if data, ok := comm_try_recv(&comms.channels[idx]); ok {
            return data, true
        }
    }
    if intrinsics.atomic_load_explicit(&comms.closed, .Acquire) do return data, false

    return comms_recv_wait(comms, thread_index)
}

@(private)
comms_recv_wait :: proc(comms: ^Comms($T), thread_index: int) -> (data: T, ok: bool) {
    channel_count := len(comms.channels)
    intrinsics.atomic_add_explicit(&comms.waiters, 1, .Release)
    defer intrinsics.atomic_sub_explicit(&comms.waiters, 1, .Release)
    if sync.guard(&comms.mutex) {
        for {
            when COMM_PROFILING_ENABLED do prof.region("comms_recv_wait")
            start_idx := thread_index % channel_count
            for i in 0..<channel_count {
                idx := (start_idx + i) % channel_count
                if data, ok = comm_try_recv(&comms.channels[idx]); ok do return data, true
            }
            if intrinsics.atomic_load_explicit(&comms.closed, .Acquire) do return data, false
            sync.wait(&comms.cond, &comms.mutex)
        }
    }
    return data, false
}

type_comms_recv :: proc(comms: ^Comms($U), $T: typeid) -> (data: T, received: bool)
    where intrinsics.type_is_union(U) {
    udata := comms_recv(comms, intrinsics.type_variant_index_of(U, T)) or_return
    return udata.(T), true
}

comms_try_recv :: proc(comms: ^Comms($T), channel := ANY_CHANNEL, thread_index := 0) -> (data: T, received: bool) {
    if channel != ANY_CHANNEL {
        return comm_try_recv(&comms.channels[channel])
    }
    channel_count := len(comms.channels)
    start_idx := thread_index % channel_count
    for i in 0..<channel_count {
        idx := (start_idx + i) % channel_count
        if data, ok := comm_try_recv(&comms.channels[idx]); ok {
            return data, true
        }
    }
    return {}, false
}

type_comms_try_recv :: proc(comms: ^Comms($U), $T: typeid) -> (data: T, received: bool)
    where intrinsics.type_is_union(U) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    udata := comms_try_recv(comms, intrinsics.type_variant_index_of(U, T)) or_return
    return udata.(T), true
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
