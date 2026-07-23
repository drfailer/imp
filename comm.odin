package imp

import "core:sync"
import "core:mem"
import "base:intrinsics"
import "base:runtime"

// Messages ////////////////////////////////////////////////////////////////////

Message :: struct($T: typeid) {
    sender_index: int,
    content: T,
}

// Comm ////////////////////////////////////////////////////////////////////////

Comm :: struct($T: typeid) {
    closed:  bool, // Atomic flag
    waiters: i32,  // Atomic counter for sleeping threads
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
    intrinsics.atomic_store(&comm.closed, closed)
    if intrinsics.atomic_load(&comm.waiters) > 0 {
        sync.lock(&comm.mutex)
        sync.broadcast(&comm.cond)
        sync.unlock(&comm.mutex)
    }
}

comm_closed :: proc(comm: ^Comm($T)) -> bool {
    return intrinsics.atomic_load(&comm.closed)
}

comm_wait_open :: proc(comm: ^Comm($T)) {
    for intrinsics.atomic_load(&comm.closed) {
        intrinsics.atomic_add(&comm.waiters, 1)
        sync.lock(&comm.mutex)

        if intrinsics.atomic_load(&comm.closed) {
            sync.wait(&comm.cond, &comm.mutex)
        }

        sync.unlock(&comm.mutex)
        intrinsics.atomic_sub(&comm.waiters, 1)
    }
}

comm_send :: proc(comm: ^Comm($T), m: T) {
    queue_push(&comm.queue, m)
    if intrinsics.atomic_load(&comm.waiters) > 0 {
        sync.lock(&comm.mutex)
        sync.signal(&comm.cond)
        sync.unlock(&comm.mutex)
    }
}

comm_wait :: proc(comm: ^Comm($T)) -> bool {
    if intrinsics.atomic_load(&comm.closed) do return false

    intrinsics.atomic_add(&comm.waiters, 1)
    sync.lock(&comm.mutex)

    if !intrinsics.atomic_load(&comm.closed) {
        sync.wait(&comm.cond, &comm.mutex)
    }

    sync.unlock(&comm.mutex)
    intrinsics.atomic_sub(&comm.waiters, 1)

    return !intrinsics.atomic_load(&comm.closed)
}

comm_recv :: proc(comm: ^Comm($T)) -> (m: T, ok: bool) {
    if m, ok = queue_pop(&comm.queue); ok {
        return m, true
    }

    // slow-path: wait[or message
    for {
        if intrinsics.atomic_load(&comm.closed) do return {}, false

        intrinsics.atomic_add(&comm.waiters, 1)
        sync.lock(&comm.mutex)

        // double-check
        if m, ok = queue_pop(&comm.queue); ok {
            sync.unlock(&comm.mutex)
            intrinsics.atomic_sub(&comm.waiters, 1)
            return m, true
        }

        if !intrinsics.atomic_load(&comm.closed) {
            sync.wait(&comm.cond, &comm.mutex)
        }

        sync.unlock(&comm.mutex)
        intrinsics.atomic_sub(&comm.waiters, 1)

        if m, ok = queue_pop(&comm.queue); ok {
            return m, true
        }
    }
}

comm_try_recv :: proc(comm: ^Comm($T)) -> (m: T, received: bool) {
    return queue_pop(&comm.queue)
}


// Core utilities //////////////////////////////////////////////////////////////

ANY_CHANNEL :: -1

Comms :: struct($T: typeid) {
    closed:   bool,
    waiters:  i32,
    channels: [dynamic]Comm(T),
    mutex:    sync.Mutex,
    cond:     sync.Cond,
}

comms_init_channels :: proc(comms: ^Comms($T), channel_count: int, allocator := context.allocator) {
    comms.channels = make([dynamic]Comm(T), channel_count, allocator)
    for &channel in comms.channels {
        comm_init(&channel, allocator)
    }
}

comms_init_union :: proc(comms: ^Comms($T), allocator := context.allocator) where intrinsics.type_is_union(T) {
    type_info := type_info_of(T)
    comms_init(comms, len(type_info.variant.(runtime.Type_Info_Union).variants), allocator)
}

comms_init :: proc{ comms_init_channels, comms_init_union }

comms_destroy :: proc(comms: ^Comms($T)) {
    for &channel in comms.channels {
        comm_destroy(&channel)
    }
    delete(comms.channels)
}

comms_set_closed :: proc(comms: ^Comms($T), closed := true) {
    for &channel in comms.channels {
        comm_set_closed(&channel, closed)
    }

    intrinsics.atomic_store(&comms.closed, closed)

    if intrinsics.atomic_load(&comms.waiters) > 0 {
        sync.lock(&comms.mutex)
        sync.broadcast(&comms.cond)
        sync.unlock(&comms.mutex)
    }
}

comms_is_closed :: proc(comms: ^Comms($T)) -> bool {
    return intrinsics.atomic_load(&comms.closed)
}

comms_wait_open :: proc(comms: ^Comms($T)) {
    for intrinsics.atomic_load(&comms.closed) {
        intrinsics.atomic_add(&comms.waiters, 1)
        sync.lock(&comms.mutex)

        if intrinsics.atomic_load(&comms.closed) {
            sync.wait(&comms.cond, &comms.mutex)
        }

        sync.unlock(&comms.mutex)
        intrinsics.atomic_sub(&comms.waiters, 1)
    }
}

comms_send :: proc(comms: ^Comms($T), data: T, channel := 0) {
    comm_send(&comms.channels[channel], data)

    // wake up any global ANY_CHANNEL waiters
    if intrinsics.atomic_load(&comms.waiters) > 0 {
        sync.lock(&comms.mutex)
        sync.signal(&comms.cond)
        sync.unlock(&comms.mutex)
    }
}

comms_wait :: proc(comms: ^Comms($T)) -> bool {
    if intrinsics.atomic_load(&comms.closed) do return false

    intrinsics.atomic_add(&comms.waiters, 1)
    sync.lock(&comms.mutex)

    if !intrinsics.atomic_load(&comms.closed) {
        sync.wait(&comms.cond, &comms.mutex)
    }

    sync.unlock(&comms.mutex)
    intrinsics.atomic_sub(&comms.waiters, 1)

    return !intrinsics.atomic_load(&comms.closed)
}

comms_recv :: proc(comms: ^Comms($T), channel := ANY_CHANNEL, thread_index := 0) -> (data: T, received: bool) {
    if channel != ANY_CHANNEL {
        return comm_recv(&comms.channels[channel])
    }

    channel_count := len(comms.channels)
    if channel_count == 0 do return {}, false

    // thread can give their index to manage how channels are scanned
    start_idx := thread_index % channel_count

    for i in 0..<channel_count {
        idx := (start_idx + i) % channel_count
        if data, ok := comm_try_recv(&comms.channels[idx]); ok {
            return data, true
        }
    }

    // slow-path: prepare to sleep
    for {
        if intrinsics.atomic_load(&comms.closed) do return {}, false

        intrinsics.atomic_add(&comms.waiters, 1)
        sync.lock(&comms.mutex)

        // double-check
        found := false
        for i in 0..<channel_count {
            idx := (start_idx + i) % channel_count
            if d, ok := comm_try_recv(&comms.channels[idx]); ok {
                data = d
                found = true
                break
            }
        }

        if found {
            sync.unlock(&comms.mutex)
            intrinsics.atomic_sub(&comms.waiters, 1)
            return data, true
        }

        if !intrinsics.atomic_load(&comms.closed) {
            sync.wait(&comms.cond, &comms.mutex)
        }

        sync.unlock(&comms.mutex)
        intrinsics.atomic_sub(&comms.waiters, 1)

        for i in 0..<channel_count {
            idx := (start_idx + i) % channel_count
            if data, ok := comm_try_recv(&comms.channels[idx]); ok {
                return data, true
            }
        }
    }
}

comms_try_recv :: proc(comms: ^Comms($T), channel := ANY_CHANNEL, thread_index := 0) -> (data: T, received: bool) {
    if channel != ANY_CHANNEL {
        return comm_try_recv(&comms.channels[channel])
    }

    channel_count := len(comms.channels)
    if channel_count == 0 do return {}, false

    start_idx := thread_index % channel_count

    for i in 0..<channel_count {
        idx := (start_idx + i) % channel_count
        if data, ok := comm_try_recv(&comms.channels[idx]); ok {
            return data, true
        }
    }

    return {}, false
}

// assembly line ///////////////////////////////////////////////////////////////

Assembly_Line_Slot :: struct($T: typeid) {
    value: T,
    index: int,
}

Assembly_Line :: struct($T: typeid, $S: int) #align(64) {
    data: [S]Assembly_Line_Slot(T),
    _pad0: [64]u8,
    head: int,
    _pad1: [64]u8,
    tail: int,
    _pad2: [64]u8,
    stop: bool,
}

assembly_line_init :: proc(line: ^Assembly_Line($T, $S)) {
    #assert(((S - 1) & S) == 0)
    for &d in line.data {
        d.index = -1
    }
}

assembly_line_set_stop :: proc(line: ^Assembly_Line($T, $S), stop := true) {
    sync.atomic_store_explicit(&line.stop, stop, .Release)
}

assembly_line_put :: proc(line: ^Assembly_Line($T, $S), data: T) {
    head := sync.atomic_add_explicit(&line.head, 1, .Relaxed)
    slot := &line.data[head & (S - 1)]

    // wait for the slot to be free
    for sync.atomic_load_explicit(&slot.index, .Acquire) != -1 {
        intrinsics.cpu_relax()
    }
    slot.value = data
    sync.atomic_store_explicit(&slot.index, head, .Release)
}

assembly_line_get :: proc(line: ^Assembly_Line($T, $S)) -> (T, bool) {
    tail := sync.atomic_add_explicit(&line.tail, 1, .Relaxed)
    slot := &line.data[tail & (S - 1)]

    // wait for the slot to be ready
    for sync.atomic_load_explicit(&slot.index, .Acquire) != tail {
        if sync.atomic_load_explicit(&line.stop, .Acquire) {
            // Check if our ticket was never claimed by a writer.
            // If tail >= head, no writer is coming for this slot.
            curr_head := sync.atomic_load_explicit(&line.head, .Acquire)
            if tail >= curr_head {
                return T{}, false
            }
        }
        intrinsics.cpu_relax()
    }
    value := slot.value
    sync.atomic_store_explicit(&slot.index, -1, .Release)
    return value, true
}
