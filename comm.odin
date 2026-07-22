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
    closed: bool,
    // queue: LockQueue(T),
    queue: MPMC_Queue(T, 1024),
    cond: sync.Cond,
    mutex: sync.Mutex,
}

comm_init :: proc(comm: ^Comm($T), allocator := context.allocator) {
    queue_init(&comm.queue, allocator)
}

comm_destroy :: proc(comm: ^Comm($T)) {
    queue_destroy(&comm.queue)
}

comm_open :: proc(comm: ^Comm($T)) {
    sync.lock(&comm.mutex)
    comm.closed = false
    sync.unlock(&comm.mutex)
    sync.broadcast(&comm.cond)
}

comm_set_closed :: proc(comm: ^Comm($T), closed := true) {
    sync.lock(&comm.mutex)
    comm.closed = closed
    sync.unlock(&comm.mutex)
    sync.broadcast(&comm.cond)
}

comm_closed :: proc(comm: ^Comm($T)) -> bool {
    sync.guard(&comm.mutex)
    return comm.closed
}

comm_wait_open :: proc(comm: ^Comm($T)) {
    sync.guard(&comm.mutex)
    for comm.closed {
        sync.wait(&comm.cond, &comm.mutex)
    }
}

comm_send :: proc(comm: ^Comm($T), m: T) {
    queue_push(&comm.queue, m)
    sync.signal(&comm.cond)
}

comm_wait :: proc(comm: ^Comm($T)) -> bool {
    sync.guard(&comm.mutex)
    if comm.closed do return false
    sync.wait(&comm.cond, &comm.mutex)
    return comm.closed == false
}

comm_recv :: proc(comm: ^Comm($T)) -> (m: T, ok: bool) {
    for {
        if m, ok = queue_pop(&comm.queue); ok {
            return m, true
        }
        comm_wait(comm) or_return
    }
}

comm_try_recv :: proc(comm: ^Comm($T)) -> (m: T, received: bool) {
    return queue_pop(&comm.queue)
}

// Core utilities //////////////////////////////////////////////////////////////

ANY_CHANNEL :: -1

Comms :: struct($T: typeid) {
    closed: bool,
    channels: [dynamic]Comm(T),
    mutex: sync.Mutex,
    cond: sync.Cond,
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
    if sync.guard(&comms.mutex) {
        comms.closed = closed
    }
    sync.broadcast(&comms.cond)
}

comms_is_closed :: proc(comms: ^Comms($T)) -> bool {
    sync.guard(&comms.mutex)
    return comms.closed
}

comms_wait_open :: proc(comms: ^Comms($T)) {
    sync.guard(&comms.mutex)
    for comms.closed {
        sync.wait(&comms.cond, &comms.mutex)
    }
}

comms_send :: proc(comms: ^Comms($T), data: T, channel := 0) {
    comm_send(&comms.channels[channel], data)
    sync.signal(&comms.cond)
}

comms_wait :: proc(comms: ^Comms($T)) -> bool {
    sync.guard(&comms.mutex)
    if comms.closed do return false
    sync.wait(&comms.cond, &comms.mutex)
    return comms.closed == false
}

comms_recv :: proc(comms: ^Comms($T), channel := ANY_CHANNEL) -> (data: T, received: bool) {
    if channel == ANY_CHANNEL {
        for {
            for &channel in comms.channels {
                if data, ok := comm_try_recv(&channel); ok do return data, true
            }
            comms_wait(comms) or_return
        }
    }
    return comm_recv(&comms.channels[channel])
}

comms_try_recv :: proc(comms: ^Comms($T), channel := ANY_CHANNEL) -> (data: T, received: bool) {
    if channel == ANY_CHANNEL {
        for &channel in comms.channels {
            if data, ok := comm_try_recv(&channel); ok do return data, true
        }
        return
    }
    return comm_try_recv(&comms.channels[channel])
}

