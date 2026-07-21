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
    closed: bool,
    queue: LockQueue(Message(T)),
    cond: sync.Atomic_Cond,
    mutex: sync.Atomic_Mutex,
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

comm_close :: proc(comm: ^Comm($T)) {
    sync.lock(&comm.mutex)
    comm.closed = true
    sync.unlock(&comm.mutex)
    sync.broadcast(&comm.cond)
}

comm_closed :: proc(comm: ^Comm($T)) -> bool {
    sync.guard(&comm.mutex)
    return comm.closed
}

comm_wait_openning :: proc(comm: ^Comm($T)) {
    sync.guard(&comm.mutex)
    for comm.closed {
        sync.wait(&comm.cond, &comm.mutex)
    }
}

comm_send :: proc(comm: ^Comm($T), m: Message(T)) {
    sync.lock(&comm.mutex)
    queue_push(&comm.queue, m)
    sync.unlock(&comm.mutex)
    sync.signal(&comm.cond)
}

comm_recv :: proc(comm: ^Comm($T)) -> (m: Message(T), ok: bool) {
    sync.guard(&comm.mutex)
    for {
        if m, ok = queue_pop(&comm.queue); ok {
            return m, true
        }
        if comm.closed do return m, false
        sync.wait(&comm.cond, &comm.mutex)
    }
}

comm_try_recv :: proc(comm: ^Comm($T)) -> (m: Message(T), received: bool) {
    return queue_pop(&comm.queue)
}

// Core utilities //////////////////////////////////////////////////////////////

ANY_CHANNEL :: -1

@(private)
Comms :: struct {
    channels: [dynamic]Comm(Data),
    mutex: sync.Atomic_Mutex,
    cond: sync.Atomic_Cond,
}

@(private)
comms_init :: proc(comms: ^Comms, channel_count: int, allocator := context.allocator) {
    comms.channels = make([dynamic]Comm(Data), channel_count, allocator)
    for &channel in comms.channels {
        comm_init(&channel, allocator)
    }
}

@(private)
comms_destroy :: proc(comms: ^Comms) {
    for &channel in comms.channels {
        comm_destroy(&channel)
    }
    delete(comms.channels)
}

@(private)
comms_send :: proc(comms: ^Comms, m: Message($T), channel := 0) {
    comm_send(&comms.channels[channel], m)
    sync.signal(&comms.cond)
}

@(private)
comms_recv :: proc(comms: ^Comms, $T: typeid, channel := ANY_CHANNEL) -> (Message(T), bool) {
    if channel == ANY_CHANNEL {
        for {
            opened := false
            for &channel in comms.channels {
                if msg, ok := comm_try_recv(&channel); ok do return msg, true
                opened |= !comm_closed(&channel)
            }
            if !opened do return {}, false
            if sync.guard(&comms.mutex) {
                sync.wait(&comms.cond, &comms.mutex)
            }
        }
    }
    return comm_recv(&comms.channels[channel])
}

@(private)
comms_try_recv :: proc(comms: ^Comms, $T: typeid, channel := ANY_CHANNEL) -> (m: Message(T), received: bool) {
    if channel == ANY_CHANNEL {
        for &channel in comms.channels {
            if msg, ok := comm_try_recv(&channel); ok do return msg, true
        }
        return {}, false
    }
    return comm_try_recv(&comms.channels[channel])
}

