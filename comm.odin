package imp

import "prof"
import "core:sync"
import "core:mem"
import "base:intrinsics"
import "base:runtime"
import q "core:container/queue"

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
    channels: [dynamic]Lock_Queue(T),
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
    sync.guard(&comm.mutex)
    comm.closed = closed
    sync.broadcast(&comm.cond)
}

comm_is_closed :: proc(comm: ^Comm($T)) -> bool {
    sync.guard(&comm.mutex)
    return comm.closed
}

comm_wait_open :: proc(comm: ^Comm($T)) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    sync.guard(&comm.mutex)
    for comm.closed {
        sync.wait(&comm.cond, &comm.mutex)
    }
}

comm_send :: proc(comm: ^Comm($T), data: T, channel := 0) {
    when COMM_PROFILING_ENABLED do prof.procedure()
    queue_push(&comm.channels[channel], data)
    sync.guard(&comm.mutex)
    sync.signal(&comm.cond)
}

type_comm_send :: proc(comm: ^Comm($U), data: $T) {
    when intrinsics.type_is_union(U) {
        comm_send(comm, data, intrinsics.type_variant_index_of(U, T))
    } else {
        comm_send(comm, data, 0)
    }
}

comm_recv :: proc(comm: ^Comm($T), channel := ANY_CHANNEL) -> (data: T, received: bool) {
    // greedy try recv before locking the global mutex
    data, received = comm_try_recv(comm, channel)
    if received do return data, true

    // wait loop
    sync.guard(&comm.mutex)
    for {
        data, received = comm_try_recv(comm, channel)
        if received do return data, true
        sync.wait(&comm.cond, &comm.mutex);
    }

    panic("unreachable")
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

comm_try_recv :: proc(comm: ^Comm($T), channel := ANY_CHANNEL) -> (data: T, received: bool) {
    when COMM_PROFILING_ENABLED do prof.procedure()

    if channel != ANY_CHANNEL {
        return queue_pop(&comm.channels[channel])
    }

    for &channel in comm.channels {
        data, received = queue_pop(&channel)
        if received do return data, true
    }

    return data, false
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
