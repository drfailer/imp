package imp

import "core:sync"
import "base:intrinsics"
import q "core:container/queue"

CACHE_LINE :: 64

// queue api ///////////////////////////////////////////////////////////////////

queue_init :: proc{
    lock_queue_init,
    ms_queue_init,
    boudned_mpmc_queue_init,
}

queue_destroy :: proc{
    lock_queue_destroy,
    ms_queue_destroy,
    bounded_mpmc_queue_destroy,
}

queue_push :: proc{
    lock_queue_push,
    ms_queue_push,
    bounded_mpmc_queue_push,
}

queue_pop :: proc{
    lock_queue_pop,
    ms_queue_pop,
    bounded_mpmc_queue_pop,
}

queue_size :: proc{
    lock_queue_size,
}

// lock queue //////////////////////////////////////////////////////////////////

Lock_Queue :: struct($T: typeid) #align(CACHE_LINE) {
    mutex: sync.Mutex,
    datas: q.Queue(T),
}

lock_queue_init :: proc(queue: ^Lock_Queue($T), allocator := context.allocator) {
    q.init(&queue.datas, allocator = allocator)
}

lock_queue_destroy :: proc(queue: ^Lock_Queue($T)) {
    q.destroy(&queue.datas)
}

lock_queue_push :: proc(queue: ^Lock_Queue($T), data: T) -> bool {
    sync.guard(&queue.mutex)
    ok, err := q.enqueue(&queue.datas, data)
    ensure(ok && err == nil, "failed to grow the queue")
    return true
}

lock_queue_pop :: proc(queue: ^Lock_Queue($T)) -> (result: T, popped: bool){
    sync.guard(&queue.mutex)
    if q.len(queue.datas) == 0 do return result, false
    return q.dequeue(&queue.datas), true
}

lock_queue_size :: proc(queue: ^Lock_Queue($T)) -> int {
    sync.guard(&queue.mutex)
    return q.len(queue.datas)
}

// lock free queue /////////////////////////////////////////////////////////////

/*
 * This is the implementation of the lock free queue described by Maged M.
 * Michael and Michael L. Scott in "Simple, Fast, and Practical Non-Blocking
 * and Blocking Concurrent Queue Algorithms".
 * paper link: https://www.cs.rochester.edu/u/scott/papers/1996_PODC_queues.pdf
 */

// this queue needs atomic16 ops which is only truely lock free on ARM

atomic_load16 :: proc(val: ^$T, $mode: sync.Atomic_Memory_Order) -> T {
    return transmute(T)sync.atomic_load_explicit(transmute(^u128)val, mode)
}

atomic_store16 :: proc(dst: ^$T, val: T, $mode: sync.Atomic_Memory_Order) {
    sync.atomic_store_explicit(transmute(^u128)dst, transmute(u128)val, mode)
}

atomic_compare_exchange_weak16 :: proc(dst: ^$T, old, new: T, $success: sync.Atomic_Memory_Order) -> (T, bool) {
    result, ok := sync.atomic_compare_exchange_weak_explicit(transmute(^u128)dst, transmute(u128)old, transmute(u128)new, success, .Relaxed)
    return transmute(T)result, ok
}

MS_Queue_Node_Ptr :: struct($T: typeid) #align(16) {
    ptr: ^MS_Queue_Node(T),
    count: uint,
}

MS_Queue_Node :: struct($T: typeid) #align(CACHE_LINE) {
    data: T,
    next: MS_Queue_Node_Ptr(T),
}

MS_Queue :: struct($T: typeid) {
    head: MS_Queue_Node_Ptr(T),
    tail: MS_Queue_Node_Ptr(T),
    free: MS_Queue_Node_Ptr(T),
}

ms_queue_init :: proc(queue: ^MS_Queue($T)) {
    node := ms_queue_allocate_node(queue)
    queue.head = {node, 0}
    queue.tail = {node, 0}
}

// NOTE(atomic): this function is supposed to be executed by a single thread,
//               therefore we don't use atomics here.
ms_queue_destroy :: proc(queue: ^MS_Queue($T)) {
    // used nodes
    node := queue.head
    for node.ptr != nil {
        next := node.ptr.next
        free(node.ptr)
        node = next
    }
    // free nodes
    fnode := queue.free
    for fnode.ptr != nil {
        next := fnode.ptr.next
        free(fnode.ptr)
        fnode = next
    }
}

ms_queue_push :: proc(queue: ^MS_Queue($T), value: T) {
    tail, next: MS_Queue_Node_Ptr(T)
    node := ms_queue_allocate_node(queue)

    node.data = value
    atomic_store16(&node.next, MS_Queue_Node_Ptr(T){nil, 0}, .Relaxed)
    for {
        tail = atomic_load16(&queue.tail, .Acquire)
        next = atomic_load16(&tail.ptr.next, .Acquire)

        if tail == atomic_load16(&queue.tail, .Acquire) {
            if next.ptr == nil {
                new_next := MS_Queue_Node_Ptr(T){node, next.count + 1}
                if _, ok := atomic_compare_exchange_weak16(&tail.ptr.next, next, new_next, .Release); ok {
                    break
                } else {
                    intrinsics.cpu_relax()
                }
            } else {
                new_tail := MS_Queue_Node_Ptr(T){next.ptr, tail.count + 1}
                atomic_compare_exchange_weak16(&queue.tail, tail, new_tail, .Release)
                intrinsics.cpu_relax()
            }
        }
    }
    new_tail := MS_Queue_Node_Ptr(T){node, tail.count + 1}
    atomic_compare_exchange_weak16(&queue.tail, tail, new_tail, .Release)
}

ms_queue_pop :: proc(queue: ^MS_Queue($T)) -> (result: T, popped: bool) {
    head, tail, next: MS_Queue_Node_Ptr(T)

    for {
        head = atomic_load16(&queue.head, .Acquire)
        tail = atomic_load16(&queue.tail, .Acquire)
        next = atomic_load16(&head.ptr.next, .Acquire)

        if head == atomic_load16(&queue.head, .Acquire) {
            if head == tail {
                if next.ptr == nil {
                    return result, false
                }
                new_tail := MS_Queue_Node_Ptr(T){next.ptr, tail.count + 1}
                atomic_compare_exchange_weak16(&queue.tail, tail, new_tail, .Release)
                intrinsics.cpu_relax()
            } else {
                result = next.ptr.data
                new_head := MS_Queue_Node_Ptr(T){next.ptr, head.count + 1}
                if _, ok := atomic_compare_exchange_weak16(&queue.head, head, new_head, .Release); ok {
                    break
                } else {
                    intrinsics.cpu_relax()
                }
            }
        }
    }
    ms_queue_release_node(queue, head.ptr)
    return result, true
}

ms_queue_allocate_node :: proc(queue: ^MS_Queue($T)) -> ^MS_Queue_Node(T) {
    free: MS_Queue_Node_Ptr(T)

    for {
        free = atomic_load16(&queue.free, .Acquire)
        if free.ptr == nil {
            return new(MS_Queue_Node(T))
        }
        next := atomic_load16(&free.ptr.next, .Acquire)
        new_free := MS_Queue_Node_Ptr(T){next.ptr, next.count + 1}
        if val, ok := atomic_compare_exchange_weak16(&queue.free, free, new_free, .Release); ok {
            return val.ptr
        }
    }
}


ms_queue_release_node :: proc(queue: ^MS_Queue($T), node: ^MS_Queue_Node(T)) {
    assert(node != nil)
    free: MS_Queue_Node_Ptr(T)

    // TODO: add a counter and a max pool size so we don't keep increasing the
    //       pool size infinitely

    for {
        free = atomic_load16(&queue.free, .Acquire)
        atomic_store16(&node.next, MS_Queue_Node_Ptr(T){free.ptr, free.count + 1}, .Relaxed)
        new_free := MS_Queue_Node_Ptr(T){node, free.count + 1}
        if _, ok := atomic_compare_exchange_weak16(&queue.free, free, new_free, .Release); ok {
            break
        }
    }
}

// bounded mpmc queue //////////////////////////////////////////////////////////

/*
 * Implementation of Dmitry Vyukov MPMC queue.
 */

Bounded_MPMC_Queue :: struct($T: typeid, $SIZE: uint) #align(CACHE_LINE) {
    datas: [SIZE]T,
    indices: [SIZE]uint,
    head: uint,
    tail: uint,
}

boudned_mpmc_queue_init :: proc(queue: ^Bounded_MPMC_Queue($T, $SIZE)) {
    for i: uint = 0; i < SIZE; i += 1 {
        queue.indices[i] = i
    }
}

bounded_mpmc_queue_destroy :: proc(queue: ^Bounded_MPMC_Queue($T, $SIZE)) {}

bounded_mpmc_queue_push :: proc(queue: ^Bounded_MPMC_Queue($T, $SIZE), value: T) -> bool {
    t := sync.atomic_load_explicit(&queue.tail, .Relaxed)
    mask := SIZE - 1
    ok: bool

    for {
        seq := sync.atomic_load_explicit(&queue.indices[t & mask], .Acquire)
        diff := int(seq) - int(t)
        if diff == 0 {
            if t, ok = sync.atomic_compare_exchange_weak_explicit(&queue.tail, t, t + 1, .Relaxed); ok {
                break
            }
        } else if diff < 0 {
            return false
        } else {
            intrinsics.cpu_relax()
            t = sync.atomic_load_explicit(&queue.tail, .Relaxed)
        }
    }
    queue.datas[t & mask] = value
    sync.atomic_store_explicit(&queue.indices[t & mask], t + 1, .Release)
    return true
}

bounded_mpmc_queue_pop :: proc(queue: ^Bounded_MPMC_Queue($T, $SIZE)) -> (result: T, popped: bool) {
    h := sync.atomic_load_explicit(&queue.head, .Relaxed)
    mask := SIZE - 1
    ok: bool

    for {
        seq := sync.atomic_load_explicit(&queue.indices[h & mask], .Acquire)
        diff := int(seq) - int(h + 1)

        if diff == 0 {
            if h, ok = sync.atomic_compare_exchange_weak_explicit(&queue.head, h, h + 1, .Relaxed); ok {
                break
            }
        } else if diff < 0 {
            return result, false
        } else {
            intrinsics.cpu_relax()
            h = sync.atomic_load_explicit(&queue.head, .Relaxed)
        }
    }
    result = queue.datas[h & mask]
    sync.atomic_store_explicit(&queue.indices[h & mask], h + SIZE, .Release)
    return result, true
}
