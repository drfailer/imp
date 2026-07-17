package imp

import "core:sync"
import q "core:container/queue"

// queue api ///////////////////////////////////////////////////////////////////

queue_init :: proc{
    lock_queue_init,
}

queue_destroy :: proc{
    lock_queue_destroy,
}

queue_push :: proc{
    lock_queue_push,
}

queue_pop :: proc{
    lock_queue_pop,
}

queue_size :: proc{
    lock_queue_size,
}

// lock queue //////////////////////////////////////////////////////////////////

LockQueue :: struct($T: typeid) {
    datas: q.Queue(T),
    mutex: sync.Mutex,
}

lock_queue_init :: proc(queue: ^LockQueue($T)) {
    q.init(&queue.datas)
}

lock_queue_destroy :: proc(queue: ^LockQueue($T)) {
    q.destroy(&queue.datas)
}

lock_queue_push :: proc(queue: ^LockQueue($T), data: T) -> bool {
    sync.lock(&queue.mutex)
    defer sync.unlock(&queue.mutex)
    q.enqueue(&queue.datas, data)
    return true
}

lock_queue_pop :: proc(queue: ^LockQueue($T)) -> (result: T, popped: bool){
    sync.lock(&queue.mutex)
    defer sync.unlock(&queue.mutex)
    if q.len(queue.datas) == 0 do return result, false
    return q.dequeue(&queue.datas), true
}

lock_queue_size :: proc(queue: ^LockQueue($T)) -> int {
    sync.lock(&queue.mutex)
    defer sync.unlock(&queue.mutex)
    return q.len(queue.datas)
}
