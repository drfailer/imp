package imp

import "core:mem"
import vmem "core:mem/virtual"
import "core:sync"
import "base:runtime"
import p "core:container/pool"

// Pool ////////////////////////////////////////////////////////////////////////

Pool_Alloc_Mode :: enum {
    Fail,
    Wait,
    Dynamic,
}

@(private)
Pool_Node :: struct($T: typeid) {
    using _data: T,
    _next: ^Pool_Node(T),
}

Pool :: struct($T: typeid) {
    free_list: ^Pool_Node(T),
    arena: vmem.Arena,
    mutex: sync.Mutex,
    cond: sync.Cond,
    elem_init: proc(elem: ^T, data: rawptr, allocator: mem.Allocator),
    elem_init_data: rawptr,
}

pool_init :: proc(pool: ^Pool($T), capacity: uint,
                  elem_init: proc(elem: ^T, data: rawptr, allocator: mem.Allocator) = nil,
                  elem_init_data: rawptr = nil) -> (err: runtime.Allocator_Error) {
    vmem.arena_init_growing(&pool.arena) or_return
    allocator := vmem.arena_allocator(&pool.arena)

    pool.elem_init = elem_init
    pool.elem_init_data = elem_init_data

    for _ in 0..<capacity {
        node := new(Pool_Node(T), allocator)
        if elem_init != nil do elem_init(node, elem_init_data, allocator)
        node._next = pool.free_list
        pool.free_list = node
    }
    return
}

pool_destroy_simple :: proc(pool: ^Pool($T)) {
    vmem.arena_destroy(&pool.arena)
}

pool_destroy_with_item_destroy :: proc(pool: ^Pool($T), item_destroy: proc(item: ^T, data: $D), data: D) {
    for node := pool.free_list; node != nil; node = node.next {
        item_destroy(node, data)
    }
    vmem.arena_destroy(&pool.arena)
}

pool_destroy :: proc{
    pool_destroy_simple,
    pool_destroy_with_item_destroy,
}

pool_alloc :: proc(pool: ^Pool($T), mode := Pool_Alloc_Mode.Fail) -> (data: ^T, ok: bool){
    sync.guard(&pool.mutex)
    if pool.free_list != nil {
        node := pool.free_list
        pool.free_list = node._next
        node._next = node
        return node, true
    }
    switch mode {
    case .Fail:
        return nil, false
    case .Wait:
        for pool.free_list == nil {
            sync.wait(&pool.cond, &pool.mutex)
        }
        node := pool.free_list
        pool.free_list = node._next
        node._next = node
        return node, true
    case .Dynamic:
        allocator := vmem.arena_allocator(&pool.arena)
        node := new(Pool_Node(T), allocator)
        if pool.elem_init != nil do pool.elem_init(node, pool.elem_init_data, allocator)
        node._next = node
        return node, true
    }
    panic("unreachable")
}

pool_release :: proc(pool: ^Pool($T), elem: ^T) {
    node := cast(^Pool_Node(T))elem
    if node._next != node do panic("tried to release an invalid or corrupted element")
    if sync.guard(&pool.mutex) {
        node._next = pool.free_list
        pool.free_list = node
    }
    sync.signal(&pool.cond)
}
