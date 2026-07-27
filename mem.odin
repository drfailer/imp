package imp

import "prof"
import "core:mem"
import "core:sync"
import "base:runtime"
import p "core:container/pool"

MEM_PROFILING_ENABLED :: #config(IMP_MEM_PROFILING_ENABLED, true)

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
    arena: mem.Dynamic_Arena,
    mutex: sync.Mutex,
    cond: sync.Cond,
    elem_init: proc(elem: ^T, data: rawptr),
    elem_init_data: rawptr,
}

pool_init :: proc(pool: ^Pool($T), #any_int capacity: int,
                  elem_init: proc(elem: ^T, data: rawptr) = nil,
                  elem_init_data: rawptr = nil) -> (err: runtime.Allocator_Error) {
    block_size := round_to_power_of_2(capacity * size_of(Pool_Node(T)))
    mem.dynamic_arena_init(&pool.arena, block_size = block_size)
    allocator := mem.dynamic_arena_allocator(&pool.arena)

    pool.elem_init = elem_init
    pool.elem_init_data = elem_init_data

    for _ in 0..<capacity {
        node := new(Pool_Node(T), allocator)
        if elem_init != nil do elem_init(node, elem_init_data)
        node._next = pool.free_list
        pool.free_list = node
    }
    return
}

pool_destroy_simple :: proc(pool: ^Pool($T)) {
    mem.dynamic_arena_destroy(&pool.arena)
}

pool_destroy_with_item_destroy :: proc(pool: ^Pool($T), item_destroy: proc(item: ^T, data: rawptr), data: rawptr = nil) {
    for node := pool.free_list; node != nil; node = node._next {
        item_destroy(node, data)
    }
    mem.dynamic_arena_destroy(&pool.arena)
}

pool_destroy :: proc{
    pool_destroy_simple,
    pool_destroy_with_item_destroy,
}

pool_alloc :: proc(pool: ^Pool($T), mode := Pool_Alloc_Mode.Fail) -> (data: ^T, ok: bool){
    when MEM_PROFILING_ENABLED do prof.procedure()
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
        {
            when MEM_PROFILING_ENABLED do prof.region("pool_alloc_wait")
            for pool.free_list == nil {
                sync.wait(&pool.cond, &pool.mutex)
            }
        }
        node := pool.free_list
        pool.free_list = node._next
        node._next = node
        return node, true
    case .Dynamic:
        allocator := mem.dynamic_arena_allocator(&pool.arena)
        node := new(Pool_Node(T), allocator)
        if pool.elem_init != nil do pool.elem_init(node, pool.elem_init_data)
        node._next = node
        return node, true
    }
    panic("unreachable")
}

pool_release :: proc(pool: ^Pool($T), elem: ^T) {
    when MEM_PROFILING_ENABLED do prof.procedure()
    node := cast(^Pool_Node(T))elem
    when ODIN_DEBUG {
        if node._next != node do panic("tried to release an invalid or corrupted element")
    }
    if sync.guard(&pool.mutex) {
        node._next = pool.free_list
        pool.free_list = node
    }
    sync.signal(&pool.cond)
}

@(private = "file")
round_to_power_of_2 :: proc(n: int) -> int {
    result: int = 1
    for result < n {
        result <<= 1
    }
    ensure(result > 0)
    return result
}
