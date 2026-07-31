package imp

import "prof"
import "core:mem"
import "core:sync"
import "base:runtime"
import p "core:container/pool"

MEM_PROFILING_ENABLED :: #config(IMP_MEM_PROFILING_ENABLED, false)

// Pool ////////////////////////////////////////////////////////////////////////

Pool_Alloc_Mode :: enum {
    Fail, // if pool empty, return nil and false
    Wait, // if pool empty, wait for data to be released
    Grow, // if pool empty, the pool size is increased
    Add,  // if pool empty, allocate a new element which does not bellong to the pool (freed on release)
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
    elem_fini: proc(elem: ^T, data: rawptr),
    user_data: rawptr,
    add_allocator: mem.Allocator,
}

pool_init :: proc(pool: ^Pool($T), #any_int capacity: int,
                  elem_init: proc(elem: ^T, data: rawptr) = nil,
                  elem_fini: proc(elem: ^T, data: rawptr) = nil,
                  user_data: rawptr = nil,
                  add_allocator := context.allocator) -> (err: runtime.Allocator_Error) {
    block_size := round_to_power_of_2(capacity * size_of(Pool_Node(T)))
    mem.dynamic_arena_init(&pool.arena, block_size = block_size)
    allocator := mem.dynamic_arena_allocator(&pool.arena)

    pool.elem_init = elem_init
    pool.elem_fini = elem_fini
    pool.user_data = user_data
    pool.add_allocator = add_allocator

    for _ in 0..<capacity {
        node := new(Pool_Node(T), allocator)
        if elem_init != nil do elem_init(node, user_data)
        node._next = pool.free_list
        pool.free_list = node
    }
    return
}

pool_destroy :: proc(pool: ^Pool($T)) {
    mem.dynamic_arena_destroy(&pool.arena)
    if pool.elem_fini != nil {
        for node := pool.free_list; node != nil; node = node._next {
            pool.elem_fini(node, pool.user_data)
        }
    }
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
    case .Grow:
        allocator := mem.dynamic_arena_allocator(&pool.arena)
        node := new(Pool_Node(T), allocator)
        if pool.elem_init != nil do pool.elem_init(node, pool.user_data)
        node._next = node
        return node, true
    case .Add:
        node := new(Pool_Node(T), pool.add_allocator)
        if pool.elem_init != nil do pool.elem_init(node, pool.user_data)
        node._next = cast(^Pool_Node(T))~uintptr(node)
        return node, true
    }
    panic("unreachable")
}

pool_release :: proc(pool: ^Pool($T), elem: ^T, loc := #caller_location) {
    when MEM_PROFILING_ENABLED do prof.procedure()
    node := cast(^Pool_Node(T))elem
    nextptr := uintptr(node._next)
    nodeptr := uintptr(node)
    if nextptr == nodeptr {
        if sync.guard(&pool.mutex) {
            node._next = pool.free_list
            pool.free_list = node
        }
        sync.signal(&pool.cond)
    } else if nextptr == ~nodeptr {
        if sync.guard(&pool.mutex) {
            pool.elem_fini(node, pool.user_data)
            free(node, pool.add_allocator)
        }
    } else {
        panic("tried to release an invalid or corrupted element", loc = loc)
    }
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
