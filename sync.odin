package imp

import "core:sync"
import "base:intrinsics"
import "core:mem"
import "core:fmt"

// Barriers ////////////////////////////////////////////////////////////////////

SpinBarrier :: struct #align(64) {
    thread_count: int,
    _pad0: [64 - size_of(int)]u8,
    counter: int,
    _pad1: [64 - size_of(int)]u8,
    generation: int,
}

spin_barrier_init :: proc(barrier: ^SpinBarrier, thread_count: int) {
    barrier.thread_count = thread_count
    barrier.generation = 0
    barrier.counter = 0
}

spin_barrier_wait :: proc(barrier: ^SpinBarrier) {
    gen := sync.atomic_load_explicit(&barrier.generation, .Acquire)
    if sync.atomic_add_explicit(&barrier.counter, 1, .Release) == barrier.thread_count - 1 {
        sync.atomic_store_explicit(&barrier.counter, 0, .Relaxed)
        sync.atomic_add_explicit(&barrier.generation, 1, .Release)
        return
    }
    for sync.atomic_load_explicit(&barrier.generation, .Acquire) == gen {
        for _ in 0..<16 {
            intrinsics.cpu_relax()
        }
    }
}

BarrierKind :: enum {
    Sleep,
    Spin,
}

Barrier :: struct {
    sleep: sync.Barrier,
    spin: SpinBarrier,
}

// we assume that this function is called by a single thread
barrier_init :: proc(barrier: ^Barrier, thread_count: int) {
    spin_barrier_init(&barrier.spin, thread_count)
    sync.barrier_init(&barrier.sleep, thread_count)
}

barrier_wait :: proc(barrier: ^Barrier, kind := BarrierKind.Sleep) {
    switch kind {
    case .Sleep: sync.barrier_wait(&barrier.sleep)
    case .Spin: spin_barrier_wait(&barrier.spin)
    }
}

// Remote barrier //////////////////////////////////////////////////////////////

// TODO: barrier controllable from another thread

// Index loop //////////////////////////////////////////////////////////////////

Index_Loop :: struct {
    done: bool,
    prod_index: int,
    cons_index: int,
    cond: sync.Cond,
    mutex: sync.Mutex,
}

index_loop_reset :: proc(loop: ^Index_Loop) {
    sync.guard(&loop.mutex)
    loop.prod_index = 0
    loop.cons_index = 0
    loop.done = false
}

index_loop_inc :: proc(loop: ^Index_Loop, count := 1) {
    sync.atomic_add_explicit(&loop.prod_index, count, .Release)
    if count == 1 {
        sync.signal(&loop.cond)
    } else {
        sync.broadcast(&loop.cond)
    }
}

index_loop_done :: proc(loop: ^Index_Loop) {
    sync.lock(&loop.mutex)
    loop.done = true
    sync.unlock(&loop.mutex)
    sync.broadcast(&loop.cond)
}

index_loop_step :: proc(loop: ^Index_Loop, index: ^int = nil) -> bool {
    next_index := sync.atomic_add_explicit(&loop.cons_index, 1, .Release)
    if index != nil do index^ = next_index
    if next_index < sync.atomic_load_explicit(&loop.prod_index, .Acquire) do return true

    sync.guard(&loop.mutex)
    for {
        if next_index < sync.atomic_load_explicit(&loop.prod_index, .Acquire) do break
        if loop.done do return false // we exit the loop only when the max is reached
        sync.wait(&loop.cond, &loop.mutex)
    }
    return true
}
