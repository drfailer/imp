package imp

import "core:sync"
import "base:intrinsics"
import "core:mem"
import "core:fmt"

// Barriers ////////////////////////////////////////////////////////////////////

SpinBarrier :: struct #align(64) {
    counter: int,
    generation: int,
    thread_count: int,
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
        intrinsics.cpu_relax()
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

// Job /////////////////////////////////////////////////////////////////////////

Job :: struct {
    mutex: sync.Mutex,
    cond: sync.Cond,
    steps: int,
    comm: Comm(Data),
    allocator: mem.Allocator
}

job_create :: proc(step_count := 1, allocator := context.allocator) -> ^Job {
    job := new(Job, allocator)
    comm_init(&job.comm)
    job.steps = step_count
    job.allocator = allocator
    return job
}

job_destroy :: proc(job: ^Job) {
    comm_destroy(&job.comm)
    free(job, job.allocator)
}

job_reset :: proc(job: ^Job, step_count := 1) {
    sync.guard(&job.mutex)
    job.steps = step_count
}

job_complete_step :: proc(job: ^Job) -> (done: bool) {
    sync.lock(&job.mutex)
    job.steps -= 1
    done = (job.steps == 0)
    sync.unlock(&job.mutex)
    if done do sync.broadcast(&job.cond)
    return done
}

job_add_data :: proc(job: ^Job, data: Data) {
    comm_send(&job.comm, Message(Data){0, data})
}

job_get_data :: proc(job: ^Job) -> Data {
    return comm_recv(&job.comm).content
}

job_try_get_data :: proc(job: ^Job) -> (Data, bool) {
    msg, ok := comm_try_recv(&job.comm)
    if ok do return msg.content, true
    return Data{}, false
}

job_is_done :: proc(job: ^Job) -> bool {
    sync.guard(&job.mutex)
    return job.steps == 0
}

job_wait_completion :: proc(job: ^Job) {
    sync.guard(&job.mutex)
    for job.steps > 0 {
        sync.cond_wait(&job.cond, &job.mutex)
    }
}
