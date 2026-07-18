package imp

import "core:sync"
import "base:intrinsics"
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
    queue: LockQueue(Data),
}

job_create :: proc(step_count := 1) -> ^Job {
    job := new(Job)
    queue_init(&job.queue)
    job.steps = step_count
    return job
}

job_destroy :: proc(job: ^Job) {
    queue_destroy(&job.queue)
    free(job)
}

job_reset :: proc(job: ^Job, step_count := 1) {
    sync.lock(&job.mutex)
    defer sync.unlock(&job.mutex)
    job.steps = step_count
    // note: the queue is not cleared, may change later
}

job_done :: proc(job: ^Job, mdata: Maybe(Data) = nil) {
    sync.lock(&job.mutex)
    job.steps -= 1
    sync.unlock(&job.mutex)

    should_signal := job.steps <= 1
    if data, ok := mdata.?; ok {
        queue_push(&job.queue, data)
        should_signal = true
    }
    if should_signal {
        sync.cond_broadcast(&job.cond)
    }
}

job_result :: proc(job: ^Job, data: Data) {
    queue_push(&job.queue, data)
    sync.cond_broadcast(&job.cond)
}

job_is_done :: proc(job: ^Job) -> bool {
    sync.lock(&job.mutex)
    defer sync.unlock(&job.mutex)
    return job.steps == 0
}

job_has_data :: proc(job: ^Job) -> bool {
    return queue_size(&job.queue) > 0
}

job_pop_data :: proc(job: ^Job) -> (Data, bool) {
    return queue_pop(&job.queue)
}

job_wait :: proc(job: ^Job) {
    sync.lock(&job.mutex)
    defer sync.unlock(&job.mutex)
    for job.steps > 0 {
        sync.cond_wait(&job.cond, &job.mutex)
    }
}

job_wait_data :: proc(job: ^Job) -> (data: Data, has_data: bool) {
    sync.lock(&job.mutex)
    defer sync.unlock(&job.mutex)
    for {
        if queue_size(&job.queue) > 0 do return queue_pop(&job.queue)
        if job.steps <= 0 do break
        sync.cond_wait(&job.cond, &job.mutex)
    }
    return data, false
}
