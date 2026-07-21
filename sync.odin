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

// Remote barrier //////////////////////////////////////////////////////////////

// TODO: barrier controllable from another thread

// First & Last ////////////////////////////////////////////////////////////////

// TODO: run something by the first thread or the last thread to reach a checkpoint

// Index loop //////////////////////////////////////////////////////////////////

Index_Loop :: struct {
    done: bool,
    prod_index: int,
    cons_index: int,
    cond: sync.Atomic_Cond,
    mutex: sync.Atomic_Mutex,
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

// Job /////////////////////////////////////////////////////////////////////////

Job :: struct {
    mutex: sync.Atomic_Mutex,
    cond: sync.Atomic_Cond,
    work_count: int,
}

job_init :: proc(job: ^Job, work_count := 1) {
    job.work_count = work_count
}

job_reset :: proc(job: ^Job, work_count := 1) {
    sync.guard(&job.mutex)
    job.work_count = work_count
}

job_complete_work :: proc(job: ^Job, work_count := 1) -> (done: bool) {
    sync.lock(&job.mutex)
    job.work_count = max(0, job.work_count - work_count)
    done = (job.work_count == 0)
    sync.unlock(&job.mutex)
    if done do sync.broadcast(&job.cond)
    return done
}

job_add_work :: proc(job: ^Job, work_count := 1) {
    sync.lock(&job.mutex)
    job.work_count += work_count
    sync.unlock(&job.mutex)
    if work_count == 1 {
        sync.signal(&job.cond)
    } else {
        sync.broadcast(&job.cond)
    }
}

job_is_done :: proc(job: ^Job) -> bool {
    sync.guard(&job.mutex)
    return job.work_count == 0
}

job_wait_completion :: proc(job: ^Job) {
    sync.guard(&job.mutex)
    for job.work_count > 0 {
        sync.wait(&job.cond, &job.mutex)
    }
}

job_wait_update :: proc(job: ^Job) -> bool {
    sync.guard(&job.mutex)
    work_count := job.work_count
    for job.work_count == work_count && job.work_count > 0 {
        sync.wait(&job.cond, &job.mutex)
    }
    return job.work_count == 0
}

Comm_Job :: struct($T: typeid) {
    using job: Job,
    comm: Comm(T),
}

comm_job_init :: proc(job: ^Comm_Job($T), work_count := 1, allocator := context.allocator) {
    job_init(job, work_count)
    comm_init(&job.comm)
}

comm_job_destroy :: proc(job: ^Comm_Job($T)) {
    comm_destroy(&job.comm)
}

comm_job_send :: proc(job: ^Comm_Job($T), data: T) {
    comm_send(&job.comm, Message(int){0, data})
}

comm_job_recv :: proc(job: ^Comm_Job($T)) -> T {
    msg, ok := comm_recv(&job.comm)
    ensure(ok, "communicator should not be closed for jobs")
    return msg.content
}

comm_job_try_recv :: proc(job: ^Comm_Job($T)) -> (T, bool) {
    msg, ok := comm_try_recv(&job.comm)
    return msg.content, ok
}
