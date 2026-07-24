package deadlock

import "core:sync"
import "core:os"
import "core:mem"
import "core:fmt"
import "base:runtime"

ENABLED :: #config(DEADLOCK_ENABLED, true)

when ENABLED {

#assert(!ODIN_NO_CRT, "Prof requires the C Runtime (CRT) to be enabled!")

MAX_STACK_SIZE :: #config(DEADLOCK_MAX_STACK_SIZE, 64)
MAX_THREAD_COUNT :: #config(DEADLOCK_MAX_THREAD_COUNT, 512)
MAX_THREAD_MEMORY_SIZE :: #config(DEADLOCK_MAX_THREAD_MEMORY_SIZE, 128)
MAX_DUMP_MEMORY_SIZE :: #config(DEADLOCK_MAX_DUMP_MEMORY_SIZE, 1024)

@(thread_local)
THREAD_INDEX: int
THREAD_COUNTER := 0

THREADS_DATA: [MAX_THREAD_COUNT]Thread_Data
PRINT_USER_DATA: proc(data: rawptr, size: int)
DUMP_MEMORY: [MAX_THREAD_MEMORY_SIZE]u8

Thread_Data :: struct {
    stack: [dynamic; MAX_STACK_SIZE]Checkpoint,
    mem: [MAX_THREAD_MEMORY_SIZE]u8,
    size: int,
}

Checkpoint :: struct {
    name: string,
    loc: runtime.Source_Code_Location,
}

@(init)
init :: proc "contextless" () {
    init_signal_handler()
}

set_print_user_data :: proc(fun: proc(data: rawptr, size: int)) {
    PRINT_USER_DATA = fun
}

@(private="file")
get_thread_data :: proc() -> ^Thread_Data {
    if THREAD_INDEX == 0 {
        THREAD_INDEX = sync.atomic_add_explicit(&THREAD_COUNTER, 1, .Release) + 1
        ensure((THREAD_INDEX - 1) < MAX_THREAD_COUNT)
    }
    return &THREADS_DATA[THREAD_INDEX - 1]
}

checkpoint_pop :: proc() {
    thread_data := get_thread_data()
    pop(&thread_data.stack)
}

@(deferred_out=checkpoint_pop)
checkpoint_default :: proc(name := "", loc := #caller_location) {
    thread_data := get_thread_data()
    append(&thread_data.stack, Checkpoint{name, loc})
}

@(deferred_out=checkpoint_pop)
checkpoint_with_data :: proc(data: rawptr, size: int, name := "", loc := #caller_location) {
    thread_data := get_thread_data()
    append(&thread_data.stack, Checkpoint{name, loc})
    mem.copy(raw_data(thread_data.mem[:size]), data, size)
    thread_data.size = size
}

checkpoint :: proc{
    checkpoint_default,
    checkpoint_with_data,
}

dump_checkpoints :: proc "contextless" () {
    arena: mem.Arena
    context = runtime.default_context()
    mem.arena_init(&arena, DUMP_MEMORY[:])
    context.allocator = mem.arena_allocator(&arena)

    dump_checkpoint := proc(thread_data: ^Thread_Data, thread_index: int) {
        fmt.printfln("--------------- thread {} ---------------", thread_index)
        if PRINT_USER_DATA != nil do PRINT_USER_DATA(raw_data(thread_data.mem[:thread_data.size]), thread_data.size)
        for checkpoint in thread_data.stack {
            name := checkpoint.name if len(checkpoint.name) > 0 else "checkpoint"
            fmt.printfln("{}: {}({}:{})", name, checkpoint.loc.procedure, checkpoint.loc.line, checkpoint.loc.column)
        }
        fmt.println()
    }

    fmt.println()
    FMT.PRINTLN("=================================== DEADLOCK ===================================")
    for &thread_data, thread_index in THREADS_DATA[:sync.atomic_load(&THREAD_COUNTER)] {
        dump_checkpoint(&thread_data, thread_index)
    }
    fmt.println("================================================================================")
}

} else {

// emtpy stubs when disabled

set_print_user_data :: proc(fun: proc(data: rawptr)) {}
checkpoint_default :: proc(name := "", loc := #caller_location) {}
checkpoint_with_data :: proc(data: ^$T, name := "", loc := #caller_location) { }

checkpoint :: proc{
    checkpoint_default,
    checkpoint_with_data,
}

}
