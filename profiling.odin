package imp

import "core:fmt"
import "core:time"
import "core:os"

PROFILER_ENABLED :: #config(IMP_PROFILER_ENABLED, false)
PROF_MAX_STACK_SIZE :: #config(IMP_PROF_MAX_STACK_SIZE, 64)

when PROFILER_ENABLED {

Profilers :: struct {
    profilers: []Profiler,
    stopwatch: time.Stopwatch,
}

Profiler :: struct {
    entries: map[string]Profile_Entry,
    stack: [dynamic; PROF_MAX_STACK_SIZE]string,
    stack_overflow_counter: uint,
    stopwatch: time.Stopwatch,
}

profiler_init :: proc(profiler: ^Profiler, allocator := context.allocator) {
    profiler.entries = make(map[string]Profile_Entry, PROF_MAX_STACK_SIZE, allocator)
    append(&profiler.stack, "root")
    time.stopwatch_start(&profiler.stopwatch)
}

profiler_destroy :: proc(profiler: ^Profiler) {
    delete(profiler.entries)
}

} else {

Profilers :: struct {}
Profiler :: struct {}

profiler_init :: proc(profiler: ^Profiler, allocator := context.allocator) {}
profiler_destroy :: proc(profiler: ^Profiler) {}

}

Profile_Entry :: struct {
    parents: map[string]Parent_Profile_Info,
    stopwatch: time.Stopwatch,
    min: time.Duration,
    max: time.Duration,
    ttl: time.Duration,
    count: int,
}

Parent_Profile_Info :: struct {
    call_count: uint,
}

// profiling API ///////////////////////////////////////////////////////////////

when PROFILER_ENABLED {

prof_region_begin_profiler :: proc(profiler: ^Profiler, name: string) {
    // get or insert the entry
    entry, found := &profiler.entries[name]
    if !found {
        profiler.entries[name] = Profile_Entry{}
        entry = &profiler.entries[name]
    }

    // update the parent info
    parent_name := profiler.stack[len(profiler.stack) - 1]
    parent_info, parent_found := &entry.parents[parent_name]
    if !parent_found {
        entry.parents[parent_name] = Parent_Profile_Info{}
        parent_info = &entry.parents[parent_name]
    }
    parent_info.call_count += 1

    // update the call stack if possible
    if len(profiler.stack) + 1 < cap(profiler.stack) {
        append(&profiler.stack, name)
    } else {
        profiler.stack_overflow_counter += 1
    }

    // start the region timer
    time.stopwatch_reset(&entry.stopwatch)
    time.stopwatch_start(&entry.stopwatch)
}

prof_region_begin_ctx :: proc(ctx: Ctx, name: string) {
    prof_region_begin_profiler(ctx.thread_ctx.profiler, name)
}

prof_region_end_profiler :: proc(profiler: ^Profiler, name: string) {
    entry, found := &profiler.entries[name]
    assert(found)

    // stop the region timer and get the duration
    time.stopwatch_stop(&entry.stopwatch)
    duration := time.stopwatch_duration(entry.stopwatch)

    // profile info update
    entry.min = min(entry.min, duration) if entry.min > 0 else duration
    entry.max = max(entry.max, duration)
    entry.ttl += duration
    entry.count += 1

    // call stack update
    if profiler.stack_overflow_counter > 0 do profiler.stack_overflow_counter -= 1
    if profiler.stack_overflow_counter == 0 do pop(&profiler.stack)
}

prof_region_end_ctx :: proc(ctx: Ctx, name: string) {
    prof_region_end_profiler(ctx.thread_ctx.profiler, name)
}


@(deferred_in=prof_region_end_profiler)
prof_region_profiler :: proc(profiler: ^Profiler, name: string) -> bool {
    prof_region_begin_profiler(profiler, name)
    return true
}

@(deferred_in=prof_region_end_ctx)
prof_region_ctx :: proc(ctx: Ctx, name: string) -> bool {
    prof_region_begin_ctx(ctx, name)
    return true
}

@(private)
prof_procedure_end_profiler :: proc(profiler: ^Profiler, loc := #caller_location) {
    prof_region_end_profiler(profiler, loc.procedure)
}

@(deferred_in=prof_procedure_end_profiler)
prof_procedure_profiler :: proc(profiler: ^Profiler, loc := #caller_location) {
    prof_region_begin_profiler(profiler, loc.procedure)
}

@(private)
prof_procedure_end_ctx :: proc(ctx: Ctx, loc := #caller_location) {
    prof_region_end_ctx(ctx, loc.procedure)
}

@(deferred_in=prof_procedure_end_ctx)
prof_procedure_ctx :: proc(ctx: Ctx, loc := #caller_location) {
    prof_region_begin_ctx(ctx, loc.procedure)
}

} else {

prof_region_begin_profiler :: proc(profiler: ^Profiler, name: string) {}
prof_region_end_profiler :: proc(profiler: ^Profiler, name: string) {}
prof_region_profiler :: proc(profiler: ^Profiler, name: string) -> bool { return true }
prof_procedure_profiler :: proc(profiler: ^Profiler, loc := #caller_location) {}

prof_region_begin_ctx :: proc(ctx: Ctx, name: string) {}
prof_region_end_ctx :: proc(ctx: Ctx, name: string) {}
prof_region_ctx :: proc(ctx: Ctx, name: string) -> bool { return true }
prof_procedure_ctx :: proc(ctx: Ctx, loc := #caller_location) {}

}

prof_region_end :: proc{ prof_region_end_profiler, prof_region_end_ctx }
prof_region_begin :: proc{ prof_region_begin_profiler, prof_region_begin_ctx }
prof_region :: proc{ prof_region_profiler, prof_region_ctx }
prof_procedure :: proc{ prof_procedure_profiler, prof_procedure_ctx }

// report //////////////////////////////////////////////////////////////////////

when PROFILER_ENABLED {

@(private="file")
Global_Profile_Entry :: struct {
    using entry: Profile_Entry,
    thread_count: uint,
}

@(private="file")
map_get_ptr :: proc(m: ^map[$K]$V, key: K) -> ^V {
    value_ptr, found := &m[key]
    if !found {
        m[key] = {}
        value_ptr = &m[key]
    }
    return value_ptr
}

@(private="file")
compile_global_entries :: proc(profilers: Profilers) -> map[string]Global_Profile_Entry {
    global_entries := make(map[string]Global_Profile_Entry)
    // compute the global entries (merge informations from all the threads)
    for profiler in profilers.profilers {
        for entry_name, entry in profiler.entries {
            global_entry := map_get_ptr(&global_entries, entry_name)
            for parent_name, parent_info in entry.parents {
                global_parent_info := map_get_ptr(&global_entry.parents, parent_name)
                global_parent_info.call_count += parent_info.call_count
            }
            global_entry.min = min(entry.min, global_entry.min) if global_entry.min > 0 else entry.min
            global_entry.max = max(entry.max, global_entry.max)
            global_entry.ttl += entry.ttl
            global_entry.count += entry.count
            global_entry.thread_count += 1
        }
    }
    return global_entries
}

@(private="file")
destroy_global_entries :: proc(global_entries: ^map[string]Global_Profile_Entry) {
    for _, &entry in global_entries^ {
        delete(entry.parents)
    }
    delete(global_entries^)
}

prof_print_report_dot :: proc(ctx: Global_Ctx, filename: string) {
    file, err := os.open(filename, {.Write, .Create, .Trunc}, {.Read_Other, .Write_Group, .Read_Other, .Write_User, .Read_User})
    ensure(err == nil, "failed to open file")
    global_dur := time.stopwatch_duration(ctx.profilers.stopwatch)
    global_entries := compile_global_entries(ctx.profilers)
    defer destroy_global_entries(&global_entries)

    fmt.fprintln(file, "digraph Program_Execution {")

    // set the src entry
    fmt.fprintfln(file, "root [label=\"{}\",shape=rectangle];", os.args[0])

    for entry_name, entry in global_entries {
        avg_dur := time.Duration(f64(entry.ttl) / f64(entry.count))
        ttl_dur := cast(time.Duration)(f64(entry.ttl) / f64(entry.thread_count))
        ratio   := f64(ttl_dur) / f64(global_dur)
        percent := 100 * ratio
        // determin the node colors
        red := u8(255 * ratio)
        green := u8(1 - 2 * abs(ratio - 0.5))
        blue := u8(255 * (1 - ratio))

        fmt.fprintfln(file, "{} [label=\"{}\\ncount = {}\\navg = {}, min = {}, max = {}\\nttl = {} ({:.3f}%%)\\nthreads = {} ({})\",shape=rectangle,color=\"#%2X%2X%2X\",penwidth=2];",
            entry_name, entry_name, entry.count, avg_dur, entry.min, entry.max, ttl_dur, percent, entry.thread_count, entry.ttl, red, green, blue)

        for parent_name, parent_info in entry.parents {
            parent_entry, parent_found := &global_entries[parent_name]
            parent_ttl := parent_entry.ttl if parent_found else global_dur
            percent := 100 * (f64(entry.ttl) / f64(parent_ttl))
            fmt.fprintfln(file, "{} -> {} [label=\"x {} / {:.3f}%%\"];",
                parent_name, entry_name, parent_info.call_count, percent)
        }
    }
    fmt.fprintfln(file, "}")
}

prof_print_report :: proc(profiler: Profiler) {
    global_dur := time.stopwatch_duration(profiler.stopwatch)

    for entry_name, entry in profiler.entries {
        avg_dur := time.Duration(f64(entry.ttl) / f64(entry.count))
        ratio   := f64(entry.ttl) / f64(global_dur)
        percent := 100 * ratio

        fmt.printfln("- {}: count = {}, avg = {}, min = {}, max = {}, ttl = {} ({:.3f}%%)",
            entry_name, entry.count, avg_dur, entry.min, entry.max, entry.ttl, percent)

        for parent_name, parent_info in entry.parents {
            parent_entry, parent_found := &profiler.entries[parent_name]
            parent_ttl := parent_entry.ttl if parent_found else global_dur
            percent := 100 * (f64(entry.ttl) / f64(parent_ttl))
            fmt.printfln("  - {} -> {}: x {} / {:.3f}%%",
                parent_name, entry_name, parent_info.call_count, percent)
        }
    }
}

} else {

// Stubs for when the profiler is disabled
prof_print_report_dot :: proc(ctx: Global_Ctx, filename: string) {}
prof_print_report :: proc(profiler: Profiler) {}

}
