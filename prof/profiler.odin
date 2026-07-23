package profiler

import "core:fmt"
import "core:time"
import "core:sync"
import "core:os"
import "core:container/small_array"

ENABLED :: #config(PROF_ENABLED, false)

when ENABLED {

#assert(!ODIN_NO_CRT, "Prof requires the C Runtime (CRT) to be enabled!")

MAX_STACK_SIZE :: #config(PROF_MAX_STACK_SIZE, 64)
MAX_THREAD_COUNT :: #config(PROF_MAX_THREAD_COUNT, 512)
INIT_ENTRIES_CAPACITY :: #config(PROF_INIT_ENTRIES_CAPACITY, 32)

// global stopwatch used to compute the global execution time
GLOBAL_STOPWATCH: time.Stopwatch

// on first call to a profiler function, threads will atomically increment the
// thread counter to generate a profiler index
@(thread_local)
PROFILER_INDEX: int
THREAD_COUNTER := 0

PROFILERS: [MAX_THREAD_COUNT]Profiler

Profiler :: struct {
    entries: map[string]Profile_Entry,
    stack: [dynamic; MAX_STACK_SIZE]string,
    stack_overflow_counter: uint,
    stopwatch: time.Stopwatch,
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

}

// init ////////////////////////////////////////////////////////////////////////

when ENABLED {

init :: proc() {
    time.stopwatch_start(&GLOBAL_STOPWATCH)
}

fini :: proc() {
    clear()
}

clear :: proc() {
    for &profiler in PROFILERS[:MAX_THREAD_COUNT] {
        for _, &entry in profiler.entries {
            delete(entry.parents)
        }
        delete(profiler.entries)
    }
    sync.atomic_store(&THREAD_COUNTER, 0)
}

reset :: proc() {
    time.stopwatch_reset(&GLOBAL_STOPWATCH)
    time.stopwatch_start(&GLOBAL_STOPWATCH)
}

@(private="file")
get_profiler :: proc() -> ^Profiler {
    if PROFILER_INDEX == 0 {
        PROFILER_INDEX = sync.atomic_add_explicit(&THREAD_COUNTER, 1, .Release) + 1
        ensure((PROFILER_INDEX - 1) < MAX_THREAD_COUNT)
        profiler := &PROFILERS[PROFILER_INDEX - 1]
        profiler.entries = make(map[string]Profile_Entry, INIT_ENTRIES_CAPACITY)
        append(&profiler.stack, "main")
    }
    return &PROFILERS[PROFILER_INDEX - 1]
}

} else {

init :: proc() {}
fini :: proc() {}
clear :: proc() {}
reset :: proc() {}

}

// region //////////////////////////////////////////////////////////////////////

//
// profile a specific region of the code
//

when ENABLED {

region_begin :: proc(name: string) {
    profiler := get_profiler()

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
    if append(&profiler.stack, name) == 0 do profiler.stack_overflow_counter += 1

    // start the region timer
    time.stopwatch_reset(&entry.stopwatch)
    time.stopwatch_start(&entry.stopwatch)
}

region_end :: proc(name: string) {
    profiler := get_profiler()
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

@(deferred_in=region_end)
region :: proc(name: string) -> bool {
    region_begin(name)
    return true
}

procedure_end :: proc(loc := #caller_location) {
    region_end(loc.procedure)
}

@(deferred_in=procedure_end)
procedure :: proc(loc := #caller_location) {
    region_begin(loc.procedure)
}

} else {

region_begin :: proc(name: string) {}
region_end :: proc(name: string) {}
region :: proc(name: string) -> bool { return true }

procedure_end :: proc(loc := #caller_location) {}
procedure :: proc(loc := #caller_location) {}

}

// report //////////////////////////////////////////////////////////////////////

ReportFormat :: enum {
    Dot,
    // html table?
    // json?
}

when ENABLED {

report :: proc(target_entries: []string = {}) {
    infos := gather_profile_infos()
    defer destroy_gathered_profile_infos(infos)

    print_entry :: proc(name: string, infos: Gathered_Profile_Infos) {
        entry := &infos.entries[name]
        avg := time.Duration(f64(entry.ttl) / f64(entry.count))
        ttl_avg := time.Duration(f64(entry.ttl) / f64(entry.thread_count))
        ratio   := f64(ttl_avg) / f64(infos.global_time)
        percent := 100 * ratio

        fmt.println("ENTRY:", name)
        fmt.printfln("THREAD TIME: {} ({})", entry.ttl, entry.thread_count)
        fmt.printfln("TOTAL TIME: {} ({:.3f}%%)", ttl_avg, percent)
        fmt.printfln("ELEMENT TIME: avg = {}, min = {}, max = {} ({})", avg, entry.min, entry.max, entry.count)

        fmt.println("PARENTS:")
        for parent_name, parent_info in entry.parents {
            parent_entry, parent_found := &infos.entries[parent_name]
            parent_ttl := parent_entry.ttl if parent_found else infos.global_time
            percent := 100 * (f64(entry.ttl) / f64(parent_ttl))
            fmt.printfln("  {}: x{} / {:.3f}%%", parent_name, parent_info.call_count, percent)
        }
        fmt.println()
    }

    fmt.println("===================================== prof =====================================")
    fmt.println()
    if len(target_entries) == 0 {
        for entry in infos.entries {
            print_entry(entry, infos)
        }
    } else {
        for entry in target_entries {
            print_entry(entry, infos)
        }
    }
    fmt.println("================================================================================")
}

print_report_to_file :: proc(filename: string, format := ReportFormat.Dot) {
    file, err := os.open(filename, {.Write, .Create, .Trunc}, {.Read_Other, .Write_Group, .Read_Other, .Write_User, .Read_User})
    ensure(err == nil, "failed to open file")
    switch format {
    case .Dot: generate_dot_file(file)
    }
}

@(private="file")
time_to_rgb :: proc(dur, ttl: time.Duration) -> (r, g, b: u8) {
    dur := f64(dur)
    ttl := f64(ttl)
    fr, fg, fb: f64
    fr = 1
    fg = 1
    fb = 1

    if dur < 0.25 * ttl {
        fr = 0
        fg = 4 * f64(dur) / f64(ttl)
    } else if dur < 0.5 * ttl {
        fr = 0
        fb = 1 + 4 * (0.25 * ttl - dur) / ttl
    } else if dur < 0.75 * ttl {
        fr = 4 * (dur - 0.5 * ttl) / ttl
        fb = 0
    } else {
        fg = 1 + 4 * (0.75 * ttl - dur) / ttl
        fb = 0
    }
    r = cast(u8)clamp(fr * 255, 0, 255)
    g = cast(u8)clamp(fg * 255, 0, 255)
    b = cast(u8)clamp(fb * 255, 0, 255)
    return r, g, b
}

@(private="file")
generate_dot_file :: proc(file: ^os.File) {
    infos := gather_profile_infos()
    defer destroy_gathered_profile_infos(infos)

    fmt.fprintln(file, "digraph Program_Execution {")
    fmt.fprintfln(file, "label=\"execution time = {}\";", infos.global_time)

    // set the main entry
    fmt.fprintfln(file, "main [label=\"{} ({})\",shape=rectangle];", os.args[0], infos.global_time)

    for entry_name, entry in infos.entries {
        avg := time.Duration(f64(entry.ttl) / f64(entry.count))
        ttl_avg := time.Duration(f64(entry.ttl) / f64(entry.thread_count))
        percent := 100 * f64(ttl_avg) / f64(infos.global_time)
        r, g, b := time_to_rgb(ttl_avg, infos.global_time)

        fmt.fprintfln(file, "\"{}\" [label=\"{}\\ncount = {}\\navg = {}, min = {}, max = {}\\nttl = {} ({:.3f}%%)\\nthreads = {} ({})\",shape=rectangle,color=\"#%2X%2X%2X\",penwidth=2];",
            entry_name, entry_name, entry.count, avg, entry.min, entry.max, ttl_avg,
            percent, entry.thread_count, entry.ttl, r, g, b)

        for parent_name, parent_info in entry.parents {
            parent_entry, parent_found := &infos.entries[parent_name]
            parent_ttl := parent_entry.ttl if parent_found else infos.global_time
            percent := 100 * (f64(entry.ttl) / f64(parent_ttl))
            fmt.fprintfln(file, "{} -> {} [label=\"x {} / {:.3f}%%\"];",
                parent_name, entry_name, parent_info.call_count, percent)
        }
    }
    fmt.fprintfln(file, "}")
}

} else {

report :: proc(target_entries: []string = {}) {}
print_report_to_file :: proc(filename: string, format := ReportFormat.Dot) {}

}

// internals ///////////////////////////////////////////////////////////////////

when ENABLED {

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
Gathered_Profile_Entry :: struct {
    using entry: Profile_Entry,
    thread_count: uint,
}

@(private="file")
Gathered_Profile_Infos :: struct {
    entries: map[string]Gathered_Profile_Entry,
    global_time: time.Duration,
}

@(private="file")
gather_profile_infos :: proc(allocator := context.allocator) -> (infos: Gathered_Profile_Infos) {
    infos.global_time = time.stopwatch_duration(GLOBAL_STOPWATCH)

    // compute the global entries (merge informations from all the threads)
    for &profiler in PROFILERS {
        for entry_name, entry in profiler.entries {
            global_entry := map_get_ptr(&infos.entries, entry_name)

            global_entry.min = min(entry.min, global_entry.min) if global_entry.min > 0 else entry.min
            global_entry.max = max(entry.max, global_entry.max)
            global_entry.ttl += entry.ttl
            global_entry.count += entry.count
            global_entry.thread_count += 1
            for parent_name, parent_info in entry.parents {
                global_parent_info := map_get_ptr(&global_entry.parents, parent_name)
                global_parent_info.call_count += parent_info.call_count
            }
        }
    }
    return
}

@(private="file")
destroy_gathered_profile_infos :: proc(infos: Gathered_Profile_Infos) {
    for _, &entry in infos.entries {
        delete(entry.parents)
    }
    delete(infos.entries)
}

}
