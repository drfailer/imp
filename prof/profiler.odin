package profiler

import "core:fmt"
import "core:time"
import "core:sync"
import "core:os"
import "core:strings"
import "core:mem"
import vmem "core:mem/virtual"
import "base:runtime"

ENABLED :: #config(PROF_ENABLED, false)

when ENABLED {

#assert(!ODIN_NO_CRT, "Prof requires the C Runtime (CRT) to be enabled!")

MAX_STACK_SIZE :: #config(PROF_MAX_STACK_SIZE, 64)
INIT_ENTRIES_CAPACITY :: #config(PROF_INIT_ENTRIES_CAPACITY, 32)

// global stopwatch used to compute the global execution time
GLOBAL_STOPWATCH: time.Stopwatch
PROFILERS: [dynamic]^Profiler
PROFILERS_MUTEX: sync.Mutex

@(thread_local) PROFILER: ^Profiler

Profiler :: struct {
    index: int,
    root: ^Profile_Entry,
    current: ^Profile_Entry,
    arena: vmem.Arena,
    stopwatch: time.Stopwatch,
}

Profile_Entry :: struct {
    name: string,
    parent: ^Profile_Entry,
    children: map[string]^Profile_Entry,
    stopwatch: time.Stopwatch,
    min: time.Duration,
    max: time.Duration,
    ttl: time.Duration,
    count: int,
}

} else {

Profiler :: struct {}

}

// init ////////////////////////////////////////////////////////////////////////

when ENABLED {

@(init)
init :: proc "contextless" () {
    context = runtime.default_context()
    time.stopwatch_start(&GLOBAL_STOPWATCH)
    PROFILERS = make([dynamic]^Profiler)
}

@(fini)
fini :: proc "contextless" () {
    context = runtime.default_context()
    clear()
    delete(PROFILERS)
}

clear :: proc "contextless" () {
    context = runtime.default_context()
    for profiler in PROFILERS {
        vmem.arena_destroy(&profiler.arena)
        free(profiler)
    }
    clear_dynamic_array(&PROFILERS)
}

reset :: proc() {
    time.stopwatch_reset(&GLOBAL_STOPWATCH)
    time.stopwatch_start(&GLOBAL_STOPWATCH)
}

new_thread :: proc(parent_path: string) {
    profiler := get_profiler()
    profiler.root.name = parent_path
}

get_parent_path :: proc() -> string {
    profiler := get_profiler()
    allocator := vmem.arena_allocator(&profiler.arena)
    path := make([dynamic]u8, allocator = allocator)
    build_path_up_to_root(profiler.current, &path)
    return string(path[:])
}

@(private="file")
build_path_up_to_root :: proc(entry: ^Profile_Entry, acc: ^[dynamic]u8) {
    if entry == nil do return
    if entry.parent != nil {
        build_path_up_to_root(entry.parent, acc)
        append(acc, '/')
    }
    append(acc, entry.name)
}

get_profiler :: proc "contextless" () -> ^Profiler {
    context = runtime.default_context()
    if PROFILER == nil {
        PROFILER = new(Profiler)

        if sync.guard(&PROFILERS_MUTEX) {
            append(&PROFILERS, PROFILER)
        }

        err := vmem.arena_init_growing(&PROFILER.arena)
        ensure(err == nil, "failed to initialize profiler arena")
        allocator := vmem.arena_allocator(&PROFILER.arena)

        PROFILER.root = new(Profile_Entry, allocator)
        PROFILER.root.name = "main"
        PROFILER.root.parent = nil
        PROFILER.root.children = make(map[string]^Profile_Entry, INIT_ENTRIES_CAPACITY, allocator)
        PROFILER.current = PROFILER.root
    }
    return PROFILER
}

} else {

init :: proc() {}
fini :: proc() {}
clear :: proc() {}
reset :: proc() {}
new_thread :: proc(parent_profiler: ^Profiler) {}
get_profiler :: proc() -> ^Profiler { return nil }

}

// region //////////////////////////////////////////////////////////////////////

//
// profile a specific region of the code
//

when ENABLED {

region_begin :: proc(name: string) {
    profiler := get_profiler()

    // Get or create child in current node
    child_entry: ^Profile_Entry
    if existing, found := profiler.current.children[name]; found {
        child_entry = existing
    } else {
        allocator := vmem.arena_allocator(&profiler.arena)
        child_entry = new(Profile_Entry, allocator)
        child_entry.name = name
        child_entry.parent = profiler.current
        child_entry.children = make(map[string]^Profile_Entry, INIT_ENTRIES_CAPACITY, allocator)
        profiler.current.children[name] = child_entry
    }

    // Move down tree
    profiler.current = child_entry

    // Start timing
    time.stopwatch_reset(&child_entry.stopwatch)
    time.stopwatch_start(&child_entry.stopwatch)
}

region_end :: proc(name: string) {
    profiler := get_profiler()
    entry := profiler.current
    assert(entry.name == name, "region_end mismatch")

    // Stop timer and update stats
    time.stopwatch_stop(&entry.stopwatch)
    duration := time.stopwatch_duration(entry.stopwatch)

    entry.min = min(entry.min, duration) if entry.min > 0 else duration
    entry.max = max(entry.max, duration)
    entry.ttl += duration
    entry.count += 1

    // Navigate up tree
    profiler.current = entry.parent
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

report :: proc(target_paths: []string = {}) {
    infos := gather_profile_infos()
    defer destroy_gathered_profile_infos(infos)

    print_entry :: proc(path: string, infos: Gathered_Profile_Infos) {
        entry := &infos.entries[path]
        avg := time.Duration(f64(entry.ttl) / f64(entry.count))
        ttl_avg := time.Duration(f64(entry.ttl) / f64(entry.thread_count))
        ratio   := f64(ttl_avg) / f64(infos.global_time)
        percent := 100 * ratio

        fmt.println("PATH:", path)
        fmt.printfln("THREAD TIME: {} ({})", entry.ttl, entry.thread_count)
        fmt.printfln("TOTAL TIME: {} ({:.3f}%%)", ttl_avg, percent)
        fmt.printfln("ELEMENT TIME: avg = {}, min = {}, max = {} ({})", avg, entry.min, entry.max, entry.count)
        fmt.println()
    }

    fmt.println("===================================== PROF =====================================")
    fmt.println()
    if len(target_paths) == 0 {
        for path, entry in infos.entries {
            print_entry(path, infos)
        }
    } else {
        // Only print exact path suffix matches
        for path, entry in infos.entries {
            for target in target_paths {
                if strings.has_suffix(path, target) {
                    print_entry(path, infos)
                    break
                }
            }
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

    for path, entry in infos.entries {
        avg := time.Duration(f64(entry.ttl) / f64(entry.count))
        ttl_avg := time.Duration(f64(entry.ttl) / f64(entry.thread_count))
        percent := 100 * f64(ttl_avg) / f64(infos.global_time)
        r, g, b := time_to_rgb(ttl_avg, infos.global_time)

        fmt.fprintfln(file, "\"{}\" [label=\"{}\\ncount = {}\\navg = {}, min = {}, max = {}\\nttl = {} ({:.3f}%%)\\nthreads = {} ({})\",shape=rectangle,color=\"#%2X%2X%2X\",penwidth=2];",
            path, entry.name, entry.count, avg, entry.min, entry.max, ttl_avg,
            percent, entry.thread_count, entry.ttl, r, g, b)
    }

    // Generate edges with labels
    for path, entry in infos.entries {
        parent_path := entry.parent_path
        if parent_path != "" {
            // Get parent entry to compute percentage
            parent_entry, parent_found := &infos.entries[parent_path]
            parent_ttl := parent_entry.ttl if parent_found else infos.global_time
            edge_percent := 100 * (f64(entry.ttl) / f64(parent_ttl))
            fmt.fprintfln(file, "\"{}\" -> \"{}\" [label=\"x {} / {:.3f}%%\"];",
                parent_path, path, entry.count, edge_percent)
        } else if path != "main" {
            // Direct child of main
            edge_percent := 100 * (f64(entry.ttl) / f64(infos.global_time))
            fmt.fprintfln(file, "main -> \"{}\" [label=\"x {} / {:.3f}%%\"];",
                path, entry.count, edge_percent)
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
    name: string,
    min: time.Duration,
    max: time.Duration,
    ttl: time.Duration,
    count: int,
    thread_count: uint,
    parent_path: string,  // For edge generation
}

@(private="file")
Gathered_Profile_Infos :: struct {
    entries: map[string]Gathered_Profile_Entry,
    global_time: time.Duration,
}

@(private="file")
gather_profile_infos :: proc(allocator := context.allocator) -> (infos: Gathered_Profile_Infos) {
    infos.global_time = time.stopwatch_duration(GLOBAL_STOPWATCH)
    infos.entries = make(map[string]Gathered_Profile_Entry, allocator)

    // Traverse all profiler trees
    for profiler in PROFILERS {
        if profiler.root != nil {
            flatten_tree_recursive(profiler.root, "", &infos.entries, allocator)
        }
    }
    return
}

@(private="file")
flatten_tree_recursive :: proc(
    entry: ^Profile_Entry,
    parent_path: string,
    gathered: ^map[string]Gathered_Profile_Entry,
    allocator := context.allocator,
) {
    // Build path
    current_path: string
    if parent_path == "" {
        current_path = entry.name
    } else {
        current_path = fmt.aprintf("{}/{}", parent_path, entry.name, allocator=allocator)
    }

    // Aggregate if entry was called
    if entry.count > 0 {
        global_entry := map_get_ptr(gathered, current_path)

        global_entry.min = min(entry.min, global_entry.min) if global_entry.min > 0 else entry.min
        global_entry.max = max(entry.max, global_entry.max)
        global_entry.ttl += entry.ttl
        global_entry.count += entry.count
        global_entry.thread_count += 1
        global_entry.name = entry.name
        global_entry.parent_path = parent_path if parent_path != "" else ""
    }

    // Recurse children
    for _, child in entry.children {
        flatten_tree_recursive(child, current_path, gathered, allocator)
    }
}

@(private="file")
destroy_gathered_profile_infos :: proc(infos: Gathered_Profile_Infos) {
    delete(infos.entries)
}

}
