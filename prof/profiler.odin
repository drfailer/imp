package profiler

import "core:fmt"
import "core:time"
import "core:sync"
import "core:os"
import "core:strings"
import vmem "core:mem/virtual"
import "core:math"
import "base:runtime"

ENABLED :: #config(PROF_ENABLED, false)

Profiler :: struct {
    thread_profilers: [dynamic]^Thread_Profiler,
    mutex: sync.Mutex,
    stopwatch: time.Stopwatch,
}

Thread_Profiler :: struct {
    profiles: map[string]^Profile,
    disabled: bool,
    arena: vmem.Arena,
}

Profile :: struct {
    stopwatch: time.Stopwatch,
    measure: Measure,
    // TODO: info: string,
    thread_count: u64,
}

when ENABLED {

PROFILER: Profiler

@(thread_local)
THREAD_PROFILER: ^Thread_Profiler

@(init)
init :: proc "contextless" () {
    context = runtime.default_context()
    PROFILER.thread_profilers = make([dynamic]^Thread_Profiler)
    time.stopwatch_start(&PROFILER.stopwatch)
}

@(fini)
fini :: proc "contextless" () {
    context = runtime.default_context()
    for profiler in PROFILER.thread_profilers {
        delete(profiler.profiles)
        vmem.arena_destroy(&profiler.arena)
    }
    delete(PROFILER.thread_profilers)
}

@(private)
init_thread :: proc() {
    THREAD_PROFILER = new(Thread_Profiler)
    err := vmem.arena_init_growing(&THREAD_PROFILER.arena)
    ensure(err == nil, "Failed to initialize thread profiler arena.")
    THREAD_PROFILER.profiles = make(map[string]^Profile)
    if sync.guard(&PROFILER.mutex) {
        append(&PROFILER.thread_profilers, THREAD_PROFILER)
    }
}

@(private)
get_thread_profiler :: proc() -> ^Thread_Profiler {
    if THREAD_PROFILER == nil do init_thread()
    return THREAD_PROFILER
}

get_profile :: proc(name: string) -> ^Profile {
    profiler := get_thread_profiler()
    if profile, ok := profiler.profiles[name]; ok {
        return profile
    }
    profile := new(Profile, allocator = vmem.arena_allocator(&profiler.arena))
    measure_init(&profile.measure)
    profiler.profiles[name] = profile
    return profile
}

region_begin_profile :: proc(profile: ^Profile) -> ^Profile {
    time.stopwatch_reset(&profile.stopwatch)
    time.stopwatch_start(&profile.stopwatch)
    return profile
}

region_begin_name :: proc(name: string) -> ^Profile {
    return region_begin_profile(get_profile(name))
}

region_begin :: proc { region_begin_profile, region_begin_name }

region_end :: proc(profile: ^Profile) {
    time.stopwatch_stop(&profile.stopwatch)
    measure_add_value(&profile.measure, cast(f64) time.stopwatch_duration(profile.stopwatch))
}

@(deferred_out=region_end)
region :: proc(name: string) -> ^Profile {
    return region_begin(name)
}

@(deferred_out=region_end)
procedure :: proc(loc := #caller_location) -> ^Profile {
    return region_begin(loc.procedure)
}

profile_report :: proc(profiles_to_print: ..string) {
    sync.guard(&PROFILER.mutex)
    ttl_time := time.stopwatch_duration(PROFILER.stopwatch)
    merged_profiles := make(map[string]Profile)
    defer delete(merged_profiles)

    for profiler in PROFILER.thread_profilers {
        for name, profile in profiler.profiles {
            if name in merged_profiles {
                mp := &merged_profiles[name]
                measure_merge(&mp.measure, profile.measure)
                mp.thread_count += 1
            } else {
                mp := profile^
                mp.thread_count = 1
                merged_profiles[name] = mp
            }
        }
    }

    print_profile := proc(name: string, profile: Profile, ttl_time: time.Duration) {
        mean   := time.Duration(profile.measure.mean)
        stddev := time.Duration(measure_stddev(profile.measure))
        min    := time.Duration(profile.measure.min)
        max    := time.Duration(profile.measure.max)
        ttl    := time.Duration(profile.measure.ttl)
        count  := profile.measure.count
        ratio  := profile.measure.ttl / (f64(profile.thread_count) * f64(ttl_time)) * 100
        fmt.printfln("{}: {} +- {} [{}; {}] ({} | {} | {}) {:.1f}%%", name, mean, stddev, min, max, ttl, profile.thread_count, count, ratio)
    }

    fmt.println("===================================== PROF =====================================")
    if len(profiles_to_print) == 0 {
        for name, profile in merged_profiles {
            print_profile(name, profile, ttl_time)
        }
    } else {
        for name in profiles_to_print {
            print_profile(name, merged_profiles[name], ttl_time)
        }
    }
    fmt.println("================================================================================")
}

} else {

@(thread_local) DUMMY_PROFILE: Profile

region_begin :: proc(name: string) -> ^Profile {
    return &DUMMY_PROFILE
}

region_end :: proc(profile: ^Profile) {}

@(deferred_out=region_end)
region :: proc(name: string) -> ^Profile {
    return region_begin(name)
}

@(deferred_out=region_end)
procedure :: proc(loc := #caller_location) -> ^Profile {
    return region_begin(loc.procedure)
}

profile_report :: proc() {}

}

Measure :: struct {
    count: u64,
    mean: f64,
    m2: f64,
    min: f64,
    max: f64,
    ttl: f64,
}

measure_init :: proc(m: ^Measure) {
    m.count = 0
    m.mean = 0
    m.m2 = 0
    m.min = max(f64)
    m.max = 0
    m.ttl = 0
}

measure_add_value :: proc(m: ^Measure, x: f64) {
    m.count += 1
    old_mean := m.mean
    m.mean += (x - m.mean) / f64(m.count)
    m.m2   += (x - old_mean) * (x - m.mean)
    m.min = min(m.min, x)
    m.max = max(m.max, x)
    m.ttl += x
}

measure_merge :: proc(dst: ^Measure, m: Measure) {
    if m.count == 0 do return
    count  := dst.count + m.count
    delta  := dst.mean - m.mean
    mean   := (f64(dst.count) * dst.mean + f64(m.count) * m.mean) / f64(count)
    m2     := dst.m2 + m.m2 + delta * delta * f64(dst.count * m.count) / f64(count)

    dst.count = count
    dst.mean  = mean
    dst.m2    = m2
    dst.min   = min(dst.min, m.min)
    dst.max   = max(dst.max, m.max)
    dst.ttl  += m.ttl
}

measure_stddev :: proc(m: Measure) -> f64 {
    return math.sqrt(m.m2 / f64(m.count))
}
