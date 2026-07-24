#+build linux
package deadlock

import "core:sys/posix"
import "core:os"
import "core:c"
import "core:mem"
import "base:runtime"


when ENABLED {

@(private)
sigint_handler :: proc "cdecl" (signum: posix.Signal) {
    dump_checkpoints()
    os.exit(1)
}

// Call this from your deadlock init() function
init_signal_handler :: proc "contextless" () {
    sa: posix.sigaction_t
    sa.sa_handler = sigint_handler
    posix.sigemptyset(&sa.sa_mask)
    sa.sa_flags = {}
    posix.sigaction(.SIGINT, &sa, nil)
}

}
