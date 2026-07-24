// TODO: this is untested

#+build windows
package deadlock

import "core:sys/windows"
import "core:os"
import "base:runtime"

when ENABLED {

init_signal_handler :: proc "contextless" () {
    windows.SetConsoleCtrlHandler(sigint_handler, true)
}

@(private)
sigint_handler :: proc "system" (ctrl_type: windows.DWORD) -> windows.BOOL {
    if ctrl_type == windows.CTRL_C_EVENT {
        dump_checkpoints()
        os.exit(1)
        return true
    }
    return false
}

}
