//go:build linux || darwin

package protocol

import (
	"fmt"
	"syscall"
)

// cpuTimes returns user and system CPU time as "seconds.microseconds".
func cpuTimes() (user, system string, ok bool) {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return "", "", false
	}
	return fmt.Sprintf("%d.%06d", ru.Utime.Sec, ru.Utime.Usec),
		fmt.Sprintf("%d.%06d", ru.Stime.Sec, ru.Stime.Usec), true
}
