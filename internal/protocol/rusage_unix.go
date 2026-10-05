//go:build linux || darwin

package protocol

import "syscall"

// CPUSeconds returns the process's user and system CPU time in seconds.
func CPUSeconds() (user, system float64, ok bool) {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return 0, 0, false
	}
	tv := func(sec, usec int64) float64 { return float64(sec) + float64(usec)/1e6 }
	return tv(int64(ru.Utime.Sec), int64(ru.Utime.Usec)), tv(int64(ru.Stime.Sec), int64(ru.Stime.Usec)), true
}
