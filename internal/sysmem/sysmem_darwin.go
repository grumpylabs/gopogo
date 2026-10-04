package sysmem

import "golang.org/x/sys/unix"

func cgroupLimit() int64 { return 0 }

func physical() int64 {
	n, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0
	}
	return int64(n)
}
