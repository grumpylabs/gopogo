package sysmem

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// cgroupLimit returns the cgroup v2 or v1 memory limit, or 0 if unlimited or
// not found.
func cgroupLimit() int64 {
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		s := strings.TrimSpace(string(b))
		if s == "max" {
			return 0
		}
		n, _ := strconv.ParseInt(s, 10, 64)
		return n
	}
	if b, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		n, _ := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
		// cgroup v1 reports "unlimited" as a value near the max int64.
		if n >= 1<<60 {
			return 0
		}
		return n
	}
	return 0
}

// physical returns MemTotal from /proc/meminfo.
func physical() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kb, _ := strconv.ParseInt(fields[1], 10, 64)
			return kb * 1024
		}
	}
	return 0
}
