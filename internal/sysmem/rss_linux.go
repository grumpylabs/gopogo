package sysmem

import (
	"os"
	"strconv"
	"strings"
)

// RSS returns the process's resident set size in bytes.
func RSS() int64 {
	b, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return goMappedBytes()
	}
	fields := strings.Fields(string(b))
	if len(fields) < 2 {
		return goMappedBytes()
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return goMappedBytes()
	}
	return pages * int64(os.Getpagesize())
}
