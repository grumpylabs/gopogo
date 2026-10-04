// Package sysmem reports how much memory the process may use, for sizing
// --maxmemory as a percentage.
package sysmem

// Available returns the memory available to the process in bytes: the
// container (cgroup) memory limit when one is set, capped at physical memory.
// It returns 0 when it cannot be determined.
func Available() int64 {
	phys := physical()
	if lim := cgroupLimit(); lim > 0 && (phys == 0 || lim < phys) {
		return lim
	}
	return phys
}
