//go:build !linux && !darwin

package protocol

// CPUSeconds is not available on this platform.
func CPUSeconds() (user, system float64, ok bool) { return 0, 0, false }
