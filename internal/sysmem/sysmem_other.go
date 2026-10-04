//go:build !linux && !darwin

package sysmem

func cgroupLimit() int64 { return 0 }

func physical() int64 { return 0 }
