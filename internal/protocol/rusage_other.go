//go:build !linux && !darwin

package protocol

func cpuTimes() (user, system string, ok bool) { return "", "", false }
