//go:build !linux

package sysmem

// RSS returns the memory the Go runtime has mapped, an upper bound on the
// process's resident set size; there is no portable direct source.
func RSS() int64 {
	return goMappedBytes()
}
