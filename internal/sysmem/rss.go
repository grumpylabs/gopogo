package sysmem

import "runtime/metrics"

// goMappedBytes returns the memory the Go runtime has mapped from the OS, an
// upper bound on resident memory, for platforms without a direct RSS source.
func goMappedBytes() int64 {
	s := []metrics.Sample{{Name: "/memory/classes/total:bytes"}}
	metrics.Read(s)
	if s[0].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	return int64(s[0].Value.Uint64())
}
