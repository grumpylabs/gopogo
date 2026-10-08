package cache

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

// BenchmarkShards measures lock contention across shard counts: every
// goroutine works on random keys from a shared keyspace, so contention
// depends only on how many shards the keys spread over.
func BenchmarkShards(b *testing.B) {
	const keys = 1 << 20
	keyNames := make([][]byte, keys)
	for i := range keyNames {
		keyNames[i] = []byte(fmt.Sprintf("key:%d", i))
	}
	value := make([]byte, 64)

	for _, writes := range []int{10, 100} {
		for _, shards := range []int{16, 64, 256, 1024, 4096} {
			name := fmt.Sprintf("writes=%d%%/shards=%d", writes, shards)
			b.Run(name, func(b *testing.B) {
				c := New(&Options{NumShards: shards})
				for _, k := range keyNames {
					c.Store(k, value, nil)
				}
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					r := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
					for pb.Next() {
						k := keyNames[r.IntN(keys)]
						if r.IntN(100) < writes {
							c.Store(k, value, nil)
						} else {
							c.Load(k)
						}
					}
				})
			})
		}
	}
}
