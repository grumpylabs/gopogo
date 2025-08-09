package cache

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type Entry struct {
	key        []byte
	value      []byte
	expireAt   int64
	flags      uint32
	cas        uint64
	metadata   unsafe.Pointer
}

func (e *Entry) Key() []byte {
	return e.key
}

func (e *Entry) Value() []byte {
	return e.value
}

func (e *Entry) SetValue(v []byte) {
	e.value = v
}

func (e *Entry) ExpireAt() int64 {
	return atomic.LoadInt64(&e.expireAt)
}

func (e *Entry) SetExpireAt(t int64) {
	atomic.StoreInt64(&e.expireAt, t)
}

func (e *Entry) IsExpired() bool {
	expireAt := e.ExpireAt()
	return expireAt > 0 && expireAt < time.Now().UnixNano()
}

func (e *Entry) Flags() uint32 {
	return atomic.LoadUint32(&e.flags)
}

func (e *Entry) SetFlags(f uint32) {
	atomic.StoreUint32(&e.flags, f)
}

func (e *Entry) CAS() uint64 {
	return atomic.LoadUint64(&e.cas)
}

func (e *Entry) IncrementCAS() uint64 {
	return atomic.AddUint64(&e.cas, 1)
}

func (e *Entry) Size() int64 {
	return int64(len(e.key) + len(e.value) + 24)
}

type Bucket struct {
	entry    *Entry
	hash     uint64
	distance uint16
}

type Map struct {
	buckets  []Bucket
	numItems int
	mask     uint64
	growAt   int
	shrinkAt int
}

func NewMap(initialSize int) *Map {
	size := 16
	for size < initialSize {
		size *= 2
	}
	
	return &Map{
		buckets:  make([]Bucket, size),
		mask:     uint64(size - 1),
		growAt:   int(float64(size) * 0.75),
		shrinkAt: int(float64(size) * 0.10),
	}
}

type Shard struct {
	mu          sync.RWMutex
	m           *Map
	memUsed     int64
	maxMemory   int64
	numOps      uint64
	numHits     uint64
	numMisses   uint64
	numEvicted  uint64
	numExpired  uint64
}

func NewShard(maxMemory int64) *Shard {
	return &Shard{
		m:         NewMap(16),
		maxMemory: maxMemory,
	}
}

func (s *Shard) MemUsed() int64 {
	return atomic.LoadInt64(&s.memUsed)
}

func (s *Shard) addMemUsed(delta int64) {
	atomic.AddInt64(&s.memUsed, delta)
}

func (s *Shard) NumOps() uint64 {
	return atomic.LoadUint64(&s.numOps)
}

func (s *Shard) NumHits() uint64 {
	return atomic.LoadUint64(&s.numHits)
}

func (s *Shard) NumMisses() uint64 {
	return atomic.LoadUint64(&s.numMisses)
}

func (s *Shard) NumEvicted() uint64 {
	return atomic.LoadUint64(&s.numEvicted)
}

func (s *Shard) NumExpired() uint64 {
	return atomic.LoadUint64(&s.numExpired)
}

type Cache struct {
	shards    []*Shard
	numShards int
	maxMemory int64
}

func New(numShards int, maxMemory int64) *Cache {
	if numShards <= 0 {
		numShards = 16
	}
	
	shards := make([]*Shard, numShards)
	shardMaxMem := maxMemory / int64(numShards)
	
	for i := 0; i < numShards; i++ {
		shards[i] = NewShard(shardMaxMem)
	}
	
	return &Cache{
		shards:    shards,
		numShards: numShards,
		maxMemory: maxMemory,
	}
}

func (c *Cache) getShard(key []byte) *Shard {
	h := hashKey(key)
	return c.shards[h%uint64(c.numShards)]
}

func (c *Cache) MemUsed() int64 {
	var total int64
	for _, shard := range c.shards {
		total += shard.MemUsed()
	}
	return total
}

func (c *Cache) NumItems() int {
	var total int
	for _, shard := range c.shards {
		shard.mu.RLock()
		total += shard.m.numItems
		shard.mu.RUnlock()
	}
	return total
}

func (c *Cache) Stats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	var ops, hits, misses, evicted, expired uint64
	var memUsed int64
	var numItems int
	
	for _, shard := range c.shards {
		ops += shard.NumOps()
		hits += shard.NumHits()
		misses += shard.NumMisses()
		evicted += shard.NumEvicted()
		expired += shard.NumExpired()
		memUsed += shard.MemUsed()
		
		shard.mu.RLock()
		numItems += shard.m.numItems
		shard.mu.RUnlock()
	}
	
	stats["num_items"] = numItems
	stats["mem_used"] = memUsed
	stats["max_memory"] = c.maxMemory
	stats["num_ops"] = ops
	stats["num_hits"] = hits
	stats["num_misses"] = misses
	stats["num_evicted"] = evicted
	stats["num_expired"] = expired
	
	if ops > 0 {
		stats["hit_rate"] = float64(hits) / float64(hits+misses)
	} else {
		stats["hit_rate"] = 0.0
	}
	
	return stats
}