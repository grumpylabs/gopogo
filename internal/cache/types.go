package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Entry struct {
	key        []byte // stored key (may be sixpack-compressed)
	origKeyLen int    // original key length (before compression), 0 = not compressed
	value      []byte
	expireAt   int64
	accessedAt int64
	flags      uint32
	cas        uint64
}

func (e *Entry) Key() []byte {
	if e.origKeyLen > 0 {
		return Unsixpack(e.key)
	}
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

func (e *Entry) AccessedAt() int64 {
	return atomic.LoadInt64(&e.accessedAt)
}

func (e *Entry) touch(now int64) {
	atomic.StoreInt64(&e.accessedAt, now)
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
	// Account for: slice headers (24 bytes each for key, value),
	// int64 fields (expireAt, accessedAt, cas = 24), uint32 flags (4),
	// origKeyLen int (8), plus the actual key and value data.
	// Total fixed overhead: 24+24+24+4+8 = 84 bytes, rounded to 80 for alignment.
	return int64(len(e.key) + len(e.value) + 80)
}

type Bucket struct {
	entry    *Entry
	hash     uint64
	distance uint16
}

type Map struct {
	buckets     []Bucket
	numItems    int
	mask        uint64
	growAt      int
	shrinkAt    int
	loadFactor  float64
	allowShrink bool
}

func NewMap(initialSize int, loadFactor float64, allowShrink bool) *Map {
	size := 16
	for size < initialSize {
		size *= 2
	}

	return &Map{
		buckets:     make([]Bucket, size),
		mask:        uint64(size - 1),
		loadFactor:  loadFactor,
		allowShrink: allowShrink,
		growAt:      int(float64(size) * loadFactor),
		shrinkAt:    int(float64(size) * 0.10),
	}
}

type Shard struct {
	mu         sync.RWMutex
	m          *Map
	memUsed    int64
	maxMemory  int64
	numOps     uint64
	numHits    uint64
	numMisses  uint64
	numEvicted uint64
	numExpired uint64
	sweepPos   int    // current position for incremental sweep
	cas        uint64 // last CAS token issued, guarded by mu
}

func newShard(maxMemory int64, loadFactor float64, allowShrink bool) *Shard {
	return &Shard{
		m:         NewMap(16, loadFactor, allowShrink),
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

// EvictionReason indicates why an entry was evicted.
type EvictionReason int

const (
	ReasonExpired EvictionReason = iota + 1 // TTL elapsed
	ReasonLowMem                            // evicted to free memory
	ReasonCleared                           // cache was cleared
)

// EvictedFunc is called when an entry is evicted from the cache.
// The entry data is valid only for the duration of the call.
type EvictedFunc func(reason EvictionReason, key, value []byte, expires int64, flags uint32, cas uint64)

// NotifyFunc is called on every mutation to the cache.
// For inserts: newEntry is set, oldEntry is nil.
// For replaces: both are set.
// For deletes: newEntry is nil, oldEntry is set.
type NotifyFunc func(newEntry, oldEntry *Entry)

// Options configures a new Cache instance.
type Options struct {
	NumShards   int     // default 256
	MaxMemory   int64   // default 0 (unlimited)
	LoadFactor  float64 // 0.55–0.95, default 0.75
	NoSixpack   bool    // disable sixpack key compression (enabled by default)
	NoEvict     bool    // disable eviction (Store fails when memory is full)
	UseCAS      bool    // assign a fresh CAS token on every write (pogocache --cas)
	AllowShrink bool    // allow hash map shrinking on delete (default: always shrink)

	// Evicted is called for every entry evicted due to expiration, low memory,
	// or when the cache is cleared. Matching the C implementation's evicted callback.
	Evicted EvictedFunc

	// Notify is called for every change to the cache (insert, replace, delete).
	// Matching the C implementation's notify callback.
	Notify NotifyFunc
}

// MetricsRecorder is the interface the cache uses to record telemetry.
// This avoids a direct dependency on the telemetry package.
type MetricsRecorder interface {
	RecordStore(ctx context.Context, result string, elapsed time.Duration)
	RecordLoad(ctx context.Context, hit bool, elapsed time.Duration)
	RecordDelete(ctx context.Context, found bool, elapsed time.Duration)
	RecordEviction(ctx context.Context, count int64)
	RecordSave(ctx context.Context, success bool, elapsed time.Duration)
	RecordLoadFile(ctx context.Context, success bool, elapsed time.Duration)
	RecordSweep(ctx context.Context, expired int64, elapsed time.Duration)
}

type Cache struct {
	shards      []*Shard
	numShards   int
	maxMemory   int64
	metrics     MetricsRecorder
	evicted     EvictedFunc
	notify      NotifyFunc
	noSixpack   bool
	useCAS      bool
	noEvict     bool
	allowShrink bool
	totalStored atomic.Uint64 // successful writes since start, for TotalItems
}

// New creates a new Cache. Pass nil for defaults.
func New(opts *Options) *Cache {
	numShards := 256
	var maxMemory int64
	loadFactor := 0.75
	allowShrink := true

	if opts != nil {
		if opts.NumShards > 0 {
			numShards = opts.NumShards
		}
		maxMemory = opts.MaxMemory
		allowShrink = opts.AllowShrink
		if opts.LoadFactor > 0 {
			loadFactor = opts.LoadFactor
			if loadFactor < 0.55 {
				loadFactor = 0.55
			} else if loadFactor > 0.95 {
				loadFactor = 0.95
			}
		}
	}

	shards := make([]*Shard, numShards)
	shardMaxMem := maxMemory / int64(numShards)

	for i := 0; i < numShards; i++ {
		shards[i] = newShard(shardMaxMem, loadFactor, allowShrink)
	}

	c := &Cache{
		shards:    shards,
		numShards: numShards,
		maxMemory: maxMemory,
	}
	if opts != nil {
		c.evicted = opts.Evicted
		c.notify = opts.Notify
		c.noSixpack = opts.NoSixpack
		c.useCAS = opts.UseCAS
		c.noEvict = opts.NoEvict
		c.allowShrink = opts.AllowShrink
	}
	return c
}

func (c *Cache) getShard(key []byte) *Shard {
	h := hashKey(key)
	// Use upper bits for shard selection to decorrelate from bucket placement
	// which uses lower bits. This matches the C implementation's approach.
	return c.shards[(h>>32)%uint64(c.numShards)]
}

// SetMetrics attaches a metrics recorder to the cache.
func (c *Cache) SetMetrics(m MetricsRecorder) {
	c.metrics = m
}

// NumShards returns the number of shards in the cache.
func (c *Cache) NumShards() int {
	return c.numShards
}

func (c *Cache) MemUsed() int64 {
	var total int64
	for _, shard := range c.shards {
		total += shard.MemUsed()
	}
	return total
}

// ShardMemUsed returns memory used by a single shard.
func (c *Cache) ShardMemUsed(shardIdx int) int64 {
	if shardIdx < 0 || shardIdx >= c.numShards {
		return 0
	}
	return c.shards[shardIdx].MemUsed()
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

// ShardNumItems returns the item count for a single shard.
func (c *Cache) ShardNumItems(shardIdx int) int {
	if shardIdx < 0 || shardIdx >= c.numShards {
		return 0
	}
	shard := c.shards[shardIdx]
	shard.mu.RLock()
	n := shard.m.numItems
	shard.mu.RUnlock()
	return n
}

// TotalItems returns the number of successful writes (inserts and
// replacements) since the cache was created.
func (c *Cache) TotalItems() uint64 {
	return c.totalStored.Load()
}

// NumExpired returns how many entries have been removed because their TTL
// elapsed, whether found on access, deleted or swept.
func (c *Cache) NumExpired() uint64 {
	var n uint64
	for _, shard := range c.shards {
		n += shard.NumExpired()
	}
	return n
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