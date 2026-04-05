package cache

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"time"
)

var ErrOverflow = errors.New("increment or decrement would overflow")

// StoreResult indicates the outcome of a Store operation.
type StoreResult int

const (
	Inserted  StoreResult = iota // new entry was inserted
	Replaced                     // existing entry was replaced
	NotStored                    // NX/XX condition not met
)

type StoreOptions struct {
	TTL     time.Duration
	Flags   uint32
	CAS     uint64
	NX      bool // only store if key does Not eXist
	XX      bool // only store if key already eXists
	KeepTTL bool // preserve existing TTL on update
}

func (c *Cache) Store(key, value []byte, opts *StoreOptions) (StoreResult, error) {
	start := time.Now()
	shard := c.getShard(key)
	now := start.UnixNano()

	storeKey, origLen := c.compressKey(key)

	entry := &Entry{
		key:        storeKey,
		origKeyLen: origLen,
		value:      value,
		accessedAt: now,
	}

	if opts != nil {
		if opts.TTL > 0 {
			entry.expireAt = time.Now().Add(opts.TTL).UnixNano()
		}
		entry.flags = opts.Flags
		entry.cas = opts.CAS
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	atomic.AddUint64(&shard.numOps, 1)

	// Handle NX/XX conditional store at the engine level
	if opts != nil && (opts.NX || opts.XX || opts.KeepTTL) {
		existing := shard.m.get(storeKey)
		alive := existing != nil && !existing.IsExpired()

		if opts.NX && alive {
			c.recordStore(start, "not_stored")
			return NotStored, nil
		}
		if opts.XX && !alive {
			c.recordStore(start, "not_stored")
			return NotStored, nil
		}
		if opts.KeepTTL && alive {
			entry.expireAt = existing.expireAt
		}
	}

	c.evictIfNeeded(shard, entry.Size(), hashKey(storeKey))

	oldEntry := shard.m.insert(entry)

	if oldEntry != nil {
		shard.addMemUsed(-oldEntry.Size())
		shard.addMemUsed(entry.Size())
		c.fireNotifyReplaced(entry, oldEntry)
		c.recordStore(start, "replaced")
		return Replaced, nil
	}
	shard.addMemUsed(entry.Size())
	c.fireNotifyInserted(entry)
	c.recordStore(start, "inserted")
	return Inserted, nil
}

// LoadUpdate is returned from a LoadOptions.Entry callback to atomically
// update the entry during a load operation, matching C's pogocache_update.
type LoadUpdate struct {
	Value   []byte
	Flags   uint32
	Expires int64 // absolute nanosecond timestamp, 0 = no expiry
}

// LoadOptions configures a Load operation.
type LoadOptions struct {
	NoTouch bool // don't update LRU access time

	// Entry is called with the found entry's data. If it returns a non-nil
	// LoadUpdate, the entry is atomically updated in-place (under write lock).
	// Matches C's pogocache_load_opts.entry callback with update support.
	Entry func(key, value []byte, expires int64, flags uint32, cas uint64) *LoadUpdate
}

// DeleteOptions configures a Delete operation.
type DeleteOptions struct {
	// Entry is called with the entry about to be deleted. Return true to
	// proceed with deletion, false to cancel and keep the entry.
	// Matches C's pogocache_delete_opts.entry callback.
	Entry func(key, value []byte, expires int64, flags uint32, cas uint64) bool
}

func (c *Cache) Load(key []byte) (*Entry, bool) {
	return c.LoadWithOptions(key, nil)
}

func (c *Cache) LoadWithOptions(key []byte, opts *LoadOptions) (*Entry, bool) {
	start := time.Now()
	shard := c.getShard(key)
	lk := c.lookupKey(key)
	needsWrite := opts != nil && opts.Entry != nil

	// If the caller wants to update, take a write lock from the start.
	// Otherwise use read lock with upgrade-on-expired.
	if needsWrite {
		return c.loadWithWrite(start, shard, lk, opts)
	}

	shard.mu.RLock()
	entry := shard.m.get(lk)
	expired := entry != nil && entry.IsExpired()
	shard.mu.RUnlock()

	atomic.AddUint64(&shard.numOps, 1)

	if entry == nil {
		atomic.AddUint64(&shard.numMisses, 1)
		c.recordLoad(start, false)
		return nil, false
	}

	if expired {
		// Upgrade to write lock and delete the expired entry from the map.
		shard.mu.Lock()
		entry2 := shard.m.get(lk)
		if entry2 != nil && entry2.IsExpired() {
			deleted := shard.m.delete(lk, hashKey(lk))
			if deleted != nil {
				shard.addMemUsed(-deleted.Size())
				atomic.AddUint64(&shard.numExpired, 1)
				c.fireEvicted(ReasonExpired, deleted)
			}
		}
		shard.mu.Unlock()
		atomic.AddUint64(&shard.numMisses, 1)
		c.recordLoad(start, false)
		return nil, false
	}

	if opts == nil || !opts.NoTouch {
		entry.touch(time.Now().UnixNano())
	}

	atomic.AddUint64(&shard.numHits, 1)
	c.recordLoad(start, true)
	return entry, true
}

// loadWithWrite handles Load when an Entry callback is set (needs write lock).
func (c *Cache) loadWithWrite(start time.Time, shard *Shard, lk []byte, opts *LoadOptions) (*Entry, bool) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	atomic.AddUint64(&shard.numOps, 1)

	entry, idx := shard.m.getWithIndex(lk)
	if entry == nil {
		atomic.AddUint64(&shard.numMisses, 1)
		c.recordLoad(start, false)
		return nil, false
	}

	if entry.IsExpired() {
		deleted := shard.m.delete(lk, hashKey(lk))
		if deleted != nil {
			shard.addMemUsed(-deleted.Size())
			atomic.AddUint64(&shard.numExpired, 1)
			c.fireEvicted(ReasonExpired, deleted)
		}
		atomic.AddUint64(&shard.numMisses, 1)
		c.recordLoad(start, false)
		return nil, false
	}

	if opts != nil && !opts.NoTouch {
		entry.touch(time.Now().UnixNano())
	}

	// Call the Entry callback with decompressed key
	update := opts.Entry(entry.Key(), entry.value, entry.expireAt, entry.flags, entry.cas)
	if update != nil {
		// Atomically update the entry by replacing the pointer in the bucket.
		newEntry := &Entry{
			key:        entry.key,
			origKeyLen: entry.origKeyLen,
			value:      update.Value,
			expireAt:   update.Expires,
			flags:      update.Flags,
			accessedAt: entry.accessedAt,
			cas:        entry.cas + 1,
		}
		oldEntry := entry
		shard.m.buckets[idx].entry = newEntry
		shard.addMemUsed(newEntry.Size() - oldEntry.Size())
		c.fireNotifyReplaced(newEntry, oldEntry)
		entry = newEntry
	}

	atomic.AddUint64(&shard.numHits, 1)
	c.recordLoad(start, true)
	return entry, true
}

func (c *Cache) Delete(key []byte) bool {
	return c.DeleteWithOptions(key, nil)
}

func (c *Cache) DeleteWithOptions(key []byte, opts *DeleteOptions) bool {
	start := time.Now()
	shard := c.getShard(key)
	lk := c.lookupKey(key)
	h := hashKey(lk)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	atomic.AddUint64(&shard.numOps, 1)

	// If there's a cancel callback, look up entry first without deleting
	if opts != nil && opts.Entry != nil {
		entry := shard.m.get(lk)
		if entry == nil {
			c.recordDelete(start, false)
			return false
		}
		if !opts.Entry(entry.Key(), entry.value, entry.expireAt, entry.flags, entry.cas) {
			// Caller canceled the delete
			c.recordDelete(start, false)
			return false
		}
	}

	entry := shard.m.delete(lk, h)
	if entry == nil {
		c.recordDelete(start, false)
		return false
	}

	shard.addMemUsed(-entry.Size())

	// C differentiates: expired entries fire NOTIFY_EXPIRED, live entries fire NOTIFY_DELETED
	if entry.IsExpired() {
		atomic.AddUint64(&shard.numExpired, 1)
		c.fireEvicted(ReasonExpired, entry)
	} else {
		c.fireNotifyDeleted(entry)
	}

	c.recordDelete(start, true)
	return true
}

func (c *Cache) CompareAndSwap(key, value []byte, cas uint64, opts *StoreOptions) (bool, error) {
	shard := c.getShard(key)
	lk := c.lookupKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	atomic.AddUint64(&shard.numOps, 1)

	existing, idx := shard.m.getWithIndex(lk)
	if existing == nil {
		return false, nil
	}

	if existing.CAS() != cas {
		return false, nil
	}

	newEntry := &Entry{
		key:   existing.key,
		value: value,
		cas:   existing.cas + 1,
	}
	if opts != nil {
		if opts.TTL > 0 {
			newEntry.expireAt = time.Now().Add(opts.TTL).UnixNano()
		}
		newEntry.flags = opts.Flags
	}

	sizeDelta := newEntry.Size() - existing.Size()
	c.evictIfNeeded(shard, sizeDelta, hashKey(lk))

	// Replace entry pointer in bucket — old pointer remains valid for concurrent readers
	shard.m.buckets[idx].entry = newEntry
	shard.addMemUsed(sizeDelta)

	c.fireNotifyReplaced(newEntry, existing)
	return true, nil
}

func (c *Cache) Increment(key []byte, delta int64) (int64, error) {
	shard := c.getShard(key)
	storeKey, origLen := c.compressKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	atomic.AddUint64(&shard.numOps, 1)

	existing, idx := shard.m.getWithIndex(storeKey)
	if existing == nil {
		val := delta
		entry := &Entry{
			key:        storeKey,
			origKeyLen: origLen,
			value:      int64ToBytes(val),
		}

		c.evictIfNeeded(shard, entry.Size(), hashKey(storeKey))
		shard.m.insert(entry)
		shard.addMemUsed(entry.Size())

		return val, nil
	}

	currentVal := bytesToInt64(existing.value)
	// Overflow detection matching C's __builtin_add_overflow behavior
	if (delta > 0 && currentVal > math.MaxInt64-delta) ||
		(delta < 0 && currentVal < math.MinInt64-delta) {
		return currentVal, ErrOverflow
	}
	newVal := currentVal + delta

	newEntry := &Entry{
		key:      existing.key,
		value:    int64ToBytes(newVal),
		expireAt: existing.expireAt,
		flags:    existing.flags,
		cas:      existing.cas + 1,
	}

	// Replace entry pointer in bucket
	shard.m.buckets[idx].entry = newEntry
	shard.addMemUsed(newEntry.Size() - existing.Size())

	return newVal, nil
}

func (c *Cache) Sweep() int {
	start := time.Now()
	expired := 0
	for _, shard := range c.shards {
		shard.mu.Lock()
		expired += c.sweepShard(shard)
		shard.mu.Unlock()
	}
	c.recordSweep(start, expired)
	return expired
}

// SweepPoll performs an incremental sweep, checking up to pollSize entries per
// shard for expiration. It resumes from where the last poll left off, cycling
// through all buckets over time. This matches the C implementation's
// pogocache_sweep_poll which avoids locking entire shards for a full scan.
func (c *Cache) SweepPoll(pollSize int) int {
	if pollSize <= 0 {
		pollSize = 20
	}
	expired := 0
	for _, shard := range c.shards {
		shard.mu.Lock()
		expired += c.sweepPollShard(shard, pollSize)
		shard.mu.Unlock()
	}
	return expired
}

// SweepShard performs a full sweep on a single shard by index.
func (c *Cache) SweepShard(shardIdx int) int {
	if shardIdx < 0 || shardIdx >= c.numShards {
		return 0
	}
	shard := c.shards[shardIdx]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return c.sweepShard(shard)
}

func (c *Cache) sweepShard(shard *Shard) int {
	expired := 0
	toDelete := make([][]byte, 0)
	shard.m.iter(func(e *Entry) bool {
		if e.IsExpired() {
			toDelete = append(toDelete, e.key)
		}
		return true
	})
	for _, key := range toDelete {
		if entry := shard.m.delete(key, hashKey(key)); entry != nil {
			shard.addMemUsed(-entry.Size())
			expired++
			atomic.AddUint64(&shard.numExpired, 1)
			c.fireEvicted(ReasonExpired, entry)
		}
	}
	return expired
}

// SweepPollShard performs an incremental sweep on a single shard.
func (c *Cache) SweepPollShard(shardIdx, pollSize int) int {
	if shardIdx < 0 || shardIdx >= c.numShards {
		return 0
	}
	if pollSize <= 0 {
		pollSize = 20
	}
	shard := c.shards[shardIdx]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return c.sweepPollShard(shard, pollSize)
}

func (c *Cache) sweepPollShard(shard *Shard, pollSize int) int {
	expired := 0
	nbuckets := len(shard.m.buckets)
	if nbuckets == 0 {
		return 0
	}
	checked := 0
	for checked < pollSize {
		if shard.sweepPos >= nbuckets {
			shard.sweepPos = 0
		}
		bucket := &shard.m.buckets[shard.sweepPos]
		shard.sweepPos++
		checked++
		if bucket.entry == nil {
			continue
		}
		if bucket.entry.IsExpired() {
			entry := shard.m.delete(bucket.entry.key, hashKey(bucket.entry.key))
			if entry != nil {
				shard.addMemUsed(-entry.Size())
				expired++
				atomic.AddUint64(&shard.numExpired, 1)
				c.fireEvicted(ReasonExpired, entry)
			}
		}
	}
	return expired
}

// SweepEvicted is a no-op. Evicted entries are now deleted from the map
// immediately during eviction, matching the C implementation's behavior.
func (c *Cache) SweepEvicted() int {
	return 0
}

func (c *Cache) Iterate(fn func(*Entry) bool) {
	for _, shard := range c.shards {
		if !c.iterateShard(shard, fn) {
			break
		}
	}
}

// IterateShard iterates over entries in a single shard.
func (c *Cache) IterateShard(shardIdx int, fn func(*Entry) bool) {
	if shardIdx < 0 || shardIdx >= c.numShards {
		return
	}
	c.iterateShard(c.shards[shardIdx], fn)
}

func (c *Cache) iterateShard(shard *Shard, fn func(*Entry) bool) bool {
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	cont := true
	shard.m.iter(func(e *Entry) bool {
		if e.IsExpired() {
			return true
		}
		if !fn(e) {
			cont = false
			return false
		}
		return true
	})
	return cont
}

func (c *Cache) Clear() {
	for _, shard := range c.shards {
		c.clearShard(shard)
	}
}

// ClearShard clears a single shard by index.
func (c *Cache) ClearShard(shardIdx int) {
	if shardIdx < 0 || shardIdx >= c.numShards {
		return
	}
	c.clearShard(c.shards[shardIdx])
}

func (c *Cache) clearShard(shard *Shard) {
	shard.mu.Lock()
	if c.evicted != nil {
		shard.m.iter(func(e *Entry) bool {
			c.fireEvicted(ReasonCleared, e)
			return true
		})
	}
	shard.m = NewMap(16, shard.m.loadFactor, shard.m.allowShrink)
	atomic.StoreInt64(&shard.memUsed, 0)
	shard.mu.Unlock()
}

func (c *Cache) evictIfNeeded(shard *Shard, requiredSpace int64, skipHash uint64) {
	if shard.maxMemory <= 0 || c.noEvict {
		return
	}
	for shard.MemUsed()+requiredSpace > shard.maxMemory && shard.m.numItems > 0 {
		entries := shard.m.randomEntries(2, skipHash)
		if len(entries) == 0 {
			break
		}

		var toEvict *Entry
		if len(entries) == 1 {
			toEvict = entries[0]
		} else {
			e0Expired := entries[0].IsExpired()
			e1Expired := entries[1].IsExpired()

			switch {
			case e0Expired && !e1Expired:
				toEvict = entries[0]
			case !e0Expired && e1Expired:
				toEvict = entries[1]
			case e0Expired && e1Expired:
				if entries[0].ExpireAt() < entries[1].ExpireAt() {
					toEvict = entries[0]
				} else {
					toEvict = entries[1]
				}
			default:
				// Neither expired — evict the one with oldest access time (LRU-style),
				// matching the C implementation's 2-random eviction strategy.
				if entries[0].AccessedAt() <= entries[1].AccessedAt() {
					toEvict = entries[0]
				} else {
					toEvict = entries[1]
				}
			}
		}

		// Delete from map immediately (matches C implementation behavior)
		deleted := shard.m.delete(toEvict.key, hashKey(toEvict.key))
		if deleted != nil {
			shard.addMemUsed(-deleted.Size())
			atomic.AddUint64(&shard.numEvicted, 1)
			c.fireEvicted(ReasonLowMem, deleted)
			c.recordEvictions(1)
		}
	}
}

// Metrics recording helpers — all nil-safe

func (c *Cache) recordStore(start time.Time, result string) {
	if c.metrics != nil {
		c.metrics.RecordStore(context.Background(), result, float64(time.Since(start).Microseconds())/1000.0)
	}
}

func (c *Cache) recordLoad(start time.Time, hit bool) {
	if c.metrics != nil {
		c.metrics.RecordLoad(context.Background(), hit, float64(time.Since(start).Microseconds())/1000.0)
	}
}

func (c *Cache) recordDelete(start time.Time, found bool) {
	if c.metrics != nil {
		c.metrics.RecordDelete(context.Background(), found, float64(time.Since(start).Microseconds())/1000.0)
	}
}

func (c *Cache) recordEvictions(count int64) {
	if c.metrics != nil && count > 0 {
		c.metrics.RecordEviction(context.Background(), count)
	}
}

func (c *Cache) recordSweep(start time.Time, expired int) {
	if c.metrics != nil {
		c.metrics.RecordSweep(context.Background(), int64(expired), float64(time.Since(start).Microseconds())/1000.0)
	}
}

// Key compression helpers

// compressKey returns the key to store and the original length.
// If origLen > 0, the key was compressed.
func (c *Cache) compressKey(key []byte) ([]byte, int) {
	if c.noSixpack {
		return key, 0
	}
	packed := Sixpack(key)
	if packed == nil {
		return key, 0
	}
	return packed, len(key)
}

// lookupKey returns the key form used for map lookups.
func (c *Cache) lookupKey(key []byte) []byte {
	if c.noSixpack {
		return key
	}
	packed := Sixpack(key)
	if packed == nil {
		return key
	}
	return packed
}

// Callback helpers

func (c *Cache) fireEvicted(reason EvictionReason, e *Entry) {
	if c.evicted != nil {
		c.evicted(reason, e.Key(), e.value, e.expireAt, e.flags, e.cas)
	}
}

func (c *Cache) fireNotifyInserted(newEntry *Entry) {
	if c.notify != nil {
		c.notify(newEntry, nil)
	}
}

func (c *Cache) fireNotifyReplaced(newEntry, oldEntry *Entry) {
	if c.notify != nil {
		c.notify(newEntry, oldEntry)
	}
}

func (c *Cache) fireNotifyDeleted(oldEntry *Entry) {
	if c.notify != nil {
		c.notify(nil, oldEntry)
	}
}

func int64ToBytes(n int64) []byte {
	b := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		b[i] = byte(n)
		n >>= 8
	}
	return b
}

func bytesToInt64(b []byte) int64 {
	if len(b) != 8 {
		return 0
	}
	var n int64
	for i := 0; i < 8; i++ {
		n = (n << 8) | int64(b[i])
	}
	return n
}