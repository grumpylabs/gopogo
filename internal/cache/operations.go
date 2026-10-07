package cache

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	ErrOverflow   = errors.New("increment or decrement would overflow")
	ErrNotInteger = errors.New("value is not an integer or out of range")
	ErrNotFound   = errors.New("not found")
	// ErrOutOfMemory is returned for a write that would exceed MaxMemory
	// when eviction is disabled (NoEvict).
	ErrOutOfMemory = errors.New("out of memory")
)

// nextCAS returns the CAS token for a new write to shard, or 0 when CAS is
// disabled. A non-zero want (e.g. restored from a save file) is kept and the
// shard counter advanced past it. The caller must hold shard.mu for writing.
func (c *Cache) nextCAS(shard *Shard, want uint64) uint64 {
	if !c.useCAS {
		return 0
	}
	if want != 0 {
		if want > shard.cas {
			shard.cas = want
		}
		return want
	}
	shard.cas++
	return shard.cas
}

// StoreResult indicates the outcome of a Store operation.
type StoreResult int

const (
	Inserted  StoreResult = iota // new entry was inserted
	Replaced                     // existing entry was replaced
	NotStored                    // NX/XX condition not met
	NoMemory                     // eviction is disabled and the cache is full
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
	}

	shard.mu.Lock()
	defer shard.mu.Unlock()

	atomic.AddUint64(&shard.numOps, 1)

	// Handle NX/XX conditional store at the engine level
	if opts != nil && (opts.NX || opts.XX || opts.KeepTTL) {
		existing := shard.m.get(storeKey, origLen > 0)
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

	if opts != nil {
		entry.cas = c.nextCAS(shard, opts.CAS)
	} else {
		entry.cas = c.nextCAS(shard, 0)
	}

	if !c.makeRoom(shard, c.growth(shard, entry), hashKey(storeKey)) {
		c.recordStore(start, "no_memory")
		return NoMemory, ErrOutOfMemory
	}

	oldEntry := shard.m.insert(entry)

	if oldEntry != nil {
		shard.addMemUsed(-oldEntry.Size())
		shard.addMemUsed(entry.Size())
		c.fireNotifyReplaced(entry, oldEntry)
		c.recordStore(start, "replaced")
		c.totalStored.Add(1)
		return Replaced, nil
	}
	shard.addMemUsed(entry.Size())
	c.fireNotifyInserted(entry)
	c.recordStore(start, "inserted")
	c.totalStored.Add(1)
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
	lk, packed := c.lookupKey(key)
	needsWrite := opts != nil && opts.Entry != nil

	// If the caller wants to update, take a write lock from the start.
	// Otherwise use read lock with upgrade-on-expired.
	if needsWrite {
		return c.loadWithWrite(start, shard, lk, packed, opts)
	}

	shard.mu.RLock()
	entry := shard.m.get(lk, packed)
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
		entry2 := shard.m.get(lk, packed)
		if entry2 != nil && entry2.IsExpired() {
			deleted := shard.m.deleteEntry(entry2)
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
func (c *Cache) loadWithWrite(start time.Time, shard *Shard, lk []byte, packed bool, opts *LoadOptions) (*Entry, bool) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	atomic.AddUint64(&shard.numOps, 1)

	entry, idx := shard.m.getWithIndex(lk, packed)
	if entry == nil {
		atomic.AddUint64(&shard.numMisses, 1)
		c.recordLoad(start, false)
		return nil, false
	}

	if entry.IsExpired() {
		deleted := shard.m.deleteEntry(entry)
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
			cas:        c.nextCAS(shard, 0),
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
	lk, packed := c.lookupKey(key)
	h := hashKey(lk)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	atomic.AddUint64(&shard.numOps, 1)

	// If there's a cancel callback, look up entry first without deleting
	if opts != nil && opts.Entry != nil {
		entry := shard.m.get(lk, packed)
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

	entry := shard.m.delete(lk, h, packed)
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
	lk, packed := c.lookupKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	atomic.AddUint64(&shard.numOps, 1)

	existing, idx := shard.m.getWithIndex(lk, packed)
	if existing == nil || existing.IsExpired() {
		return false, ErrNotFound
	}

	// With CAS disabled every compare-and-swap fails, as in pogocache.
	if !c.useCAS || existing.CAS() != cas {
		return false, nil
	}

	newEntry := &Entry{
		key:        existing.key,
		origKeyLen: existing.origKeyLen,
		value:      value,
		accessedAt: time.Now().UnixNano(),
		cas:        c.nextCAS(shard, 0),
	}
	if opts != nil {
		if opts.TTL > 0 {
			newEntry.expireAt = time.Now().Add(opts.TTL).UnixNano()
		}
		newEntry.flags = opts.Flags
	}

	sizeDelta := newEntry.Size() - existing.Size()
	if !c.makeRoom(shard, sizeDelta, hashKey(lk)) {
		return false, ErrOutOfMemory
	}
	// Eviction can shift entries in the table; find the bucket again.
	if _, idx = shard.m.getWithIndex(lk, packed); idx < 0 {
		return false, ErrNotFound
	}

	// Replace entry pointer in bucket — old pointer remains valid for concurrent readers
	shard.m.buckets[idx].entry = newEntry
	shard.addMemUsed(sizeDelta)

	c.fireNotifyReplaced(newEntry, existing)
	c.totalStored.Add(1)
	return true, nil
}

// Increment adds delta to the signed 64-bit decimal integer stored at key and
// returns the result. A missing key counts as 0. The value is stored as
// decimal text, so it reads back with Load like any other value.
func (c *Cache) Increment(key []byte, delta int64) (int64, error) {
	var out int64
	err := c.Update(key, func(cur []byte, found bool) ([]byte, error) {
		var n int64
		if found {
			v, err := strconv.ParseInt(string(cur), 10, 64)
			if err != nil {
				return nil, ErrNotInteger
			}
			n = v
		}
		// Overflow detection matching C's __builtin_add_overflow behavior
		if (delta > 0 && n > math.MaxInt64-delta) ||
			(delta < 0 && n < math.MinInt64-delta) {
			return nil, ErrOverflow
		}
		out = n + delta
		return strconv.AppendInt(nil, out, 10), nil
	})
	return out, err
}

// IncrementUnsigned adds (or with decr, subtracts) delta to the unsigned
// 64-bit decimal integer stored at key. A missing key counts as 0.
func (c *Cache) IncrementUnsigned(key []byte, delta uint64, decr bool) (uint64, error) {
	var out uint64
	err := c.Update(key, func(cur []byte, found bool) ([]byte, error) {
		var n uint64
		if found {
			v, err := strconv.ParseUint(string(cur), 10, 64)
			if err != nil {
				return nil, ErrNotInteger
			}
			n = v
		}
		if decr {
			if delta > n {
				return nil, ErrOverflow
			}
			out = n - delta
		} else {
			if n > math.MaxUint64-delta {
				return nil, ErrOverflow
			}
			out = n + delta
		}
		return strconv.AppendUint(nil, out, 10), nil
	})
	return out, err
}

// Update atomically replaces the value at key with the result of fn, which is
// called with the current value (found is false for a missing or expired key)
// while the shard lock is held. Flags and TTL of a live entry are preserved.
// If fn returns an error the entry is left untouched and the error returned.
func (c *Cache) Update(key []byte, fn func(cur []byte, found bool) ([]byte, error)) error {
	shard := c.getShard(key)
	storeKey, origLen := c.compressKey(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	atomic.AddUint64(&shard.numOps, 1)
	now := time.Now().UnixNano()

	existing, idx := shard.m.getWithIndex(storeKey, origLen > 0)
	if existing != nil && !existing.IsExpired() {
		val, err := fn(existing.value, true)
		if err != nil {
			return err
		}
		newEntry := &Entry{
			key:        existing.key,
			origKeyLen: existing.origKeyLen,
			value:      val,
			expireAt:   existing.expireAt,
			accessedAt: now,
			flags:      existing.flags,
			cas:        c.nextCAS(shard, 0),
		}
		if !c.makeRoom(shard, newEntry.Size()-existing.Size(), hashKey(storeKey)) {
			return ErrOutOfMemory
		}
		// makeRoom may have moved entries; find the bucket again.
		if _, idx = shard.m.getWithIndex(storeKey, origLen > 0); idx < 0 {
			return ErrOutOfMemory
		}
		// Replace entry pointer in bucket
		shard.m.buckets[idx].entry = newEntry
		shard.addMemUsed(newEntry.Size() - existing.Size())
		c.totalStored.Add(1)
		return nil
	}

	val, err := fn(nil, false)
	if err != nil {
		return err
	}
	entry := &Entry{
		key:        storeKey,
		origKeyLen: origLen,
		value:      val,
		accessedAt: now,
		cas:        c.nextCAS(shard, 0),
	}
	if !c.makeRoom(shard, c.growth(shard, entry), hashKey(storeKey)) {
		return ErrOutOfMemory
	}
	if old := shard.m.insert(entry); old != nil {
		shard.addMemUsed(-old.Size())
	}
	shard.addMemUsed(entry.Size())
	c.totalStored.Add(1)
	return nil
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
	toDelete := make([]*Entry, 0)
	shard.m.iter(func(e *Entry) bool {
		if e.IsExpired() {
			toDelete = append(toDelete, e)
		}
		return true
	})
	for _, e := range toDelete {
		if entry := shard.m.deleteEntry(e); entry != nil {
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
			entry := shard.m.deleteEntry(bucket.entry)
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

// Scan returns up to count live keys accepted by match, resuming from cursor,
// along with the cursor for the next call; a returned cursor of 0 means the
// scan is complete. The cursor packs the shard index into the upper 32 bits
// and the bucket position into the lower 32, as pogocache does. Like Redis
// SCAN, keys inserted or moved during a scan may be missed or repeated.
func (c *Cache) Scan(cursor uint64, count int, match func(key []byte) bool) ([][]byte, uint64) {
	var keys [][]byte
	pos := int(cursor & 0xFFFFFFFF)
	for idx := int(cursor >> 32); idx < c.numShards; idx++ {
		shard := c.shards[idx]
		shard.mu.RLock()
		buckets := shard.m.buckets
		for i := pos; i < len(buckets); i++ {
			e := buckets[i].entry
			if e == nil || e.IsExpired() {
				continue
			}
			key := e.Key()
			if match != nil && !match(key) {
				continue
			}
			keys = append(keys, key)
			if len(keys) == count {
				shard.mu.RUnlock()
				return keys, uint64(idx)<<32 | uint64(i+1)
			}
		}
		shard.mu.RUnlock()
		pos = 0
	}
	return keys, 0
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

// growth returns how much storing entry would add to shard's memory: its size,
// less the size of an entry it replaces. Only no-evict mode needs the exact
// figure; with eviction the entry's full size is used, as before.
func (c *Cache) growth(shard *Shard, entry *Entry) int64 {
	if c.noEvict && shard.maxMemory > 0 {
		if old := shard.m.get(entry.key, entry.origKeyLen > 0); old != nil {
			return entry.Size() - old.Size()
		}
	}
	return entry.Size()
}

// makeRoom makes space for requiredSpace more bytes in shard. With eviction it
// evicts entries as needed and always succeeds; with NoEvict it evicts
// nothing and reports false when the write would exceed the shard's limit.
func (c *Cache) makeRoom(shard *Shard, requiredSpace int64, skipHash uint64) bool {
	if shard.maxMemory <= 0 || requiredSpace <= 0 {
		return true
	}
	if c.noEvict {
		return shard.MemUsed()+requiredSpace <= shard.maxMemory
	}
	c.evictIfNeeded(shard, requiredSpace, skipHash)
	return true
}

func (c *Cache) evictIfNeeded(shard *Shard, requiredSpace int64, skipHash uint64) {
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
		deleted := shard.m.deleteEntry(toEvict)
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
		c.metrics.RecordStore(context.Background(), result, time.Since(start))
	}
}

func (c *Cache) recordLoad(start time.Time, hit bool) {
	if c.metrics != nil {
		c.metrics.RecordLoad(context.Background(), hit, time.Since(start))
	}
}

func (c *Cache) recordDelete(start time.Time, found bool) {
	if c.metrics != nil {
		c.metrics.RecordDelete(context.Background(), found, time.Since(start))
	}
}

func (c *Cache) recordEvictions(count int64) {
	if c.metrics != nil && count > 0 {
		c.metrics.RecordEviction(context.Background(), count)
	}
}

func (c *Cache) recordSweep(start time.Time, expired int) {
	if c.metrics != nil {
		c.metrics.RecordSweep(context.Background(), int64(expired), time.Since(start))
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

// lookupKey returns the key form used for map lookups and whether it is
// sixpack encoded.
func (c *Cache) lookupKey(key []byte) ([]byte, bool) {
	if c.noSixpack {
		return key, false
	}
	packed := Sixpack(key)
	if packed == nil {
		return key, false
	}
	return packed, true
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
