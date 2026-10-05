package cache

import (
	"time"
)

// Batch provides a transactional interface that holds locks on shards
// for the duration of multiple operations. This matches the C implementation's
// pogocache_begin/pogocache_end pattern.
//
// Usage:
//
//	b := cache.Begin()
//	defer b.End()
//	b.Store(key1, val1, nil)
//	b.Store(key2, val2, nil)
//	entry, ok := b.Load(key3)
type Batch struct {
	cache  *Cache
	locked map[int]bool // set of shard indices we hold write locks on
}

// Begin creates a new batch transaction. The caller must call End() when done
// to release all held locks.
func (c *Cache) Begin() *Batch {
	return &Batch{
		cache:  c,
		locked: make(map[int]bool),
	}
}

// End releases all locks held by the batch. Must be called exactly once.
func (b *Batch) End() {
	for idx := range b.locked {
		b.cache.shards[idx].mu.Unlock()
	}
	b.locked = nil
}

// acquireShard ensures the shard for the given key is locked.
// If already locked by this batch, it's a no-op.
func (b *Batch) acquireShard(key []byte) (*Shard, int) {
	h := hashKey(key)
	idx := int((h >> 32) % uint64(b.cache.numShards))
	shard := b.cache.shards[idx]
	if !b.locked[idx] {
		shard.mu.Lock()
		b.locked[idx] = true
	}
	return shard, idx
}

// Store inserts or replaces an entry within the batch.
func (b *Batch) Store(key, value []byte, opts *StoreOptions) StoreResult {
	start := time.Now()
	shard, _ := b.acquireShard(key)
	now := start.UnixNano()
	c := b.cache

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

	if opts != nil && (opts.NX || opts.XX || opts.KeepTTL) {
		existing := shard.m.get(storeKey, origLen > 0)
		alive := existing != nil && !existing.IsExpired()
		if opts.NX && alive {
			c.recordStore(start, "not_stored")
			return NotStored
		}
		if opts.XX && !alive {
			c.recordStore(start, "not_stored")
			return NotStored
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
		return NoMemory
	}

	oldEntry := shard.m.insert(entry)
	if oldEntry != nil {
		shard.addMemUsed(-oldEntry.Size())
		shard.addMemUsed(entry.Size())
		c.fireNotifyReplaced(entry, oldEntry)
		c.recordStore(start, "replaced")
		return Replaced
	}
	shard.addMemUsed(entry.Size())
	c.fireNotifyInserted(entry)
	c.recordStore(start, "inserted")
	return Inserted
}

// Load retrieves an entry within the batch.
func (b *Batch) Load(key []byte) (*Entry, bool) {
	start := time.Now()
	shard, _ := b.acquireShard(key)
	c := b.cache
	lk, packed := c.lookupKey(key)

	entry := shard.m.get(lk, packed)
	if entry == nil {
		c.recordLoad(start, false)
		return nil, false
	}
	if entry.IsExpired() {
		c.recordLoad(start, false)
		return nil, false
	}
	entry.touch(time.Now().UnixNano())
	c.recordLoad(start, true)
	return entry, true
}

// Delete removes an entry within the batch.
func (b *Batch) Delete(key []byte) bool {
	start := time.Now()
	shard, _ := b.acquireShard(key)
	c := b.cache
	lk, packed := c.lookupKey(key)

	entry := shard.m.delete(lk, hashKey(lk), packed)
	if entry == nil {
		c.recordDelete(start, false)
		return false
	}
	shard.addMemUsed(-entry.Size())
	c.fireNotifyDeleted(entry)
	c.recordDelete(start, true)
	return true
}
