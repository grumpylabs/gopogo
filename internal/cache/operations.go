package cache

import (
	"math/rand"
	"sync/atomic"
	"time"
)

type StoreOptions struct {
	TTL   time.Duration
	Flags uint32
	CAS   uint64
}

func (c *Cache) Store(key, value []byte, opts *StoreOptions) error {
	shard := c.getShard(key)
	
	entry := &Entry{
		key:   key,
		value: value,
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
	
	c.evictIfNeeded(shard, entry.Size())
	
	oldEntry := shard.m.insert(entry)
	
	if oldEntry != nil {
		shard.addMemUsed(-oldEntry.Size())
	}
	shard.addMemUsed(entry.Size())
	
	return nil
}

func (c *Cache) Load(key []byte) (*Entry, bool) {
	shard := c.getShard(key)
	
	shard.mu.RLock()
	entry := shard.m.get(key)
	shard.mu.RUnlock()
	
	atomic.AddUint64(&shard.numOps, 1)
	
	if entry == nil {
		atomic.AddUint64(&shard.numMisses, 1)
		return nil, false
	}
	
	// Check if entry was evicted
	if entry.IsEvicted() {
		c.Delete(key)
		atomic.AddUint64(&shard.numMisses, 1)
		return nil, false
	}
	
	if entry.IsExpired() {
		c.Delete(key)
		atomic.AddUint64(&shard.numExpired, 1)
		atomic.AddUint64(&shard.numMisses, 1)
		return nil, false
	}
	
	atomic.AddUint64(&shard.numHits, 1)
	return entry, true
}

func (c *Cache) Delete(key []byte) bool {
	shard := c.getShard(key)
	
	shard.mu.Lock()
	defer shard.mu.Unlock()
	
	atomic.AddUint64(&shard.numOps, 1)
	
	entry := shard.m.delete(key, hashKey(key))
	if entry == nil {
		return false
	}
	
	shard.addMemUsed(-entry.Size())
	return true
}

func (c *Cache) CompareAndSwap(key, value []byte, cas uint64, opts *StoreOptions) (bool, error) {
	shard := c.getShard(key)
	
	shard.mu.Lock()
	defer shard.mu.Unlock()
	
	atomic.AddUint64(&shard.numOps, 1)
	
	existing := shard.m.get(key)
	if existing == nil {
		return false, nil
	}
	
	if existing.CAS() != cas {
		return false, nil
	}
	
	// Calculate new expiration and flags
	var newExpireAt int64
	var newFlags uint32
	if opts != nil {
		if opts.TTL > 0 {
			newExpireAt = time.Now().Add(opts.TTL).UnixNano()
		}
		newFlags = opts.Flags
	}
	
	// Calculate size difference with new value
	sizeDelta := int64(len(value) - len(existing.value))
	
	c.evictIfNeeded(shard, sizeDelta)
	
	// Update the existing entry
	existing.value = value
	existing.expireAt = newExpireAt
	existing.flags = newFlags
	existing.IncrementCAS()
	
	shard.addMemUsed(sizeDelta)
	
	return true, nil
}

func (c *Cache) Increment(key []byte, delta int64) (int64, error) {
	shard := c.getShard(key)
	
	shard.mu.Lock()
	defer shard.mu.Unlock()
	
	atomic.AddUint64(&shard.numOps, 1)
	
	entry := shard.m.get(key)
	if entry == nil {
		val := delta
		entry = &Entry{
			key:   key,
			value: int64ToBytes(val),
		}
		
		c.evictIfNeeded(shard, entry.Size())
		shard.m.insert(entry)
		shard.addMemUsed(entry.Size())
		
		return val, nil
	}
	
	currentVal := bytesToInt64(entry.value)
	newVal := currentVal + delta
	
	oldSize := entry.Size()
	entry.value = int64ToBytes(newVal)
	entry.IncrementCAS()
	newSize := entry.Size()
	
	shard.addMemUsed(newSize - oldSize)
	
	return newVal, nil
}

func (c *Cache) Sweep() int {
	expired := 0
	
	for _, shard := range c.shards {
		shard.mu.Lock()
		
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
			}
		}
		
		shard.mu.Unlock()
	}
	
	return expired
}

// SweepEvicted removes evicted entries to free memory
// Ensures no more than 10% of cache memory is used by evicted entries
func (c *Cache) SweepEvicted() int {
	evicted := 0
	
	for _, shard := range c.shards {
		shard.mu.Lock()
		
		// Calculate how much memory is used by evicted entries
		evictedMemory := int64(0)
		totalMemory := shard.MemUsed()
		toDelete := make([][]byte, 0)
		
		shard.m.iter(func(e *Entry) bool {
			if e.IsEvicted() {
				evictedMemory += e.Size()
				toDelete = append(toDelete, e.key)
			}
			return true
		})
		
		// If evicted entries use more than 10% of memory, clean them up
		if evictedMemory > totalMemory/10 {
			for _, key := range toDelete {
				if entry := shard.m.delete(key, hashKey(key)); entry != nil {
					shard.addMemUsed(-entry.Size())
					evicted++
				}
			}
		}
		
		shard.mu.Unlock()
	}
	
	return evicted
}

func (c *Cache) Iterate(fn func(*Entry) bool) {
	for _, shard := range c.shards {
		shard.mu.RLock()
		
		stop := false
		shard.m.iter(func(e *Entry) bool {
			if e.IsExpired() {
				return true
			}
			if !fn(e) {
				stop = true
				return false
			}
			return true
		})
		
		shard.mu.RUnlock()
		
		if stop {
			break
		}
	}
}

func (c *Cache) Clear() {
	for _, shard := range c.shards {
		shard.mu.Lock()
		shard.m = NewMap(16)
		atomic.StoreInt64(&shard.memUsed, 0)
		shard.mu.Unlock()
	}
}

func (c *Cache) evictIfNeeded(shard *Shard, requiredSpace int64) {
	// Don't evict if there's no memory limit
	if shard.maxMemory <= 0 {
		return
	}
	for shard.MemUsed()+requiredSpace > shard.maxMemory && shard.m.numItems > 0 {
		entries := shard.m.randomEntries(2)
		if len(entries) == 0 {
			break
		}
		
		var toEvict *Entry
		if len(entries) == 1 {
			toEvict = entries[0]
		} else {
			// Enhanced 2-random eviction: prefer expired entries first
			entry0Expired := entries[0].IsExpired()
			entry1Expired := entries[1].IsExpired()
			
			if entry0Expired && !entry1Expired {
				toEvict = entries[0]
			} else if !entry0Expired && entry1Expired {
				toEvict = entries[1]
			} else if entry0Expired && entry1Expired {
				// Both expired, pick the one expiring soonest
				if entries[0].ExpireAt() < entries[1].ExpireAt() {
					toEvict = entries[0]
				} else {
					toEvict = entries[1]
				}
			} else {
				// Neither expired, use original 2-random with TTL consideration
				if entries[0].ExpireAt() > 0 && entries[1].ExpireAt() > 0 {
					if entries[0].ExpireAt() < entries[1].ExpireAt() {
						toEvict = entries[0]
					} else {
						toEvict = entries[1]
					}
				} else if rand.Intn(2) == 0 {
					toEvict = entries[0]
				} else {
					toEvict = entries[1]
				}
			}
		}
		
		// Mark as evicted and reduce memory usage immediately
		toEvict.SetEvicted(true)
		shard.addMemUsed(-toEvict.Size())
		atomic.AddUint64(&shard.numEvicted, 1)
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