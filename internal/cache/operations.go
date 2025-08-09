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
	
	entry := &Entry{
		key:   key,
		value: value,
		cas:   existing.CAS(),
	}
	
	if opts != nil {
		if opts.TTL > 0 {
			entry.expireAt = time.Now().Add(opts.TTL).UnixNano()
		}
		entry.flags = opts.Flags
	}
	
	c.evictIfNeeded(shard, entry.Size()-existing.Size())
	
	existing.value = value
	existing.expireAt = entry.expireAt
	existing.flags = entry.flags
	existing.IncrementCAS()
	
	shard.addMemUsed(entry.Size() - existing.Size())
	
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
	for shard.MemUsed()+requiredSpace > shard.maxMemory && shard.m.numItems > 0 {
		entries := shard.m.randomEntries(2)
		if len(entries) == 0 {
			break
		}
		
		var toEvict *Entry
		if len(entries) == 1 {
			toEvict = entries[0]
		} else {
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
		
		if entry := shard.m.delete(toEvict.key, hashKey(toEvict.key)); entry != nil {
			shard.addMemUsed(-entry.Size())
			atomic.AddUint64(&shard.numEvicted, 1)
		}
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