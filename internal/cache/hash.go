package cache

import (
	"github.com/cespare/xxhash/v2"
)

func hashKey(key []byte) uint64 {
	return xxhash.Sum64(key)
}

func (m *Map) resize(newSize int) {
	oldBuckets := m.buckets
	
	m.buckets = make([]Bucket, newSize)
	m.mask = uint64(newSize - 1)
	m.growAt = int(float64(newSize) * 0.75)
	m.shrinkAt = int(float64(newSize) * 0.10)
	m.numItems = 0
	
	for i := range oldBuckets {
		if oldBuckets[i].entry != nil {
			m.insertInternal(oldBuckets[i].entry, oldBuckets[i].hash)
		}
	}
}

func (m *Map) insertInternal(entry *Entry, hash uint64) {
	idx := hash & m.mask
	distance := uint16(0)
	
	for {
		if m.buckets[idx].entry == nil {
			m.buckets[idx] = Bucket{
				entry:    entry,
				hash:     hash,
				distance: distance,
			}
			m.numItems++
			return
		}
		
		if m.buckets[idx].distance < distance {
			m.buckets[idx], entry, hash, distance = Bucket{
				entry:    entry,
				hash:     hash,
				distance: distance,
			}, m.buckets[idx].entry, m.buckets[idx].hash, m.buckets[idx].distance
		}
		
		idx = (idx + 1) & m.mask
		distance++
	}
}

func (m *Map) lookup(key []byte, hash uint64) (*Entry, int) {
	idx := int(hash & m.mask)
	distance := uint16(0)
	
	for {
		if m.buckets[idx].entry == nil || m.buckets[idx].distance < distance {
			return nil, -1
		}
		
		if m.buckets[idx].hash == hash && 
		   len(m.buckets[idx].entry.key) == len(key) {
			match := true
			for i := range key {
				if key[i] != m.buckets[idx].entry.key[i] {
					match = false
					break
				}
			}
			if match {
				return m.buckets[idx].entry, idx
			}
		}
		
		idx = int((uint64(idx) + 1) & m.mask)
		distance++
	}
}

func (m *Map) delete(key []byte, hash uint64) *Entry {
	entry, idx := m.lookup(key, hash)
	if entry == nil {
		return nil
	}
	
	m.buckets[idx].entry = nil
	m.numItems--
	
	nextIdx := int((uint64(idx) + 1) & m.mask)
	for m.buckets[nextIdx].entry != nil && m.buckets[nextIdx].distance > 0 {
		m.buckets[idx] = m.buckets[nextIdx]
		m.buckets[idx].distance--
		m.buckets[nextIdx].entry = nil
		
		idx = nextIdx
		nextIdx = int((uint64(idx) + 1) & m.mask)
	}
	
	if m.numItems < m.shrinkAt && len(m.buckets) > 16 {
		m.resize(len(m.buckets) / 2)
	}
	
	return entry
}

func (m *Map) insert(entry *Entry) *Entry {
	hash := hashKey(entry.key)
	
	if existing, _ := m.lookup(entry.key, hash); existing != nil {
		oldEntry := *existing
		existing.value = entry.value
		existing.expireAt = entry.expireAt
		existing.flags = entry.flags
		existing.IncrementCAS()
		return &oldEntry
	}
	
	if m.numItems >= m.growAt {
		m.resize(len(m.buckets) * 2)
	}
	
	m.insertInternal(entry, hash)
	return nil
}

func (m *Map) get(key []byte) *Entry {
	hash := hashKey(key)
	entry, _ := m.lookup(key, hash)
	return entry
}

func (m *Map) randomEntries(n int) []*Entry {
	if n <= 0 || m.numItems == 0 {
		return nil
	}
	
	entries := make([]*Entry, 0, n)
	seen := 0
	
	for i := 0; i < len(m.buckets) && len(entries) < n; i++ {
		if m.buckets[i].entry != nil {
			if seen%((m.numItems/n)+1) == 0 {
				entries = append(entries, m.buckets[i].entry)
			}
			seen++
		}
	}
	
	return entries
}

func (m *Map) iter(fn func(*Entry) bool) {
	for i := range m.buckets {
		if m.buckets[i].entry != nil {
			if !fn(m.buckets[i].entry) {
				return
			}
		}
	}
}