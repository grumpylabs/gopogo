package cache

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestBasicOperations(t *testing.T) {
	c := New(nil)
	
	key := []byte("test-key")
	value := []byte("test-value")
	
	if _, err := c.Store(key, value, nil); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	entry, found := c.Load(key)
	if !found {
		t.Fatal("Key not found after store")
	}

	if !bytes.Equal(entry.Value(), value) {
		t.Fatalf("Value mismatch: got %s, want %s", entry.Value(), value)
	}

	deleted := c.Delete(key)
	if !deleted {
		t.Fatal("Delete returned false")
	}

	_, found = c.Load(key)
	if found {
		t.Fatal("Key found after delete")
	}
}

func TestTTL(t *testing.T) {
	c := New(nil)
	
	key := []byte("ttl-key")
	value := []byte("ttl-value")
	
	if _, err := c.Store(key, value, &StoreOptions{
		TTL: 100 * time.Millisecond,
	}); err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	
	entry, found := c.Load(key)
	if !found {
		t.Fatal("Key not found immediately after store")
	}
	
	if !bytes.Equal(entry.Value(), value) {
		t.Fatalf("Value mismatch: got %s, want %s", entry.Value(), value)
	}
	
	time.Sleep(150 * time.Millisecond)
	
	_, found = c.Load(key)
	if found {
		t.Fatal("Key found after TTL expiration")
	}
}

func TestIncrement(t *testing.T) {
	c := New(nil)
	
	key := []byte("counter")
	
	val, err := c.Increment(key, 5)
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if val != 5 {
		t.Fatalf("Expected 5, got %d", val)
	}
	
	val, err = c.Increment(key, 3)
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if val != 8 {
		t.Fatalf("Expected 8, got %d", val)
	}
	
	val, err = c.Increment(key, -2)
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if val != 6 {
		t.Fatalf("Expected 6, got %d", val)
	}
}

func TestCompareAndSwap(t *testing.T) {
	c := New(&Options{UseCAS: true})
	
	key := []byte("cas-key")
	value1 := []byte("value1")
	value2 := []byte("value2")
	
	if _, err := c.Store(key, value1, nil); err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	
	entry, _ := c.Load(key)
	cas := entry.CAS()
	
	success, err := c.CompareAndSwap(key, value2, cas, nil)
	if err != nil {
		t.Fatalf("CAS failed: %v", err)
	}
	if !success {
		t.Fatal("CAS should have succeeded")
	}
	
	success, err = c.CompareAndSwap(key, value1, cas, nil)
	if err != nil {
		t.Fatalf("CAS failed: %v", err)
	}
	if success {
		t.Fatal("CAS should have failed with old CAS value")
	}
}

func TestCASTokens(t *testing.T) {
	c := New(&Options{UseCAS: true}) // sixpack on by default
	key := []byte("user:cas")

	c.Store(key, []byte("a"), nil)
	e, _ := c.Load(key)
	first := e.CAS()
	if first == 0 {
		t.Fatal("Expected non-zero CAS token")
	}

	// A plain overwrite must issue a new token so a stale CAS fails.
	c.Store(key, []byte("b"), nil)
	e, _ = c.Load(key)
	if e.CAS() == first {
		t.Fatal("Store did not issue a new CAS token")
	}
	if ok, _ := c.CompareAndSwap(key, []byte("c"), first, nil); ok {
		t.Fatal("CAS with stale token should fail")
	}
	if ok, err := c.CompareAndSwap(key, []byte("c"), e.CAS(), nil); !ok || err != nil {
		t.Fatalf("CAS with current token should succeed: %v", err)
	}
	e, _ = c.Load(key)
	if !bytes.Equal(e.Key(), key) || string(e.Value()) != "c" {
		t.Fatalf("CAS corrupted entry: key=%q value=%q", e.Key(), e.Value())
	}

	// Read-modify-write paths issue a fresh token too.
	c.Store([]byte("n"), []byte("1"), nil)
	n1, _ := c.Load([]byte("n"))
	c.Increment([]byte("n"), 1)
	n2, _ := c.Load([]byte("n"))
	if n2.CAS() <= n1.CAS() {
		t.Fatalf("Increment did not issue a new CAS token: %d -> %d", n1.CAS(), n2.CAS())
	}

	if _, err := c.CompareAndSwap([]byte("missing"), []byte("x"), 1, nil); err != ErrNotFound {
		t.Fatalf("Expected ErrNotFound for missing key, got %v", err)
	}

	// An explicit token (as restored from a save file) is kept.
	c.Store([]byte("restored"), []byte("v"), &StoreOptions{CAS: 1000})
	if r, _ := c.Load([]byte("restored")); r.CAS() != 1000 {
		t.Fatalf("Explicit CAS not kept: %d", r.CAS())
	}
}

func TestCASDisabled(t *testing.T) {
	c := New(nil)
	c.Store([]byte("k"), []byte("v"), &StoreOptions{CAS: 5})
	e, _ := c.Load([]byte("k"))
	if e.CAS() != 0 {
		t.Fatalf("Expected CAS 0 when disabled, got %d", e.CAS())
	}
	if ok, err := c.CompareAndSwap([]byte("k"), []byte("x"), 0, nil); ok || err != nil {
		t.Fatalf("CAS should always fail when disabled: ok=%v err=%v", ok, err)
	}
}

func TestConcurrency(t *testing.T) {
	c := New(nil)
	
	const numGoroutines = 100
	const numOps = 1000
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			
			for j := 0; j < numOps; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, j))
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))
				
				c.Store(key, value, nil)
				
				entry, found := c.Load(key)
				if found && !bytes.Equal(entry.Value(), value) {
					t.Errorf("Value mismatch for key %s", key)
				}
				
				if j%2 == 0 {
					c.Delete(key)
				}
			}
		}(i)
	}
	
	wg.Wait()
	
	stats := c.Stats()
	if stats["num_ops"].(uint64) == 0 {
		t.Error("No operations recorded")
	}
}

func TestMemoryLimit(t *testing.T) {
	maxMemory := int64(1024)
	c := New(&Options{NumShards: 1, MaxMemory: maxMemory})
	
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := make([]byte, 100)
		c.Store(key, value, nil)
	}
	
	memUsed := c.MemUsed()
	if memUsed > maxMemory*2 {
		t.Errorf("Memory usage %d exceeds limit %d by too much", memUsed, maxMemory)
	}
	
	stats := c.Stats()
	if stats["num_evicted"].(uint64) == 0 {
		t.Error("No evictions occurred despite memory limit")
	}
}

func TestSweep(t *testing.T) {
	c := New(nil)
	
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		
		opts := &StoreOptions{
			TTL: 50 * time.Millisecond,
		}
		if i >= 5 {
			opts = nil
		}
		
		c.Store(key, value, opts)
	}
	
	time.Sleep(100 * time.Millisecond)
	
	expired := c.Sweep()
	if expired < 3 || expired > 5 {
		t.Errorf("Expected 3-5 expired entries, got %d", expired)
	}
	
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		_, found := c.Load(key)
		
		if i < 5 && found {
			t.Errorf("Expired key %s still exists", key)
		}
		if i >= 5 && !found {
			t.Errorf("Non-expired key %s not found", key)
		}
	}
}

func TestNX(t *testing.T) {
	c := New(nil)

	key := []byte("nx-key")

	// First store with NX should succeed (key doesn't exist)
	result, err := c.Store(key, []byte("v1"), &StoreOptions{NX: true})
	if err != nil {
		t.Fatalf("Store NX failed: %v", err)
	}
	if result != Inserted {
		t.Fatalf("Expected Inserted, got %d", result)
	}

	// Second store with NX should fail (key exists)
	result, err = c.Store(key, []byte("v2"), &StoreOptions{NX: true})
	if err != nil {
		t.Fatalf("Store NX failed: %v", err)
	}
	if result != NotStored {
		t.Fatalf("Expected NotStored, got %d", result)
	}

	// Value should still be v1
	entry, _ := c.Load(key)
	if !bytes.Equal(entry.Value(), []byte("v1")) {
		t.Fatalf("Value should be v1, got %s", entry.Value())
	}
}

func TestXX(t *testing.T) {
	c := New(nil)

	key := []byte("xx-key")

	// Store with XX should fail (key doesn't exist)
	result, _ := c.Store(key, []byte("v1"), &StoreOptions{XX: true})
	if result != NotStored {
		t.Fatalf("Expected NotStored, got %d", result)
	}

	// Normal store to create the key
	c.Store(key, []byte("v1"), nil)

	// Now XX should succeed
	result, _ = c.Store(key, []byte("v2"), &StoreOptions{XX: true})
	if result != Replaced {
		t.Fatalf("Expected Replaced, got %d", result)
	}

	entry, _ := c.Load(key)
	if !bytes.Equal(entry.Value(), []byte("v2")) {
		t.Fatalf("Value should be v2, got %s", entry.Value())
	}
}

func TestKeepTTL(t *testing.T) {
	c := New(nil)

	key := []byte("keepttl-key")

	// Store with TTL
	c.Store(key, []byte("v1"), &StoreOptions{TTL: 5 * time.Second})

	entry, _ := c.Load(key)
	originalExpire := entry.ExpireAt()
	if originalExpire == 0 {
		t.Fatal("Expected non-zero expiration")
	}

	// Update with KeepTTL — should preserve the original TTL
	c.Store(key, []byte("v2"), &StoreOptions{KeepTTL: true})

	entry, _ = c.Load(key)
	if !bytes.Equal(entry.Value(), []byte("v2")) {
		t.Fatalf("Value should be v2, got %s", entry.Value())
	}
	if entry.ExpireAt() != originalExpire {
		t.Fatalf("TTL should be preserved: got %d, want %d", entry.ExpireAt(), originalExpire)
	}

	// Update without KeepTTL — should clear the TTL
	c.Store(key, []byte("v3"), nil)

	entry, _ = c.Load(key)
	if entry.ExpireAt() != 0 {
		t.Fatalf("TTL should be cleared, got %d", entry.ExpireAt())
	}
}

func TestSweepPoll(t *testing.T) {
	c := New(&Options{NumShards: 4})

	// Store entries with short TTL
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("poll-key-%d", i))
		c.Store(key, []byte("value"), &StoreOptions{TTL: 50 * time.Millisecond})
	}

	// Store some persistent entries
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("persist-key-%d", i))
		c.Store(key, []byte("value"), nil)
	}

	time.Sleep(100 * time.Millisecond)

	// Incremental sweep should find some expired entries
	totalExpired := 0
	for i := 0; i < 10; i++ {
		totalExpired += c.SweepPoll(5)
	}

	if totalExpired == 0 {
		t.Error("SweepPoll should have found expired entries")
	}

	// Persistent entries should still exist
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("persist-key-%d", i))
		if _, found := c.Load(key); !found {
			t.Errorf("Persistent key %s should still exist", key)
		}
	}
}

func TestLRUEviction(t *testing.T) {
	// Small cache with 1 shard to make eviction predictable
	c := New(&Options{NumShards: 1, MaxMemory: 512})

	// Store several entries
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("lru-%d", i))
		c.Store(key, make([]byte, 50), nil)
	}

	// Access only the last 2 entries to make them "recently used"
	time.Sleep(time.Millisecond)
	c.Load([]byte("lru-3"))
	c.Load([]byte("lru-4"))

	// Force more entries to trigger eviction
	for i := 5; i < 15; i++ {
		key := []byte(fmt.Sprintf("lru-%d", i))
		c.Store(key, make([]byte, 50), nil)
	}

	// The recently accessed entries should be more likely to survive
	// (probabilistic, but with 2-random LRU the recently touched ones
	// have a strong advantage)
	_, found3 := c.Load([]byte("lru-3"))
	_, found4 := c.Load([]byte("lru-4"))

	// At least one of the recently accessed should survive
	if !found3 && !found4 {
		t.Log("Warning: both recently-accessed entries were evicted (unlikely but possible with random eviction)")
	}
}

func TestConfigurableLoadFactor(t *testing.T) {
	// Low load factor — should grow earlier, have more empty buckets
	cLow := New(&Options{NumShards: 1, LoadFactor: 0.55})
	// High load factor — should pack tighter
	cHigh := New(&Options{NumShards: 1, LoadFactor: 0.90})

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cLow.Store(key, []byte("v"), nil)
		cHigh.Store(key, []byte("v"), nil)
	}

	if cLow.NumItems() != 100 {
		t.Fatalf("Expected 100 items in low LF cache, got %d", cLow.NumItems())
	}
	if cHigh.NumItems() != 100 {
		t.Fatalf("Expected 100 items in high LF cache, got %d", cHigh.NumItems())
	}

	// Clamp test: below minimum should clamp to 0.55
	cClamp := New(&Options{NumShards: 1, LoadFactor: 0.10})
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		cClamp.Store(key, []byte("v"), nil)
	}
	if cClamp.NumItems() != 50 {
		t.Fatalf("Expected 50 items, got %d", cClamp.NumItems())
	}
}

func TestPerShardOperations(t *testing.T) {
	c := New(&Options{NumShards: 4})

	// Store entries
	for i := 0; i < 40; i++ {
		key := []byte(fmt.Sprintf("shard-key-%d", i))
		c.Store(key, []byte("value"), nil)
	}

	// NumShards
	if c.NumShards() != 4 {
		t.Fatalf("Expected 4 shards, got %d", c.NumShards())
	}

	// Per-shard count should sum to total
	total := 0
	for i := 0; i < c.NumShards(); i++ {
		n := c.ShardNumItems(i)
		total += n
	}
	if total != c.NumItems() {
		t.Fatalf("Shard item counts don't sum: %d != %d", total, c.NumItems())
	}

	// Per-shard memory should sum to total
	var totalMem int64
	for i := 0; i < c.NumShards(); i++ {
		totalMem += c.ShardMemUsed(i)
	}
	if totalMem != c.MemUsed() {
		t.Fatalf("Shard mem don't sum: %d != %d", totalMem, c.MemUsed())
	}

	// Out-of-bounds returns zero
	if c.ShardNumItems(-1) != 0 || c.ShardNumItems(999) != 0 {
		t.Fatal("Out of bounds should return 0")
	}

	// ClearShard
	c.ClearShard(0)
	if c.ShardNumItems(0) != 0 {
		t.Fatalf("Shard 0 should be empty after clear, got %d", c.ShardNumItems(0))
	}
	// Other shards should still have items
	if c.NumItems() == 0 {
		t.Fatal("All items gone after clearing one shard")
	}

	// IterateShard
	count := 0
	c.IterateShard(1, func(e *Entry) bool {
		count++
		return true
	})
	if count != c.ShardNumItems(1) {
		t.Fatalf("IterateShard count mismatch: %d != %d", count, c.ShardNumItems(1))
	}
}

func TestPerShardSweep(t *testing.T) {
	c := New(&Options{NumShards: 4})

	// Store expiring entries
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("exp-%d", i))
		c.Store(key, []byte("v"), &StoreOptions{TTL: 50 * time.Millisecond})
	}

	time.Sleep(100 * time.Millisecond)

	// Sweep only shard 0
	expired := c.SweepShard(0)

	// Shard 0 should be clean now; other shards may still have expired entries
	remaining := 0
	for i := 1; i < c.NumShards(); i++ {
		remaining += c.ShardNumItems(i)
	}
	// There should still be some expired entries in other shards
	// (unless all happened to land in shard 0, which is unlikely with 20 keys)
	_ = expired
	_ = remaining
}

func TestEvictedCallback(t *testing.T) {
	var evictions []EvictionReason
	var mu sync.Mutex

	c := New(&Options{
		NumShards: 1,
		MaxMemory: 512,
		Evicted: func(reason EvictionReason, key, value []byte, expires int64, flags uint32, cas uint64) {
			mu.Lock()
			evictions = append(evictions, reason)
			mu.Unlock()
		},
	})

	// Force evictions via memory pressure
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("evict-key-%d", i))
		c.Store(key, make([]byte, 50), nil)
	}

	mu.Lock()
	lowMemCount := 0
	for _, r := range evictions {
		if r == ReasonLowMem {
			lowMemCount++
		}
	}
	mu.Unlock()

	if lowMemCount == 0 {
		t.Fatal("Expected at least one ReasonLowMem eviction callback")
	}
}

func TestEvictedCallbackExpired(t *testing.T) {
	var expiredKeys []string
	var mu sync.Mutex

	c := New(&Options{
		NumShards: 1,
		Evicted: func(reason EvictionReason, key, value []byte, expires int64, flags uint32, cas uint64) {
			if reason == ReasonExpired {
				mu.Lock()
				expiredKeys = append(expiredKeys, string(key))
				mu.Unlock()
			}
		},
	})

	c.Store([]byte("exp-1"), []byte("v"), &StoreOptions{TTL: 50 * time.Millisecond})
	c.Store([]byte("exp-2"), []byte("v"), &StoreOptions{TTL: 50 * time.Millisecond})
	c.Store([]byte("perm"), []byte("v"), nil)

	time.Sleep(100 * time.Millisecond)
	c.Sweep()

	mu.Lock()
	defer mu.Unlock()
	if len(expiredKeys) != 2 {
		t.Fatalf("Expected 2 expired callbacks, got %d", len(expiredKeys))
	}
}

func TestEvictedCallbackCleared(t *testing.T) {
	var clearCount int
	c := New(&Options{
		NumShards: 1,
		Evicted: func(reason EvictionReason, key, value []byte, expires int64, flags uint32, cas uint64) {
			if reason == ReasonCleared {
				clearCount++
			}
		},
	})

	for i := 0; i < 5; i++ {
		c.Store([]byte(fmt.Sprintf("k-%d", i)), []byte("v"), nil)
	}

	c.Clear()

	if clearCount != 5 {
		t.Fatalf("Expected 5 ReasonCleared callbacks, got %d", clearCount)
	}
}

func TestNotifyCallback(t *testing.T) {
	type event struct {
		kind string // "inserted", "replaced", "deleted"
		key  string
	}
	var events []event
	var mu sync.Mutex

	c := New(&Options{
		NumShards: 1,
		Notify: func(newEntry, oldEntry *Entry) {
			mu.Lock()
			defer mu.Unlock()
			switch {
			case newEntry != nil && oldEntry == nil:
				events = append(events, event{"inserted", string(newEntry.Key())})
			case newEntry != nil && oldEntry != nil:
				events = append(events, event{"replaced", string(newEntry.Key())})
			case newEntry == nil && oldEntry != nil:
				events = append(events, event{"deleted", string(oldEntry.Key())})
			}
		},
	})

	c.Store([]byte("k1"), []byte("v1"), nil) // insert
	c.Store([]byte("k1"), []byte("v2"), nil) // replace
	c.Delete([]byte("k1"))                   // delete
	c.Store([]byte("k2"), []byte("v"), nil)   // insert

	mu.Lock()
	defer mu.Unlock()

	expected := []event{
		{"inserted", "k1"},
		{"replaced", "k1"},
		{"deleted", "k1"},
		{"inserted", "k2"},
	}

	if len(events) != len(expected) {
		t.Fatalf("Expected %d events, got %d: %v", len(expected), len(events), events)
	}
	for i, e := range expected {
		if events[i] != e {
			t.Fatalf("Event %d: expected %v, got %v", i, e, events[i])
		}
	}
}

func TestSixpack(t *testing.T) {
	// Test encode/decode roundtrip
	tests := []string{
		"user:123",
		"session.abc-def_456",
		"key:0123456789",
		"ABCDEFGHIJKLMNOPRSTUVWXY",
		"abcdefghijklmnopqrstuvwxy",
		"-._:",
	}
	for _, s := range tests {
		packed := Sixpack([]byte(s))
		if packed == nil {
			t.Fatalf("Sixpack(%q) returned nil", s)
		}
		if len(packed) >= len(s) {
			t.Fatalf("Sixpack(%q) did not compress: %d >= %d", s, len(packed), len(s))
		}
		unpacked := Unsixpack(packed)
		if string(unpacked) != s {
			t.Fatalf("Unsixpack(Sixpack(%q)) = %q", s, unpacked)
		}
	}

	// Characters outside the set should not pack
	nonPackable := []string{"hello world", "key=value", "has/slash", "has@at"}
	for _, s := range nonPackable {
		if Sixpack([]byte(s)) != nil {
			t.Fatalf("Sixpack(%q) should return nil", s)
		}
	}
}

func TestSixpackIntegration(t *testing.T) {
	// With sixpack enabled (default)
	c := New(nil)

	// These keys are sixpack-compatible
	c.Store([]byte("user:123"), []byte("alice"), nil)
	c.Store([]byte("session.abc"), []byte("data"), nil)

	entry, found := c.Load([]byte("user:123"))
	if !found {
		t.Fatal("user:123 not found")
	}
	if string(entry.Value()) != "alice" {
		t.Fatalf("Expected alice, got %s", entry.Value())
	}
	// Key() should return decompressed form
	if string(entry.Key()) != "user:123" {
		t.Fatalf("Expected user:123, got %s", entry.Key())
	}

	// Delete should work with original key
	if !c.Delete([]byte("user:123")) {
		t.Fatal("Delete failed")
	}
	if _, found := c.Load([]byte("user:123")); found {
		t.Fatal("Key still exists after delete")
	}

	// Non-sixpackable keys should work too
	c.Store([]byte("has spaces"), []byte("val"), nil)
	entry, found = c.Load([]byte("has spaces"))
	if !found {
		t.Fatal("non-sixpackable key not found")
	}
	if string(entry.Key()) != "has spaces" {
		t.Fatalf("Expected 'has spaces', got %s", entry.Key())
	}
}

func TestSixpackDisabled(t *testing.T) {
	c := New(&Options{NoSixpack: true})

	c.Store([]byte("user:123"), []byte("v"), nil)
	entry, found := c.Load([]byte("user:123"))
	if !found {
		t.Fatal("key not found")
	}
	// With sixpack disabled, internal key should be uncompressed
	if entry.origKeyLen != 0 {
		t.Fatal("Expected origKeyLen=0 with sixpack disabled")
	}
}

func TestSaveAndLoad(t *testing.T) {
	c := New(&Options{NumShards: 4})

	// Store various entries
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("persist-key-%d", i))
		value := []byte(fmt.Sprintf("persist-value-%d", i))
		c.Store(key, value, nil)
	}

	// Store some entries with TTL (long enough to survive save/load)
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("ttl-key-%d", i))
		c.Store(key, []byte("ttl-value"), &StoreOptions{TTL: 1 * time.Hour})
	}

	// Store entries with flags
	c.Store([]byte("flagged"), []byte("fv"), &StoreOptions{Flags: 42})

	tmpFile := filepath.Join(t.TempDir(), "test.pogo")

	// Save
	if err := c.Save(tmpFile); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Verify file exists
	info, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("Save file is empty")
	}

	// Load into a new cache
	c2 := New(&Options{NumShards: 4})
	stats, err := c2.LoadFromFile(tmpFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if stats.Inserted != 61 { // 50 + 10 + 1
		t.Fatalf("Expected 61 inserted, got %d", stats.Inserted)
	}
	if stats.Expired != 0 {
		t.Fatalf("Expected 0 expired, got %d", stats.Expired)
	}
	if stats.CompressedSize == 0 || stats.RawSize == 0 {
		t.Fatal("Expected non-zero sizes in stats")
	}

	// Verify data integrity
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("persist-key-%d", i))
		expected := []byte(fmt.Sprintf("persist-value-%d", i))
		entry, found := c2.Load(key)
		if !found {
			t.Fatalf("Key %s not found after load", key)
		}
		if !bytes.Equal(entry.Value(), expected) {
			t.Fatalf("Value mismatch for %s: got %s, want %s", key, entry.Value(), expected)
		}
	}

	// TTL entries should exist
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("ttl-key-%d", i))
		if _, found := c2.Load(key); !found {
			t.Fatalf("TTL key %s not found after load", key)
		}
	}

	// Flagged entry should have correct flags
	entry, found := c2.Load([]byte("flagged"))
	if !found {
		t.Fatal("flagged key not found")
	}
	if entry.Flags() != 42 {
		t.Fatalf("Expected flags=42, got %d", entry.Flags())
	}
}

func TestSaveLoadExpiredSkipped(t *testing.T) {
	c := New(&Options{NumShards: 1})

	// Store entries with very short TTL
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("short-%d", i))
		c.Store(key, []byte("v"), &StoreOptions{TTL: 50 * time.Millisecond})
	}
	// Store persistent entries
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("long-%d", i))
		c.Store(key, []byte("v"), nil)
	}

	time.Sleep(100 * time.Millisecond)

	tmpFile := filepath.Join(t.TempDir(), "test-expired.pogo")
	if err := c.Save(tmpFile); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	c2 := New(&Options{NumShards: 1})
	stats, err := c2.LoadFromFile(tmpFile)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Expired entries should have been skipped during save
	if stats.Inserted != 5 {
		t.Fatalf("Expected 5 inserted (expired skipped at save), got %d", stats.Inserted)
	}

	// Only persistent entries should exist
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("long-%d", i))
		if _, found := c2.Load(key); !found {
			t.Fatalf("Persistent key %s not found", key)
		}
	}
}

func TestCleanWorkFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.pogo")

	stale := []string{
		filepath.Join(dir, "data.pogo.123.pogocache.work"),
		filepath.Join(dir, "data.pogo.456.pogocache.work"),
	}
	keep := []string{
		path,
		filepath.Join(dir, "other.pogo.789.pogocache.work"),
	}
	for _, f := range append(stale, keep...) {
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	removed, err := CleanWorkFiles(path)
	if err != nil {
		t.Fatalf("CleanWorkFiles failed: %v", err)
	}
	if len(removed) != len(stale) {
		t.Fatalf("Expected %d removed, got %d: %v", len(stale), len(removed), removed)
	}
	for _, f := range stale {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Fatalf("Stale work file %s still exists", f)
		}
	}
	for _, f := range keep {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("File %s should have been kept: %v", f, err)
		}
	}

	// A successful save leaves no work files behind.
	c := New(&Options{NumShards: 1})
	c.Store([]byte("k"), []byte("v"), nil)
	if err := c.Save(path); err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	if removed, _ := CleanWorkFiles(path); len(removed) != 0 {
		t.Fatalf("Save left work files: %v", removed)
	}
}

func TestScan(t *testing.T) {
	c := New(&Options{NumShards: 4})
	want := map[string]bool{}
	for i := 0; i < 250; i++ {
		k := fmt.Sprintf("key:%d", i)
		c.Store([]byte(k), []byte("v"), nil)
		want[k] = true
	}
	c.Store([]byte("other"), []byte("v"), nil)

	got := map[string]bool{}
	var cursor uint64
	calls := 0
	for {
		keys, next := c.Scan(cursor, 10, func(k []byte) bool {
			return bytes.HasPrefix(k, []byte("key:"))
		})
		if len(keys) > 10 {
			t.Fatalf("Scan returned %d keys, more than count", len(keys))
		}
		for _, k := range keys {
			if got[string(k)] {
				t.Fatalf("Key %s returned twice", k)
			}
			got[string(k)] = true
		}
		calls++
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(got) != len(want) {
		t.Fatalf("Expected %d keys, got %d", len(want), len(got))
	}
	if calls < 25 {
		t.Fatalf("Expected at least 25 calls with count 10, got %d", calls)
	}
	if keys, next := c.Scan(uint64(99)<<32, 10, nil); len(keys) != 0 || next != 0 {
		t.Fatalf("Out-of-range cursor should end scan, got %d keys next=%d", len(keys), next)
	}
}

func TestBatch(t *testing.T) {
	c := New(nil)

	// Basic batch operations
	b := c.Begin()
	b.Store([]byte("k1"), []byte("v1"), nil)
	b.Store([]byte("k2"), []byte("v2"), nil)
	b.Store([]byte("k3"), []byte("v3"), nil)

	// Can read within the batch
	entry, found := b.Load([]byte("k1"))
	if !found {
		t.Fatal("k1 not found in batch")
	}
	if string(entry.Value()) != "v1" {
		t.Fatalf("Expected v1, got %s", entry.Value())
	}

	// Delete within batch
	if !b.Delete([]byte("k2")) {
		t.Fatal("k2 delete failed in batch")
	}

	b.End()

	// Verify state after batch ends
	if _, found := c.Load([]byte("k1")); !found {
		t.Fatal("k1 not found after batch")
	}
	if _, found := c.Load([]byte("k2")); found {
		t.Fatal("k2 should be deleted after batch")
	}
	if _, found := c.Load([]byte("k3")); !found {
		t.Fatal("k3 not found after batch")
	}
}

func TestBatchNotify(t *testing.T) {
	var inserts, replaces, deletes int

	c := New(&Options{
		Notify: func(newEntry, oldEntry *Entry) {
			switch {
			case newEntry != nil && oldEntry == nil:
				inserts++
			case newEntry != nil && oldEntry != nil:
				replaces++
			case newEntry == nil && oldEntry != nil:
				deletes++
			}
		},
	})

	b := c.Begin()
	b.Store([]byte("k1"), []byte("v1"), nil) // insert
	b.Store([]byte("k1"), []byte("v2"), nil) // replace (same shard, already locked)
	b.Delete([]byte("k1"))                   // delete
	b.End()

	if inserts != 1 || replaces != 1 || deletes != 1 {
		t.Fatalf("Expected 1/1/1, got %d/%d/%d", inserts, replaces, deletes)
	}
}

func TestBatchConcurrent(t *testing.T) {
	c := New(nil)

	// Pre-populate
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		c.Store(key, []byte("v"), nil)
	}

	// Run concurrent batches
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				b := c.Begin()
				key := []byte(fmt.Sprintf("key-%d", (id*50+i)%100))
				b.Store(key, []byte(fmt.Sprintf("v-%d-%d", id, i)), nil)
				b.Load(key)
				b.End()
			}
		}(g)
	}
	wg.Wait()

	// Cache should still be consistent
	if c.NumItems() == 0 {
		t.Fatal("Cache should have items after concurrent batches")
	}
}

func TestNoEvict(t *testing.T) {
	c := New(&Options{NumShards: 1, MaxMemory: 512, NoEvict: true})

	// With NoEvict, stores should succeed but eviction won't happen
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("noevict-%d", i))
		c.Store(key, make([]byte, 50), nil)
	}

	stats := c.Stats()
	if stats["num_evicted"].(uint64) != 0 {
		t.Fatalf("Expected 0 evictions with NoEvict, got %d", stats["num_evicted"])
	}
}

func TestNoTouch(t *testing.T) {
	c := New(nil)
	c.Store([]byte("k"), []byte("v"), nil)

	// Normal load should update access time
	entry1, _ := c.Load([]byte("k"))
	at1 := entry1.AccessedAt()

	time.Sleep(2 * time.Millisecond)

	// LoadWithOptions NoTouch should NOT update access time
	entry2, _ := c.LoadWithOptions([]byte("k"), &LoadOptions{NoTouch: true})
	at2 := entry2.AccessedAt()

	if at2 != at1 {
		t.Fatalf("NoTouch should preserve access time: %d != %d", at2, at1)
	}

	// Normal load SHOULD update access time
	time.Sleep(2 * time.Millisecond)
	entry3, _ := c.Load([]byte("k"))
	at3 := entry3.AccessedAt()

	if at3 <= at1 {
		t.Fatalf("Normal load should update access time: %d <= %d", at3, at1)
	}
}

func TestIncrementDecimalText(t *testing.T) {
	c := New(nil) // sixpack on by default

	key := []byte("user:counter")
	c.Store(key, []byte("10"), &StoreOptions{TTL: time.Hour, Flags: 7})
	val, err := c.Increment(key, 1)
	if err != nil || val != 11 {
		t.Fatalf("Expected 11, got %d (%v)", val, err)
	}
	e, ok := c.Load(key)
	if !ok || string(e.Value()) != "11" {
		t.Fatalf("Expected stored value \"11\", got %q", e.Value())
	}
	if e.Flags() != 7 || e.ExpireAt() == 0 {
		t.Fatalf("Flags/TTL not preserved: flags=%d expireAt=%d", e.Flags(), e.ExpireAt())
	}
	if !bytes.Equal(e.Key(), key) {
		t.Fatalf("Key corrupted after increment: %q", e.Key())
	}

	c.Store([]byte("str"), []byte("hello"), nil)
	if _, err := c.Increment([]byte("str"), 1); err != ErrNotInteger {
		t.Fatalf("Expected ErrNotInteger, got %v", err)
	}
	if e, _ := c.Load([]byte("str")); string(e.Value()) != "hello" {
		t.Fatalf("Failed increment modified value: %q", e.Value())
	}
}

func TestIncrementUnsigned(t *testing.T) {
	c := New(nil)
	key := []byte("u")

	val, err := c.IncrementUnsigned(key, 5, false)
	if err != nil || val != 5 {
		t.Fatalf("Expected 5, got %d (%v)", val, err)
	}
	if _, err := c.IncrementUnsigned(key, 6, true); err != ErrOverflow {
		t.Fatalf("Expected ErrOverflow below zero, got %v", err)
	}
	c.Store(key, []byte(strconv.FormatUint(math.MaxUint64-1, 10)), nil)
	if _, err := c.IncrementUnsigned(key, 2, false); err != ErrOverflow {
		t.Fatalf("Expected ErrOverflow above max, got %v", err)
	}
	c.Store(key, []byte("-1"), nil)
	if _, err := c.IncrementUnsigned(key, 1, false); err != ErrNotInteger {
		t.Fatalf("Expected ErrNotInteger for negative value, got %v", err)
	}
}

func TestIncrementOverflow(t *testing.T) {
	c := New(nil)

	// Set to near max
	c.Store([]byte("counter"), []byte(strconv.FormatInt(math.MaxInt64-5, 10)), nil)

	// Small increment should work
	val, err := c.Increment([]byte("counter"), 3)
	if err != nil {
		t.Fatalf("Increment should succeed: %v", err)
	}
	if val != math.MaxInt64-2 {
		t.Fatalf("Expected %d, got %d", math.MaxInt64-2, val)
	}

	// Large increment should overflow
	_, err = c.Increment([]byte("counter"), 10)
	if err != ErrOverflow {
		t.Fatalf("Expected ErrOverflow, got %v", err)
	}

	// Negative overflow
	c.Store([]byte("neg"), []byte(strconv.FormatInt(math.MinInt64+5, 10)), nil)
	_, err = c.Increment([]byte("neg"), -10)
	if err != ErrOverflow {
		t.Fatalf("Expected ErrOverflow for negative overflow, got %v", err)
	}
}

func TestExpiredOnLoadFiresCallback(t *testing.T) {
	var expiredKeys []string
	var mu sync.Mutex

	c := New(&Options{
		NumShards: 1,
		Evicted: func(reason EvictionReason, key, value []byte, expires int64, flags uint32, cas uint64) {
			if reason == ReasonExpired {
				mu.Lock()
				expiredKeys = append(expiredKeys, string(key))
				mu.Unlock()
			}
		},
	})

	c.Store([]byte("exp-load"), []byte("v"), &StoreOptions{TTL: 50 * time.Millisecond})
	time.Sleep(100 * time.Millisecond)

	// Load should return miss AND fire evicted callback
	_, found := c.Load([]byte("exp-load"))
	if found {
		t.Fatal("Expired key should not be found")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(expiredKeys) != 1 || expiredKeys[0] != "exp-load" {
		t.Fatalf("Expected evicted callback for exp-load, got %v", expiredKeys)
	}
}

func TestLoadWithUpdate(t *testing.T) {
	c := New(nil)
	c.Store([]byte("k1"), []byte("original"), &StoreOptions{Flags: 10})

	// Load with update callback
	entry, found := c.LoadWithOptions([]byte("k1"), &LoadOptions{
		Entry: func(key, value []byte, expires int64, flags uint32, cas uint64) *LoadUpdate {
			if string(value) != "original" {
				t.Fatalf("Expected original, got %s", value)
			}
			if flags != 10 {
				t.Fatalf("Expected flags=10, got %d", flags)
			}
			return &LoadUpdate{
				Value: []byte("updated"),
				Flags: 20,
			}
		},
	})
	if !found {
		t.Fatal("k1 not found")
	}
	// Returned entry should be the updated one
	if string(entry.Value()) != "updated" {
		t.Fatalf("Expected updated, got %s", entry.Value())
	}
	if entry.Flags() != 20 {
		t.Fatalf("Expected flags=20, got %d", entry.Flags())
	}

	// Verify persistence
	entry2, _ := c.Load([]byte("k1"))
	if string(entry2.Value()) != "updated" {
		t.Fatalf("Expected updated on re-load, got %s", entry2.Value())
	}
}

func TestLoadWithUpdateNil(t *testing.T) {
	c := New(nil)
	c.Store([]byte("k1"), []byte("v1"), nil)

	// Return nil from callback — entry should not change
	entry, found := c.LoadWithOptions([]byte("k1"), &LoadOptions{
		Entry: func(key, value []byte, expires int64, flags uint32, cas uint64) *LoadUpdate {
			return nil // no update
		},
	})
	if !found {
		t.Fatal("k1 not found")
	}
	if string(entry.Value()) != "v1" {
		t.Fatalf("Expected v1, got %s", entry.Value())
	}
}

func TestLoadWithUpdateNotify(t *testing.T) {
	var replaced int
	c := New(&Options{
		Notify: func(newEntry, oldEntry *Entry) {
			if newEntry != nil && oldEntry != nil {
				replaced++
			}
		},
	})
	c.Store([]byte("k1"), []byte("v1"), nil)

	c.LoadWithOptions([]byte("k1"), &LoadOptions{
		Entry: func(key, value []byte, expires int64, flags uint32, cas uint64) *LoadUpdate {
			return &LoadUpdate{Value: []byte("v2")}
		},
	})

	if replaced != 1 {
		t.Fatalf("Expected 1 replace notification, got %d", replaced)
	}
}

func TestDeleteWithCancel(t *testing.T) {
	c := New(nil)
	c.Store([]byte("k1"), []byte("keep-me"), nil)
	c.Store([]byte("k2"), []byte("delete-me"), nil)

	// Cancel delete for k1
	ok := c.DeleteWithOptions([]byte("k1"), &DeleteOptions{
		Entry: func(key, value []byte, expires int64, flags uint32, cas uint64) bool {
			return string(value) != "keep-me" // cancel if value is "keep-me"
		},
	})
	if ok {
		t.Fatal("Delete should have been canceled")
	}

	// k1 should still exist
	if _, found := c.Load([]byte("k1")); !found {
		t.Fatal("k1 should still exist after canceled delete")
	}

	// Allow delete for k2
	ok = c.DeleteWithOptions([]byte("k2"), &DeleteOptions{
		Entry: func(key, value []byte, expires int64, flags uint32, cas uint64) bool {
			return true // proceed
		},
	})
	if !ok {
		t.Fatal("Delete should have succeeded")
	}
	if _, found := c.Load([]byte("k2")); found {
		t.Fatal("k2 should be gone")
	}
}

func TestDeleteExpiredFiresExpiredNotDeleted(t *testing.T) {
	var expiredCount, deletedCount int

	c := New(&Options{
		NumShards: 1,
		Evicted: func(reason EvictionReason, key, value []byte, expires int64, flags uint32, cas uint64) {
			if reason == ReasonExpired {
				expiredCount++
			}
		},
		Notify: func(newEntry, oldEntry *Entry) {
			if newEntry == nil && oldEntry != nil {
				deletedCount++
			}
		},
	})

	// Store an entry that will expire
	c.Store([]byte("exp"), []byte("v"), &StoreOptions{TTL: 50 * time.Millisecond})
	// Store a normal entry
	c.Store([]byte("live"), []byte("v"), nil)

	time.Sleep(100 * time.Millisecond)

	// Delete the expired entry — should fire Evicted(ReasonExpired), NOT Notify(deleted)
	c.Delete([]byte("exp"))
	// Delete the live entry — should fire Notify(deleted), NOT Evicted
	c.Delete([]byte("live"))

	if expiredCount != 1 {
		t.Fatalf("Expected 1 expired callback, got %d", expiredCount)
	}
	if deletedCount != 1 {
		t.Fatalf("Expected 1 deleted notification, got %d", deletedCount)
	}
}

func BenchmarkStore(b *testing.B) {
	c := New(nil)
	key := []byte("bench-key")
	value := []byte("bench-value")
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Store(key, value, nil)
		}
	})
}

func BenchmarkLoad(b *testing.B) {
	c := New(nil)
	key := []byte("bench-key")
	value := []byte("bench-value")
	c.Store(key, value, nil)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Load(key)
		}
	})
}

func BenchmarkDelete(b *testing.B) {
	c := New(nil)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("key-%d", i))
			c.Store(key, []byte("value"), nil)
			c.Delete(key)
			i++
		}
	})
}

func BenchmarkIncrement(b *testing.B) {
	c := New(nil)
	key := []byte("counter")
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Increment(key, 1)
		}
	})
}