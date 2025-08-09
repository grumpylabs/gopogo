package cache

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBasicOperations(t *testing.T) {
	c := New(16, 0)
	
	key := []byte("test-key")
	value := []byte("test-value")
	
	err := c.Store(key, value, nil)
	if err != nil {
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
	c := New(16, 0)
	
	key := []byte("ttl-key")
	value := []byte("ttl-value")
	
	err := c.Store(key, value, &StoreOptions{
		TTL: 100 * time.Millisecond,
	})
	if err != nil {
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
	c := New(16, 0)
	
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
	c := New(16, 0)
	
	key := []byte("cas-key")
	value1 := []byte("value1")
	value2 := []byte("value2")
	
	err := c.Store(key, value1, nil)
	if err != nil {
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

func TestConcurrency(t *testing.T) {
	c := New(16, 0)
	
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
	c := New(1, maxMemory)
	
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
	c := New(16, 0)
	
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

func BenchmarkStore(b *testing.B) {
	c := New(16, 0)
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
	c := New(16, 0)
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
	c := New(16, 0)
	
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
	c := New(16, 0)
	key := []byte("counter")
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Increment(key, 1)
		}
	})
}