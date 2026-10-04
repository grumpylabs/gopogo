# Gopogo - High-Performance Cache Server

Gopogo is a fast caching server built from scratch with a focus on low latency and CPU efficiency. It's a Go port of [pogocache](https://github.com/tidwall/pogocache), supporting multiple wire protocols and optimized for concurrent workloads.

## Features

- **Multiple Protocol Support**: Redis, HTTP, Memcache, and PostgreSQL wire protocols with auto-detection
- **Robin Hood Hashing**: Cache-friendly open addressing with configurable load factor (55-95%)
- **Sharded Architecture**: 256 shards by default with upper-bit hash decorrelation for concurrent access
- **2-Random LRU Eviction**: Access-time-based eviction matching pogocache's algorithm
- **Sixpack Key Compression**: 6-bit encoding saves ~25% memory on typical KV keys (enabled by default)
- **LZ4 Persistence**: Atomic save/load with CRC32-verified compressed blocks
- **Batch Transactions**: Lock multiple shards atomically for multi-key operations
- **Eviction & Notify Callbacks**: Observe all cache mutations and eviction events with reason codes
- **OpenTelemetry Metrics**: Counters, histograms, and gauges for all operations
- **NX/XX/KeepTTL**: Conditional store operations at the engine level
- **Compare-and-Swap**: Optimistic concurrency control with per-entry CAS tokens
- **Incremental Sweep**: Background and on-demand expiration cleanup without full scans
- **TLS Support**: Secure connections with TLS/SSL
- **Authentication**: Password-based authentication across all protocols

## Installation

### From Source

```bash
git clone https://github.com/grumpylabs/gopogo.git
cd gopogo
make build
```

### Using Go Install

```bash
go install github.com/grumpylabs/gopogo/cmd/gopogo@latest
```

## Quick Start

```bash
# Start with default settings (Redis protocol on port 6379)
gopogo

# Start with specific settings
gopogo -h 0.0.0.0 -p 6380 --maxmemory 1GB

# Enable multiple protocols
gopogo --redis --http --memcache

# With authentication and TLS
gopogo --auth mypassword --tlsport 6380 --tlscert cert.pem --tlskey key.pem

# With telemetry
gopogo --telemetry --telemetry-exporter otlp --otlp-endpoint localhost:4317

# Disable eviction (reject writes when full) and key compression
gopogo --noevict --nosixpack

# Load data at startup and save it on shutdown (SIGINT/SIGTERM)
gopogo --persist /var/lib/gopogo/data.pogo
```

## Configuration

| Flag | Environment | Default | Description |
|------|-------------|---------|-------------|
| `-h, --host` | `GOPOGO_HOST` | `127.0.0.1` | Listening hostname |
| `-p, --port` | `GOPOGO_PORT` | `6379` | Listening port |
| `-s, --socket` | `GOPOGO_SOCKET` | | Unix socket path |
| `--auth` | `GOPOGO_AUTH` | | Authentication password |
| `--persist` | `GOPOGO_PERSIST` | | Persistence file loaded at startup and saved at shutdown |
| `--threads` | `GOPOGO_THREADS` | CPU count | Number of threads |
| `--shards` | `GOPOGO_SHARDS` | `16` | Number of cache shards |
| `--maxmemory` | `GOPOGO_MAXMEMORY` | `0` | Maximum memory (e.g., 1GB) |
| `--noevict` | `GOPOGO_NOEVICT` | `false` | Disable eviction |
| `--nosixpack` | `GOPOGO_NOSIXPACK` | `false` | Disable sixpack key compression |
| `--loadfactor` | `GOPOGO_LOADFACTOR` | `75` | Hashmap load factor percent (55-95) |
| `--cas` | `GOPOGO_CAS` | `false` | Assign compare-and-swap tokens on every write. When off, memcache `cas` and HTTP `X-CAS` writes always fail |
| `--autosweep` | `GOPOGO_AUTOSWEEP` | `true` | Enable background sweeping |
| `--sweepinterval` | `GOPOGO_SWEEPINTERVAL` | `10s` | Sweep interval |
| `--telemetry` | `GOPOGO_TELEMETRY` | `false` | Enable OpenTelemetry metrics |
| `--telemetry-exporter` | `GOPOGO_TELEMETRY_EXPORTER` | `otlp` | Exporter type (otlp, stdout) |
| `--otlp-endpoint` | `GOPOGO_OTLP_ENDPOINT` | `localhost:4317` | OTLP gRPC endpoint |
| `--tlsport` | `GOPOGO_TLSPORT` | `0` | TLS listening port |
| `--tlscert` | `GOPOGO_TLSCERT` | | TLS certificate file |
| `--tlskey` | `GOPOGO_TLSKEY` | | TLS key file |
| `--http` | `GOPOGO_HTTP` | `false` | Enable HTTP protocol |
| `--memcache` | `GOPOGO_MEMCACHE` | `false` | Enable Memcache protocol |
| `--postgres` | `GOPOGO_POSTGRES` | `false` | Enable Postgres protocol |
| `--redis` | `GOPOGO_REDIS` | `true` | Enable Redis protocol |

## Cache Library API

Gopogo's cache engine can be used as an embedded Go library:

```go
import "github.com/grumpylabs/gopogo/internal/cache"

// Create a cache with options
c := cache.New(&cache.Options{
    NumShards:  256,
    MaxMemory:  1024 * 1024 * 512, // 512MB
    LoadFactor: 0.75,
    Evicted: func(reason cache.EvictionReason, key, value []byte, expires int64, flags uint32, cas uint64) {
        log.Printf("evicted %s (reason: %d)", key, reason)
    },
    Notify: func(newEntry, oldEntry *cache.Entry) {
        // observe all mutations
    },
})

// Store with TTL, NX/XX, KeepTTL
c.Store([]byte("key"), []byte("value"), &cache.StoreOptions{
    TTL:     5 * time.Minute,
    NX:      true,  // only if not exists
    KeepTTL: true,  // preserve existing TTL on update
})

// Load with options
entry, found := c.LoadWithOptions([]byte("key"), &cache.LoadOptions{
    NoTouch: true, // don't update LRU access time
    Entry: func(key, value []byte, expires int64, flags uint32, cas uint64) *cache.LoadUpdate {
        // atomically update during load
        return &cache.LoadUpdate{Value: []byte("new-value")}
    },
})

// Delete with cancel callback
c.DeleteWithOptions([]byte("key"), &cache.DeleteOptions{
    Entry: func(key, value []byte, expires int64, flags uint32, cas uint64) bool {
        return true // return false to cancel delete
    },
})

// Compare-and-swap
success, _ := c.CompareAndSwap([]byte("key"), []byte("new"), casToken, nil)

// Atomic increment with overflow detection
val, err := c.Increment([]byte("counter"), 1)

// Batch transaction (holds locks across operations)
b := c.Begin()
b.Store([]byte("k1"), []byte("v1"), nil)
b.Store([]byte("k2"), []byte("v2"), nil)
entry, _ := b.Load([]byte("k1"))
b.Delete([]byte("k3"))
b.End() // releases all locks

// Persistence
c.Save("/tmp/cache.pogo")
stats, _ := c.LoadFromFile("/tmp/cache.pogo")

// Incremental sweep (non-blocking)
c.SweepPoll(20) // check 20 entries per shard

// Per-shard operations
c.SweepShard(0)
c.ClearShard(0)
c.IterateShard(0, func(e *cache.Entry) bool { return true })
```

## Protocol Examples

### Redis Protocol

Supported commands: GET, SET (EX/PX/EXAT/PXAT/NX/XX/KEEPTTL), SETEX, DEL, EXISTS, MGET, MGETS, MSET, APPEND, PREPEND, INCR, DECR, INCRBY, DECRBY, UINCR, UDECR, UINCRBY, UDECRBY, EXPIRE, TTL, PTTL, TOUCH, KEYS, SCAN (MATCH/COUNT/TYPE), DBSIZE, FLUSH, FLUSHDB, FLUSHALL (ASYNC/SYNC/DELAY), SWEEP, PURGE, STATS, VERSION, INFO, PING, QUIT, SELECT, ECHO, AUTH, SAVE, LOAD.

Counters are stored as decimal text. The `U`-prefixed commands operate on unsigned 64-bit values. `MGETS` returns `[flags, cas, value]` for each found key.

`SAVE [TO <path>] [FAST]` and `LOAD [FROM <path>] [FAST]` default to the `--persist` path. `LOAD` merges into the existing data rather than replacing it. `FAST` is accepted for pogocache compatibility.

```bash
redis-cli -p 6379
> SET user:123 '{"name":"alice"}' EX 3600 NX
OK
> GET user:123
"{\"name\":\"alice\"}"
> TOUCH user:123
(integer) 1
> PTTL user:123
(integer) 3599842
```

### HTTP Protocol

```bash
gopogo --http -p 8080

curl -X PUT http://localhost:8080/mykey -d "myvalue" -H "X-TTL: 3600"
curl http://localhost:8080/mykey
curl -X DELETE http://localhost:8080/mykey
curl http://localhost:8080/stats
```

### Memcache Protocol

```bash
gopogo --memcache -p 11211

telnet localhost 11211
> set key 0 3600 5
> value
STORED
> gets key
VALUE key 0 5 1
value
END
```

### PostgreSQL Protocol

```bash
gopogo --postgres -p 5432

psql -h localhost -p 5432 -U user dbname
> INSERT INTO cache VALUES ('key', 'value');
> SELECT * FROM cache WHERE key = 'key';
```

## Telemetry

When enabled, Gopogo exports OpenTelemetry metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `cache.store.count` | Counter | Store operations (with `result` attribute) |
| `cache.store.duration` | Histogram | Store latency (ms) |
| `cache.load.count` | Counter | Load operations |
| `cache.load.duration` | Histogram | Load latency (ms) |
| `cache.delete.count` | Counter | Delete operations |
| `cache.delete.duration` | Histogram | Delete latency (ms) |
| `cache.hit.count` | Counter | Cache hits |
| `cache.miss.count` | Counter | Cache misses |
| `cache.memory.used` | Gauge | Current memory usage (bytes) |
| `cache.items.count` | Gauge | Current item count |
| `cache.eviction.count` | Counter | Evictions |
| `cache.expiration.count` | Counter | Expirations |
| `cache.sweep.duration` | Histogram | Sweep latency (ms) |
| `cache.save.duration` | Histogram | Persistence save latency (ms) |
| `cache.loadfile.duration` | Histogram | Persistence load latency (ms) |

## Architecture

1. **Shards**: Cache divided into N shards (default 256) with RWMutex per shard. Upper 32 bits of hash select shard, lower bits select bucket (decorrelated).
2. **Robin Hood Hashing**: Open addressing with linear probing. Configurable load factor (55-95%, default 75%). Optional shrinking on delete.
3. **Sixpack Compression**: 6-bit encoding for keys using the character set `-.0123456789:ABCDEFGHIJKLMNOPRSTUVWXY_abcdefghijklmnopqrstuvwxy`. Transparent: `Entry.Key()` always returns the original key.
4. **2-Random LRU Eviction**: Samples 2 random entries (skipping the inserting entry's hash), prefers expired entries, falls back to oldest access time.
5. **Persistence**: LZ4-compressed blocks with 16-byte headers (`POGO` magic + CRC32 + sizes). One block per shard. Atomic writes via temp file + rename.
6. **Callbacks**: Eviction callback with reason codes (expired, lowmem, cleared). Notify callback for all mutations (insert, replace, delete). Load-with-update and delete-with-cancel callbacks.

## Building

```bash
make build          # Build binary
make test           # Run tests
make bench          # Run benchmarks
make build-race     # Build with race detector
make test-coverage  # Generate test coverage
```

## Docker

```bash
docker build -t gopogo .
docker run -p 6379:6379 -e GOPOGO_MAXMEMORY=512MB gopogo
```

## License

MIT License - see LICENSE file for details

## Acknowledgments

Go port of [pogocache](https://github.com/tidwall/pogocache) by Josh Baker / Polypoint Labs.
