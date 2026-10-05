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
gopogo --host 0.0.0.0 -p 6380 --maxmemory 1GB

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
| `--host` | `GOPOGO_HOST` | `127.0.0.1` | Listening hostname |
| `-p, --port` | `GOPOGO_PORT` | `6379` | Listening port |
| `-s, --socket` | `GOPOGO_SOCKET` | | Unix socket path |
| `--auth` | `GOPOGO_AUTH` | | Authentication password. The memcache protocol cannot authenticate, so with a password set every memcache command is refused |
| `--persist` | `GOPOGO_PERSIST` | | Persistence file loaded at startup and saved at shutdown |
| `--threads` | `GOPOGO_THREADS` | CPU count | Number of threads |
| `--shards` | `GOPOGO_SHARDS` | `16` | Number of cache shards |
| `--maxmemory` | `GOPOGO_MAXMEMORY` | `0` | Maximum memory: bytes with a k/m/g/t suffix (e.g. 1GB), a percentage of available memory (e.g. 80%; the container memory limit when set), or 0 for unlimited |
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
| `--tlscacert` | `GOPOGO_TLSCACERT` | | CA file; client certificates are verified when presented |
| `--maxconns` | `GOPOGO_MAXCONNS` | `1024` | Maximum client connections; extra connections are closed |
| `--backlog` | `GOPOGO_BACKLOG` | `1024` | Listen accept backlog (Linux, macOS) |
| `--reuseport` | `GOPOGO_REUSEPORT` | `false` | Set `SO_REUSEPORT` so several servers can share a port (Linux, macOS) |
| `--tcpnodelay` | `GOPOGO_TCPNODELAY` | `true` | Disable Nagle's algorithm |
| `--quickack` | `GOPOGO_QUICKACK` | `false` | Enable TCP quick acks (Linux) |
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

Supported commands: GET, SET (EX/PX/EXAT/PXAT/NX/XX/KEEPTTL), SETEX, DEL, EXISTS, MGET, MGETS, MSET, APPEND, PREPEND, INCR, DECR, INCRBY, DECRBY, UINCR, UDECR, UINCRBY, UDECRBY, EXPIRE, TTL, PTTL, TOUCH, KEYS, SCAN (MATCH/COUNT/TYPE), DBSIZE, FLUSH, FLUSHDB, FLUSHALL (ASYNC/SYNC/DELAY), SWEEP, PURGE, STATS, VERSION, INFO, PING, QUIT, SELECT, ECHO, AUTH, SAVE, LOAD, MONITOR, DEBUG (POPULATE/DETACH).

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

HTTP requests map onto the same commands as Redis: `GET /key`, `PUT /key` (value in the body) and `DELETE /key`. `PUT` accepts `ex` (or `ttl`), `flags`, `cas`, `nx` and `xx` query parameters, and the `X-TTL`, `X-Flags` and `X-CAS` headers. Replies are plain text: `Stored`, `Deleted` or `Not Found`. Authenticate with `?auth=<password>` or `Authorization: Bearer <password>`. Paths starting with `@` are reserved; `/@stats` and `/@keys?pattern=` return JSON.

```bash
gopogo --http -p 8080

curl -X PUT 'http://localhost:8080/mykey?ex=3600' -d "myvalue"
curl http://localhost:8080/mykey
curl -X DELETE http://localhost:8080/mykey
curl http://localhost:8080/@stats
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

A query is a cache command, not SQL, as in pogocache. Values with spaces go in single quotes, and `E'...'` strings take backslash escapes. Both the simple and the extended query protocol work, so drivers can bind parameters (`GET $1`). `BEGIN`, `COMMIT` and `ROLLBACK` are accepted and ignored, and a leading `::bytea` or `::text` sets the result column type. Passwords use SCRAM-SHA-256. With `--tlscert` and `--tlskey` set, clients can upgrade the connection to TLS on the main port (`sslmode=require` or `verify-full`); without them, TLS requests are declined. Query cancellation is not supported.

```bash
gopogo --postgres -p 5432

psql -h localhost -p 5432 -U user dbname
> SET greeting 'hello world';
> GET greeting;
> MGET greeting other;
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
make integration    # Run pogocache's protocol tests against a live server
```

`make integration` runs the test suite from pogocache's `tools/tests`, copied unchanged into `test/integration` (a separate Go module, so its client libraries stay out of gopogo's `go.mod`). `run.sh` starts `bin/gopogo` on port 9401 with every protocol enabled and stops it afterwards. `INTEGRATION_RUN` and `INTEGRATION_SKIP` select tests, e.g. `make integration INTEGRATION_RUN=^TestRESP`.

## Docker

```bash
make container
docker run -p 6379:6379 -e GOPOGO_MAXMEMORY=512MB ghcr.io/grumpylabs/gopogo:dev
```

## Container Image

The version is stamped from git: the nearest `v*` tag (`v1.2.3` builds report `1.2.3`; later commits `1.2.3-<n>-g<commit>`). The `image` workflow publishes `ghcr.io/grumpylabs/gopogo` for linux/amd64 and linux/arm64 on every push to `main` (`:latest` and `:<commit>`) and for `v1.2.3` git tags (`:1.2.3`). Each arch is built natively on its own runner: `make amd64` / `make arm64` build a static binary, `make ci-pkg-<arch>` packages and pushes `:<commit>-<arch>`, and `make ci-pkg` joins them into the multi-arch tags with `docker manifest`.

```bash
make container   # local image ghcr.io/grumpylabs/gopogo:dev for this machine
make run-ports   # run it
make dev         # push ghcr.io/grumpylabs/gopogo:<user>-<commit> (amd64 + arm64)
make images      # build the CI images :<commit>-amd64 and :<commit>-arm64 locally, no push
make push        # the whole CI publish from this machine (needs `make login`)

docker run -p 6379:6379 ghcr.io/grumpylabs/gopogo --host 0.0.0.0 --http --memcache --postgres
```

## Kubernetes

A Helm chart is in `deploy/helm/gopogo`. It runs a Deployment, or a StatefulSet with a PersistentVolumeClaim per replica when persistence is enabled (the cache is loaded at startup and saved on shutdown). By default `--maxmemory` is 80% of the container memory limit, so set `resources.limits.memory`. Each replica is an independent cache.

```bash
helm install cache deploy/helm/gopogo \
  --set protocols.http=true \
  --set auth.enabled=true --set auth.password=s3cret \
  --set persistence.enabled=true
helm test cache
```

See `deploy/helm/gopogo/values.yaml` for all options. The chart uses `ghcr.io/grumpylabs/gopogo`, tagged with the chart's `appVersion`; override `image.repository` and `image.tag` to use your own registry.

## License

MIT License - see LICENSE file for details

## Acknowledgments

Go port of [pogocache](https://github.com/tidwall/pogocache) by Josh Baker / Polypoint Labs.
