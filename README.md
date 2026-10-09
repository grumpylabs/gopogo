# Gopogo - High-Performance Cache Server

Gopogo is a fast caching server built from scratch with a focus on low latency and CPU efficiency. It's a Go port of [pogocache](https://github.com/tidwall/pogocache), supporting multiple wire protocols and optimized for concurrent workloads.

## Features

- **Multiple Protocol Support**: Redis, HTTP, Memcache, and PostgreSQL wire protocols with auto-detection
- **Event Loops**: On Linux, one epoll loop per thread serves plain connections, as in pogocache, for about pogocache's CPU per request
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

# With telemetry (metrics and traces)
gopogo --telemetry --otlp-endpoint otel-collector:4317 --telemetry-environment stage

# Disable eviction (reject writes when full) and key compression
gopogo --noevict --nosixpack

# Load data at startup and save it on shutdown (SIGINT/SIGTERM)
gopogo --persist /var/lib/gopogo/data.pogo
```

## Configuration

Boolean flags take `=true` or `=false` (e.g. `--cas=false`); pogocache-style `--cas no` is rejected rather than silently enabling the flag.

| Flag | Environment | Default | Description |
|------|-------------|---------|-------------|
| `--host` | `GOPOGO_HOST` | `127.0.0.1` | Listening hostname |
| `-p, --port` | `GOPOGO_PORT` | `6379` | Listening port |
| `-s, --socket` | `GOPOGO_SOCKET` | | Unix socket path |
| `--auth` | `GOPOGO_AUTH` | | Authentication password. The memcache protocol cannot authenticate, so with a password set every memcache command is refused |
| `--persist` | `GOPOGO_PERSIST` | | Persistence file loaded at startup and saved at shutdown |
| `--threads` | `GOPOGO_THREADS` | `0` | Threads serving connections: the number of event loops on Linux, and GOMAXPROCS (one more with event loops); 0 uses Go's default, which honors container CPU limits |
| `--eventloops` | `GOPOGO_EVENTLOOPS` | `true` | Serve plain TCP and Unix socket connections from one epoll loop per thread (Linux); `false` gives each connection a goroutine, as on other platforms |
| `--shards` | `GOPOGO_SHARDS` | `256` | Number of cache shards |
| `--maxmemory` | `GOPOGO_MAXMEMORY` | `80%` | Maximum memory: bytes with a k/m/g/t suffix (e.g. 1GB), a percentage of available memory (e.g. 80%; the container memory limit when set), or 0 for unlimited |
| `--evict` | `GOPOGO_EVICT` | `yes` | Evict keys when maxmemory is reached; `no` rejects writes instead (`ERR out of memory`) |
| `--noevict` | `GOPOGO_NOEVICT` | `false` | Same as `--evict=no` |
| `--nosixpack` | `GOPOGO_NOSIXPACK` | `false` | Disable sixpack key compression |
| `--loadfactor` | `GOPOGO_LOADFACTOR` | `75` | Hashmap load factor percent (55-95) |
| `--cas` | `GOPOGO_CAS` | `false` | Assign compare-and-swap tokens on every write. When off, memcache `cas` and HTTP `X-CAS` writes always fail |
| `--autosweep` | `GOPOGO_AUTOSWEEP` | `true` | Enable background sweeping |
| `--sweepinterval` | `GOPOGO_SWEEPINTERVAL` | `10s` | Sweep interval |
| `--log-level` | `GOPOGO_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`; `debug` logs every command and cache stats |
| `--verbose` | `GOPOGO_VERBOSE` | `false` | Log telemetry exports and other detail |
| `--pprof` | `GOPOGO_PPROF` | (off) | Serve Go runtime profiles at `/debug/pprof/` on a loopback address such as `localhost:6060`; reach it with `kubectl port-forward` |
| `--telemetry` | `GOPOGO_TELEMETRY` | `false` | Enable OpenTelemetry metrics, traces and logs (see Telemetry) |
| `--telemetry-exporter` | `GOPOGO_TELEMETRY_EXPORTER` | `otlp` | Exporter type (otlp, stdout) |
| `--metrics-exporter` | `GOPOGO_METRICS_EXPORTER` | `--telemetry-exporter` | Metrics exporter (otlp, stdout, none) |
| `--traces-exporter` | `GOPOGO_TRACES_EXPORTER` | `--telemetry-exporter` | Traces exporter (otlp, stdout, none) |
| `--otlp-protocol` | `GOPOGO_OTLP_PROTOCOL` | `grpc` | OTLP protocol: `grpc` or `http` |
| `--otlp-endpoint` | `GOPOGO_OTLP_ENDPOINT` | | OTLP endpoint: `host:port` or base URL (default `OTEL_EXPORTER_OTLP_ENDPOINT`, else localhost) |
| `--otlp-insecure` | `GOPOGO_OTLP_INSECURE` | `true` | Plaintext to a `host:port` endpoint |
| `--otlp-headers` | `GOPOGO_OTLP_HEADERS` | | OTLP request headers, `key=value,...` |
| `--telemetry-environment` | `GOPOGO_TELEMETRY_ENVIRONMENT` | | `deployment.environment.name` resource attribute |
| `--trace-sample-ratio` | `GOPOGO_TRACE_SAMPLE_RATIO` | `1.0` | Fraction of new traces sampled; when not given, `OTEL_TRACES_SAMPLER` applies if set |
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

`STATS` (and memcache `stats`) reports pogocache's fields: process info (pid, uptime, version, githash, CPU time, threads, rss), connection counts, per-command counters (`cmd_get`, `cmd_set`, `cmd_flush`, `cmd_touch`, get/delete/incr/decr/touch hits and misses, `store_no_memory`, `auth_cmds`, `auth_errors`) and cache size (`bytes`, `curr_items`, `total_items`), followed by `evictions`, `expired_unfetched` and `limit_maxbytes`. On Linux `rss` is the resident set size; elsewhere it is the memory the Go runtime has mapped.

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

Text protocol commands: `get`, `gets`, `gat`, `gats`, `set`, `add`, `replace`, `append`, `prepend`, `cas`, `delete`, `incr`, `decr`, `touch`, `flush_all`, `stats`, `version`, `verbosity` and `quit`. Expiration times follow memcached: 0 never expires, a negative value expires at once, and values over 30 days are Unix times.

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

With `--telemetry`, Gopogo exports OpenTelemetry metrics, traces and logs over OTLP (`--telemetry-exporter otlp`, the default) or to stdout. Metrics are exported every 30 seconds, and pending metrics, spans and logs are flushed on shutdown. `--metrics-exporter` and `--traces-exporter` override the exporter for one signal, for example to keep logs on OTLP while metrics and spans go to stdout as JSON lines; `none` exports nothing for that signal (spans are still created, so log records keep their trace IDs).

| Flag | Default | Description |
|------|---------|-------------|
| `--otlp-protocol` | `OTEL_EXPORTER_OTLP_PROTOCOL`, else `grpc` | `grpc` or `http` (OTLP/HTTP protobuf) |
| `--otlp-endpoint` | `OTEL_EXPORTER_OTLP_ENDPOINT`, else localhost | `host:port`, or a base URL; OTLP/HTTP posts to `<base>/v1/metrics` and `<base>/v1/traces` |
| `--otlp-insecure` | `true` | Plaintext to a `host:port` endpoint; a URL endpoint's scheme decides |
| `--otlp-headers` | `OTEL_EXPORTER_OTLP_HEADERS` | Request headers, `key=value,...` |
| `--telemetry-environment` | | `deployment.environment.name` resource attribute |
| `--trace-sample-ratio` | `1.0` | Fraction of new traces sampled; a caller's sampling decision is respected. When it is not given, the standard `OTEL_TRACES_SAMPLER` and `OTEL_TRACES_SAMPLER_ARG` choose the sampler if set |

Telemetry follows the OpenTelemetry semantic conventions (v1.40.0). The resource carries `service.name`, `service.version`, `service.instance.id` (the host name, which is the pod name in Kubernetes), `deployment.environment.name` (and the older `deployment.environment`, which some backends still read), `host.name`, `os.*`, `process.*`, `telemetry.sdk.*` and, where the cgroup shows it, `container.id` (with cgroup v2 in Kubernetes it usually does not; `k8s.pod.uid` and `k8s.container.name` identify the container instead). `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` add to or override these; the Helm chart uses `OTEL_RESOURCE_ATTRIBUTES` to add the pod's `k8s.*` attributes, `telemetry.clusterName`, `telemetry.serviceNamespace` and any `telemetry.resourceAttributes`.

For an OTLP/HTTP endpoint that takes a bearer token, pass the token in the environment rather than on the command line:

```bash
GOPOGO_OTLP_HEADERS="Authorization=Bearer <token>" gopogo --telemetry \
  --otlp-protocol http --otlp-endpoint https://ingest.example.com/src-abc
```

In the Helm chart, put the header in a Secret and set `telemetry.headersSecret.name`.

### Traces

Every command runs in a server span named after the command (`GET`, `SET`, memcache `get`; unrecognized commands are `UNKNOWN`), with `db.system.name` (`gopogo`), `db.operation.name`, `network.protocol.name` (redis, http, memcache, postgres), `network.transport` (tcp or unix), `client.address` and `client.port`. Failed commands set the span status to error and `error.type` to the error reply's first word (`ERR`, `WRONGPASS`, `CLIENT_ERROR`, ...). Keys and values are never recorded. Each HTTP request runs in its own server span, `GET /{key}` (or `/`, `/@stats`, `/@keys`), with `http.request.method`, `http.route`, `http.response.status_code`, `url.scheme`, `network.protocol.version`, `client.address`, `client.port` and `user_agent.original`; a 5xx response sets the span status to error and `error.type` to the status code. The key is never recorded, so there is no `url.path`. The cache command runs in a child span. HTTP requests continue the caller's trace from a W3C `traceparent` header; the other protocols cannot carry trace context, so their spans start new traces. Loading and saving the persistence file run in `persist.load` and `persist.save` spans.

### Logs

`--log-level debug` adds a debug record per command, such as `redis SET ok in 91us from 10.42.6.21` or `redis INCR failed in 64us from 10.42.6.21: ERR value is not an integer or out of range`. Its fields use the span's attribute names (`db.system.name`, `db.operation.name`, `network.protocol.name`, `client.address`, `client.port`, and `error.type` and `exception.message` when it failed) plus `duration_us`, and it is written with the command's span context so each record links to its trace. Every 30 seconds a stats record summarizes the cache (`cache holds 1234 items in 5.2 MiB; 9000 gets (87.5% hits), ...`) with the counters as fields. Exported log records also carry each field as an attribute, and `code.file.path`, `code.line.number` and `code.function.name`.

A debug record per command costs CPU: with telemetry on, it roughly triples the CPU per command under heavy load, so leave `--log-level debug` for low traffic or investigation. Stderr is buffered and flushed every second, and immediately for panics and fatal errors.

Gopogo logs with zap as one JSON object per line on stderr (`level`, `time`, `caller`, `msg`, then the fields), including startup and shutdown events and `--verbose` output such as telemetry export results. Records logged in a traced command also carry `trace_id` and `span_id`. With `--telemetry`, each entry is also exported as an OpenTelemetry log record whose body is that same JSON line, with the matching severity and, for command records, the command's trace and span. OpenTelemetry's own error reports and the `--verbose` log-export results go to stderr only, so a failing log export cannot feed itself.

### Metrics

| Metric | Type | Attributes / description |
|--------|------|-------------|
| `gopogo.commands` | Counter | `network.protocol.name`, `db.operation.name` (get, set, flush, touch); STATS `cmd_*` |
| `gopogo.command.duration` | Histogram (s) | Every command's duration, with its span's `db.system.name`, `db.operation.name`, `network.protocol.name` and, when it failed, `error.type`; buckets from 10µs to 1s, and exemplars link samples to their traces |
| `http.server.request.duration` | Histogram (s) | Every HTTP request's duration, with `http.request.method`, `http.response.status_code`, `http.route`, `url.scheme`, `network.protocol.name`, `network.protocol.version` and, for 5xx, `error.type`; buckets from 10µs to 1s |
| `gopogo.keyspace.lookups` | Counter | `network.protocol.name`, `operation` (get, delete, incr, decr, touch), `result` (hit, miss) |
| `gopogo.store.rejected` | Counter | `network.protocol.name`, `reason` (no_memory, too_large) |
| `gopogo.auth.attempts` | Counter | `network.protocol.name`, `result` (success, failure) |
| `gopogo.connections.active` / `.limit` | Gauge | Open connections and `--maxconns` |
| `gopogo.connections.accepted` / `.rejected` | Counter | Connections accepted and refused at the limit |
| `cache.items.stored` | Counter | Successful writes (STATS `total_items`) |
| `process.cpu.time` | Counter | Seconds, `cpu.mode` (user, system) |
| `process.memory.usage` | Gauge | Resident memory (bytes; approximate off Linux) |
| `go.*` | Various | Go runtime: memory, GC, goroutines |
| `cache.store.count` | Counter | Engine store operations (with `result` attribute) |
| `cache.store.duration` | Histogram (s) | Store latency in the cache engine, without protocol or network time; buckets 10µs to 1s |
| `cache.load.count` | Counter | Engine load operations |
| `cache.load.duration` | Histogram (s) | Load latency in the cache engine, without protocol or network time; buckets 10µs to 1s |
| `cache.delete.count` | Counter | Engine delete operations |
| `cache.delete.duration` | Histogram (s) | Delete latency in the cache engine, without protocol or network time; buckets 10µs to 1s |
| `cache.hit.count` / `cache.miss.count` | Counter | Engine lookups, including internal ones |
| `cache.memory.used` | Gauge | Current memory usage (bytes) |
| `cache.items.count` | Gauge | Current item count |
| `cache.eviction.count` | Counter | Evictions |
| `cache.expiration.count` | Counter | Entries removed because their TTL elapsed, whether found on access, deleted or swept (STATS `num_expired`) |
| `cache.sweep.duration` | Histogram (s) | Sweep latency; buckets 1ms to 60s |
| `cache.save.duration` | Histogram (s) | Persistence save latency; buckets 1ms to 60s |
| `cache.loadfile.duration` | Histogram (s) | Persistence load latency; buckets 1ms to 60s |

The `gopogo.*` counters count client commands like STATS does; the `cache.*` engine counters count every engine operation, including internal lookups.

## Architecture

1. **Shards**: Cache divided into N shards (default 256) with RWMutex per shard. Upper 32 bits of hash select shard, lower bits select bucket (decorrelated).
2. **Robin Hood Hashing**: Open addressing with linear probing. Configurable load factor (55-95%, default 75%). Optional shrinking on delete.
3. **Sixpack Compression**: 6-bit encoding for keys using the character set `-.0123456789:ABCDEFGHIJKLMNOPRSTUVWXY_abcdefghijklmnopqrstuvwxy`. Transparent: `Entry.Key()` always returns the original key.
4. **2-Random LRU Eviction**: Samples 2 random entries (skipping the inserting entry's hash), prefers expired entries, falls back to oldest access time.
5. **Persistence**: LZ4-compressed blocks with 16-byte headers (`POGO` magic + CRC32 + sizes). One block per shard. Atomic writes via temp file + rename.
6. **Callbacks**: Eviction callback with reason codes (expired, lowmem, cleared). Notify callback for all mutations (insert, replace, delete). Load-with-update and delete-with-cancel callbacks.
7. **Connections**: Each protocol is a state machine over bytes: it consumes complete requests from a connection's input and appends the replies, without blocking. On Linux, `--threads` event loops (one OS thread each) wait in epoll for any of their connections, then read, process and write each with one syscall per batch, as pogocache does. Go's own poller instead parks and wakes a goroutine per request, which costs a failed read and scheduler work every time. TLS connections, Postgres (which can upgrade to TLS) and MONITOR streams move to a goroutine of their own; other platforms, and `--eventloops=false`, give every connection a goroutine running the same state machine. Plain `GET key` and `SET key value` over RESP take a path that skips argument strings and the command table, as pogocache's do.

## Building

```bash
make build          # Build binary
make test           # Run tests
make bench          # Run benchmarks
make build-race     # Build with race detector
make test-coverage  # Generate test coverage
make integration    # Run pogocache's protocol tests against a live server
make fuzz           # Fuzz each protocol parser (FUZZTIME=60s each)
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
make loadgen     # bin/gopogo-loadgen: drive a server with a mixed RESP workload
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

Set `loadgen.enabled=true` to run `gopogo-loadgen` (shipped in the same image) as a Deployment that drives the release continuously at `loadgen.rate` commands per second, for exercising the cache and its telemetry.

See `deploy/helm/gopogo/values.yaml` for all options. The chart uses `ghcr.io/grumpylabs/gopogo`, tagged with the chart's `appVersion`; override `image.repository` and `image.tag` to use your own registry.

## License

MIT License - see [LICENSE](LICENSE). Gopogo is derived from pogocache, Copyright (c) 2025 Polypoint Labs, LLC, also MIT.

## Acknowledgments

Go port of [pogocache](https://github.com/tidwall/pogocache) by Josh Baker / Polypoint Labs.
