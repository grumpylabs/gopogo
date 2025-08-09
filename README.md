# Gopogo - High-Performance Cache Server

Gopogo is a fast caching software built from scratch with a focus on low latency and CPU efficiency. It's a Go implementation inspired by pogocache, supporting multiple wire protocols and optimized for performance.

## Features

- **Multiple Protocol Support**: Redis, HTTP, Memcache, and PostgreSQL wire protocols
- **High Performance**: Robin Hood hashing with optimized memory layout
- **Thread-Safe**: Sharded architecture for concurrent access
- **Memory Management**: Configurable memory limits with 2-random eviction
- **TLS Support**: Secure connections with TLS/SSL
- **Authentication**: Password-based authentication across all protocols
- **Flexible Configuration**: CLI flags, environment variables, and config files

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

### Basic Usage

```bash
# Start with default settings (Redis protocol on port 6379)
gopogo

# Start with specific settings
gopogo -h 0.0.0.0 -p 6380 --maxmemory 1GB

# Enable multiple protocols
gopogo --redis --http --memcache

# With authentication
gopogo --auth mypassword

# With TLS
gopogo --tlsport 6380 --tlscert cert.pem --tlskey key.pem
```

### Configuration Options

| Flag | Environment | Default | Description |
|------|-------------|---------|-------------|
| `-h, --host` | `GOPOGO_HOST` | `127.0.0.1` | Listening hostname |
| `-p, --port` | `GOPOGO_PORT` | `6379` | Listening port |
| `-s, --socket` | `GOPOGO_SOCKET` | | Unix socket path |
| `--auth` | `GOPOGO_AUTH` | | Authentication password |
| `--threads` | `GOPOGO_THREADS` | CPU count | Number of threads |
| `--shards` | `GOPOGO_SHARDS` | `16` | Number of cache shards |
| `--maxmemory` | `GOPOGO_MAXMEMORY` | `0` | Maximum memory (e.g., 1GB) |
| `--evict` | `GOPOGO_EVICT` | `2random` | Eviction policy |
| `--tlsport` | `GOPOGO_TLSPORT` | `0` | TLS listening port |
| `--tlscert` | `GOPOGO_TLSCERT` | | TLS certificate file |
| `--tlskey` | `GOPOGO_TLSKEY` | | TLS key file |
| `--http` | `GOPOGO_HTTP` | `false` | Enable HTTP protocol |
| `--memcache` | `GOPOGO_MEMCACHE` | `false` | Enable Memcache protocol |
| `--postgres` | `GOPOGO_POSTGRES` | `false` | Enable Postgres protocol |
| `--redis` | `GOPOGO_REDIS` | `true` | Enable Redis protocol |

## Protocol Examples

### Redis Protocol

```bash
# Using redis-cli
redis-cli -p 6379
> SET key value
OK
> GET key
"value"
> DEL key
(integer) 1
```

### HTTP Protocol

```bash
# Enable HTTP protocol
gopogo --http -p 8080

# Store a value
curl -X PUT http://localhost:8080/mykey -d "myvalue"

# Retrieve a value
curl http://localhost:8080/mykey

# Delete a value
curl -X DELETE http://localhost:8080/mykey

# Get stats
curl http://localhost:8080/stats
```

### Memcache Protocol

```bash
# Enable Memcache protocol
gopogo --memcache -p 11211

# Using telnet
telnet localhost 11211
> set key 0 0 5
> value
STORED
> get key
VALUE key 0 5
value
END
```

### PostgreSQL Protocol

```bash
# Enable Postgres protocol
gopogo --postgres -p 5432

# Using psql
psql -h localhost -p 5432 -U user dbname
> INSERT INTO cache VALUES ('key', 'value');
INSERT 0 1
> SELECT * FROM cache WHERE key = 'key';
```

## Performance

Gopogo is optimized for high performance with:

- **Robin Hood Hashing**: Minimizes probe distances for cache-friendly access
- **Sharded Architecture**: Reduces lock contention
- **Zero-copy Operations**: Where possible
- **Optimized Memory Layout**: Compact entry storage
- **Efficient Eviction**: 2-random algorithm balances speed and quality

## Building from Source

```bash
# Build binary
make build

# Run tests
make test

# Run benchmarks
make bench

# Build with race detector
make build-race

# Generate test coverage
make test-coverage
```

## Docker

```bash
# Build Docker image
docker build -t gopogo .

# Run container
docker run -p 6379:6379 gopogo

# With custom settings
docker run -p 6379:6379 \
  -e GOPOGO_AUTH=mypassword \
  -e GOPOGO_MAXMEMORY=512MB \
  gopogo
```

## Architecture

Gopogo uses a sharded cache architecture where:

1. **Shards**: The cache is divided into multiple shards for concurrent access
2. **Robin Hood Hashing**: Each shard uses Robin Hood hashing for O(1) operations
3. **Memory Management**: Per-shard memory tracking with global limits
4. **Eviction**: 2-random eviction when memory limits are reached
5. **Protocol Detection**: Automatic protocol detection for multi-protocol support

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see LICENSE file for details

## Acknowledgments

Inspired by [pogocache](https://github.com/tidwall/pogocache) by Josh Baker