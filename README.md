# MinIO Cache

A high-performance, transparent S3 caching proxy that sits between clients and MinIO, using the S3-FIFO eviction algorithm to cache `GetObject` responses in memory.

## What It Does

MinIO Cache is a caching layer for S3-compatible object storage (MinIO) that:

- **Transparently caches GET requests** — Clients connect to the proxy as if it were MinIO itself
- **Reduces backend load** — Frequently accessed objects are served from memory without hitting MinIO
- **Intelligently evicts entries** — Uses the S3-FIFO algorithm for optimal cache hit rates
- **Invalidates on writes** — Automatically invalidates cache entries when objects are modified or deleted
- **Supports range requests** — Caches partial object requests separately from full objects
- **Observable** — Exports OpenTelemetry metrics (cache hits, misses, evictions, size)

## How It Works

```
Client ──► MinIO Cache Proxy ──► MinIO Cluster
              │
         S3-FIFO Cache
          (in-memory)
```

### Request Flow

1. **Client sends S3 request** → The proxy receives it via the `s3s` crate (AWS Sig V4 authentication)
2. **Cache lookup** → For `GetObject`, check if `(bucket, key, range)` exists in cache
   - **Cache hit** → Return cached response immediately (metrics: `cache.hit`)
   - **Cache miss** → Forward request to upstream MinIO (metrics: `cache.miss`)
3. **Buffer response** → Stream body from MinIO, buffer up to `MAX_CACHEABLE_OBJECT_SIZE`
4. **Store in cache** → Insert into S3-FIFO cache with TTL
5. **Return to client** → Stream the buffered response back

### Write-Through Invalidation

When clients modify objects, the proxy invalidates all related cache entries:

- `PUT /bucket/key` → Invalidate all entries for `(bucket, key, *)` (all ranges)
- `DELETE /bucket/key` → Invalidate all entries for `(bucket, key, *)`
- `DELETE /?delete` → Invalidate all listed objects
- `PUT /bucket/key?x-amz-copy-source=...` → Invalidate destination key

### S3-FIFO Eviction Algorithm

The cache uses a three-tier eviction strategy optimized for object storage workloads:

1. **Small FIFO (10% capacity)** — New objects enter here
2. **Main FIFO (90% capacity)** — Objects accessed more than once are promoted
3. **Ghost list** — Tracks recently evicted keys to optimize re-insertion

This provides better hit rates than LRU for workloads with a mix of one-hit-wonders and frequently accessed objects.

## Features

- ✅ S3 protocol compatibility via `s3s` crate
- ✅ In-memory caching with configurable size and TTL
- ✅ Range request support (caches partial reads separately)
- ✅ Write-through invalidation
- ✅ OpenTelemetry metrics and logging
- ✅ AWS Signature V4 authentication
- ✅ Graceful shutdown
- ✅ Configurable via environment variables

## Configuration

All configuration is done via environment variables:

| Variable | Default | Description |
|---|---|---|
| `LISTEN_ADDR` | `0.0.0.0:8080` | Proxy listen address |
| `UPSTREAM_ENDPOINT` | *(required)* | MinIO endpoint URL |
| `UPSTREAM_ACCESS_KEY_ID` | *(required)* | Proxy's MinIO credentials |
| `UPSTREAM_SECRET_ACCESS_KEY` | *(required)* | Proxy's MinIO credentials |
| `UPSTREAM_REGION` | `us-east-1` | S3 region |
| `CLIENT_ACCESS_KEY_ID` | *(required)* | Client auth credentials |
| `CLIENT_SECRET_ACCESS_KEY` | *(required)* | Client auth credentials |
| `CACHE_MAX_ENTRIES` | `10000` | Max cached objects |
| `CACHE_MAX_SIZE_BYTES` | `1073741824` (1 GiB) | Max cache size |
| `CACHE_TTL_SECONDS` | `300` | TTL for cached entries |
| `MAX_CACHEABLE_OBJECT_SIZE` | `10485760` (10 MiB) | Skip caching above this |
| `OTEL_GRPC_ENDPOINT_URL` | *(optional)* | OpenTelemetry collector |
| `WORKER_THREADS` | `4` | Tokio worker threads |

## Building

### Prerequisites

- Rust 1.93+ (Edition 2024)
- Cargo

### Build from source

```bash
cargo build --release
```

The binary will be at `target/release/minio_cache`.

### Build Docker image

```bash
docker build -t minio-cache:latest .
```

## Usage

### Running locally

```bash
export UPSTREAM_ENDPOINT=http://localhost:9000
export UPSTREAM_ACCESS_KEY_ID=minioadmin
export UPSTREAM_SECRET_ACCESS_KEY=minioadmin
export CLIENT_ACCESS_KEY_ID=client
export CLIENT_SECRET_ACCESS_KEY=clientsecret

cargo run --release
```

### Running with Docker

```bash
docker run -p 8080:8080 \
  -e UPSTREAM_ENDPOINT=http://minio:9000 \
  -e UPSTREAM_ACCESS_KEY_ID=minioadmin \
  -e UPSTREAM_SECRET_ACCESS_KEY=minioadmin \
  -e CLIENT_ACCESS_KEY_ID=client \
  -e CLIENT_SECRET_ACCESS_KEY=clientsecret \
  minio-cache:latest
```

### Client configuration

Configure your S3 client to use the proxy:

```bash
# AWS CLI
aws configure set aws_access_key_id client
aws configure set aws_secret_access_key clientsecret

# Use the proxy endpoint
aws s3 ls s3://my-bucket --endpoint-url http://localhost:8080
aws s3 cp s3://my-bucket/file.txt . --endpoint-url http://localhost:8080
```

## Metrics

When `OTEL_GRPC_ENDPOINT_URL` is configured, the following metrics are exported:

| Metric | Type | Description |
|---|---|---|
| `cache.hit` | Counter | Number of cache hits |
| `cache.miss` | Counter | Number of cache misses |
| `cache.invalidation` | Counter | Number of cache invalidations |
| `cache.size_bytes` | Gauge | Current cache size in bytes |
| `cache.entry_count` | Gauge | Current number of cached entries |

## Testing

```bash
# Run unit tests
cargo test

# Run with verbose output
cargo test -- --nocapture
```

## Architecture

### Project Structure

```
src/
├── bin/minio_cache.rs    # Binary entrypoint
├── lib.rs                # Application setup and server
├── config.rs             # Environment variable parsing
├── error.rs              # Error types
├── telemetry.rs          # OpenTelemetry setup
├── cache.rs              # S3-FIFO cache implementation
├── cache_key.rs          # Cache key type
├── cache_entry.rs        # Cache entry type
├── proxy_service.rs      # S3 trait implementation
└── auth.rs               # Authentication setup
```

### Dependencies

- **s3s 0.13.0-alpha.3** — S3 protocol implementation
- **s3s-aws 0.12** — AWS SDK integration for upstream forwarding
- **aws-sdk-s3** — MinIO client
- **hyper-util** — HTTP server
- **tokio** — Async runtime
- **opentelemetry** — Metrics and logging

## Limitations

- **Single-node only** — Cache is not distributed across multiple proxy instances
- **No persistence** — Cache is in-memory and lost on restart
- **Large objects** — Objects exceeding `MAX_CACHEABLE_OBJECT_SIZE` return an error (stream is already consumed)
- **Limited operations** — Only caches `GetObject`; all other operations are proxied without caching

## Future Improvements

- [ ] Streaming passthrough for oversized objects (requires upstream re-request)
- [ ] Distributed cache coordination (Redis/Valkey)
- [ ] Persistent cache tier (disk spillover)
- [ ] Cache warming via background fetch
- [ ] Admin API for cache inspection and invalidation

## License

[Your License Here]

## Contributing

[Your Contributing Guidelines Here]
