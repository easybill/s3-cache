# S3 Cache

A high-performance, transparent S3 caching proxy that sits between clients and S3, using the S3-FIFO eviction algorithm to cache `GetObject` responses in memory.

> [!WARNING]
> This project is still experimental and under active development. Use in production at your own risk.

## What It Does

S3 Cache is a caching layer for S3-compatible object storage (e.g. MinIO) that:

- **Transparently caches GET requests**: Clients connect to the proxy as if it were S3 itself
- **Reduces backend load**: Frequently accessed objects are served from memory without hitting S3
- **Intelligently evicts entries**: Uses the S3-FIFO algorithm for optimal cache hit rates
- **Invalidates on writes**: Automatically invalidates cache entries when objects are modified or deleted
- **Supports range requests**: Caches partial object requests separately from full objects
- **Observable**: Exports OpenTelemetry metrics (cache hits, misses, evictions, size)

### Request Flow

1. **Client sends S3 request**: The proxy receives it via the `s3s` crate (AWS Sig V4 authentication)
2. **Cache lookup**: For `GetObject`, check if `(bucket, key, range)` exists in cache
   - **Cache hit**: Return cached response immediately (metrics: `cache.hit`)
   - **Cache miss**: Forward request to upstream S3 (metrics: `cache.miss`)
3. **Buffer response**: Stream body from S3, buffer up to `CACHE_MAX_OBJECT_SIZE_BYTES`
4. **Store in cache**: Insert into S3-FIFO cache with TTL
5. **Return to client**: Stream the buffered response back

### Write-Through Invalidation

When clients modify objects, the proxy invalidates all related cache entries:

- `PUT /bucket/key`: Invalidate all entries for `(bucket, key, *)` (all ranges)
- `DELETE /bucket/key`: Invalidate all entries for `(bucket, key, *)`
- `DELETE /?delete`: Invalidate all listed objects
- `PUT /bucket/key?x-amz-copy-source=...`: Invalidate destination key

## Configuration

All options can be set as CLI flags (e.g. `--upstream-endpoint`) or environment variables (e.g. `UPSTREAM_ENDPOINT`). CLI flags take precedence over environment variables.

| Variable / Flag                                           | Default              | Description                           |
| --------------------------------------------------------- | -------------------- | ------------------------------------- |
| `LISTEN_ADDR` / `--listen-addr`                           | `0.0.0.0:8080`       | Proxy listen address                  |
| `UPSTREAM_ENDPOINT` / `--upstream-endpoint`               | *(required)*         | S3 endpoint URL                       |
| `UPSTREAM_ACCESS_KEY_ID` / `--upstream-access-key-id`     | *(required)*         | Proxy's S3 credentials                |
| `UPSTREAM_SECRET_ACCESS_KEY` / `--upstream-secret-...`    | *(required)*         | Proxy's S3 credentials                |
| `UPSTREAM_REGION` / `--upstream-region`                   | `us-east-1`          | S3 region                             |
| `CLIENT_ACCESS_KEY_ID` / `--client-access-key-id`         | *(required)*         | Client auth credentials               |
| `CLIENT_SECRET_ACCESS_KEY` / `--client-secret-access-key` | *(required)*         | Client auth credentials               |
| `CACHE_ENABLED` / `--cache-enabled`                       | `true`               | Enable/disable caching                |
| `CACHE_DRY_RUN` / `--cache-dry-run`                       | `false`              | Dry-run verification mode (see below) |
| `CACHE_SHARDS` / `--cache-shards`                         | `16`                 | Number of cache shards                |
| `CACHE_MAX_ENTRIES` / `--cache-max-entries`               | `10000`              | Max cached objects                    |
| `CACHE_MAX_SIZE_BYTES` / `--cache-max-size-bytes`         | `1073741824` (1 GiB) | Max cache size                        |
| `CACHE_MAX_OBJECT_SIZE_BYTES` / `--cache-max-object-...`  | `10485760` (10 MiB)  | Skip caching above this               |
| `CACHE_TTL_SECONDS` / `--cache-ttl-seconds`               | `86400` (24h)        | TTL for cached entries                |
| `WORKER_THREADS` / `--worker-threads`                     | `4`                  | Tokio worker threads                  |
| `OTEL_GRPC_ENDPOINT_URL` / `--otel-grpc-endpoint-url`     | *(optional)*         | OpenTelemetry collector               |
| `PROMETHEUS_TEXTFILE_DIR` / `--prometheus-textfile-dir`   | *(optional)*         | Prometheus textfile collector dir     |

## Building

### Build from source

```bash
cargo build --release
```

The binary will be at `target/release/s3_cache`.

### Build Docker image

```bash
docker build -t s3-cache:latest .
```

## Usage

### Running locally

```bash
export UPSTREAM_ENDPOINT=http://localhost:9000
export UPSTREAM_ACCESS_KEY_ID=s3admin
export UPSTREAM_SECRET_ACCESS_KEY=s3admin
export CLIENT_ACCESS_KEY_ID=client
export CLIENT_SECRET_ACCESS_KEY=clientsecret

cargo run --release
```

### Running with Docker

```bash
docker run -p 8080:8080 \
  -e UPSTREAM_ENDPOINT=http://s3:9000 \
  -e UPSTREAM_ACCESS_KEY_ID=s3admin \
  -e UPSTREAM_SECRET_ACCESS_KEY=s3admin \
  -e CLIENT_ACCESS_KEY_ID=client \
  -e CLIENT_SECRET_ACCESS_KEY=clientsecret \
  s3-cache:latest
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

## Dry-Run Verification Mode

When `CACHE_DRY_RUN=true`, the cache is fully operational (populated, checked, evicted) but `GetObject` always returns the fresh upstream response. On every cache hit, the cached object is compared against the freshly fetched one. If they differ, a warning event is emitted with the cache key fields (`bucket`, `key`, `range`, `version_id`) and a `cache.mismatch` metric is incremented. This allows deploying the cache in production to verify correctness before switching to serving cached responses.

## Metrics

When `OTEL_GRPC_ENDPOINT_URL` is configured, the following metrics are exported:

| Metric                  | Type    | Description                                              |
| ----------------------- | ------- | -------------------------------------------------------- |
| `cache.hit`             | Counter | Number of cache hits                                     |
| `cache.miss`            | Counter | Number of cache misses                                   |
| `cache.invalidation`    | Counter | Number of cache invalidations                            |
| `cache.mismatch`        | Counter | Mismatches detected in dry-run mode                      |
| `cache.upstream_error`  | Counter | Upstream S3 errors                                       |
| `cache.buffering_error` | Counter | Buffering errors (object exceeded size limit mid-stream) |
| `cache.size_bytes`      | Gauge   | Current cache size in bytes                              |
| `cache.object_count`    | Gauge   | Current number of cached objects                         |

## Testing

```bash
# Run unit tests
cargo test

# Run with verbose output
cargo test -- --nocapture
```

## Cache Simulation

The `s3_cache_sim` binary drives the cache via direct S3 trait calls (no HTTP) with a simulated backend, allowing reproducible benchmarking of hit rates and latency under various workloads. Build it with the `sim` feature:

```bash
cargo build --release --bin s3_cache_sim --features sim
```

Run `--help` for all available flags:

```bash
cargo run --release --bin s3_cache_sim --features sim -- --help
```

See the corresponding [README.md](src/bin/s3_cache_sim/README.md) for more info.
