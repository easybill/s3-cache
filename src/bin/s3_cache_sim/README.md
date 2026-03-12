# S3 Cache Simulation

The `s3_cache_sim` binary drives the cache via direct S3 trait calls (no HTTP) with a simulated backend, allowing reproducible benchmarking of hit rates and latency under various workloads. Build it with the `sim` feature:

```bash
cargo build --release --bin s3_cache_sim --features sim
```

Run `--help` for all available flags:

```bash
cargo run --release --bin s3_cache_sim --features sim -- --help
```

## Scenarios

**1. No cache (baseline)** — Bypass the cache entirely with `--no-cache` to establish a raw backend latency baseline. Compare against cached runs to measure the actual speedup.

```bash
cargo run --release --bin s3_cache_sim --features sim -- \
  --no-cache
```

**2. Zipf baseline** — Realistic workload with moderate skew (s=1.0). Compare against scenario 1 to see the cache's effect.

```bash
cargo run --release --bin s3_cache_sim --features sim
```

**3. Heavy skew** — High Zipf exponent (s=1.5) concentrates requests on a small hot set, demonstrating near-optimal hit rates.

```bash
cargo run --release --bin s3_cache_sim --features sim -- \
  --zipf-exponent 1.5
```

**4. Scan resistance** — Sequential scan over 10K objects. S3-FIFO should resist pollution, yielding ~0% hit rate (which is correct behavior — an LRU would thrash and waste memory).

```bash
cargo run --release --bin s3_cache_sim --features sim -- \
  --pattern scan
```

**5. One-hit-wonders** — 30% of requests go to unique keys never seen again. Tests the cache's ability to avoid wasting capacity on transient objects.

```bash
cargo run --release --bin s3_cache_sim --features sim -- \
  --one-hit-wonder-ratio 0.3
```

**6. Tiny cache under pressure** — Only 100 cache entries and 1MB budget. Measures how gracefully hit rate degrades when the cache is severely undersized.

```bash
cargo run --release --bin s3_cache_sim --features sim -- \
  --cache-max-entries 100 --cache-max-size 1000000
```

**7. Size-constrained eviction** — Large objects (50KB-200KB) with the default 10MB cache budget. The byte limit kicks in well before the entry limit, forcing size-aware eviction.

```bash
cargo run --release --bin s3_cache_sim --features sim -- \
  --min-object-size 50000 --max-object-size 200000
```

**8. Uniform random** — No access skew. With the cache holding 10% of the object set, hit rate reflects pure capacity ratio. Measures baseline behavior without a hot set.

```bash
cargo run --release --bin s3_cache_sim --features sim -- \
  --pattern uniform
```

**9. High-latency backend** — Simulates a slow upstream (50ms RTT, 10MB/s throughput). Cache hits become dramatically faster than misses, making the latency p50/p99 split clearly visible. Uses fewer requests to keep runtime reasonable.

```bash
cargo run --release --bin s3_cache_sim --features sim -- \
  --latency-ms 50 --throughput-bytes-per-sec 10000000 \
  --num-requests 10000
```
