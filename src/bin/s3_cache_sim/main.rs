mod simulated_backend;
mod workload;

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use http::{HeaderMap, Method, Uri};
use s3s::dto::*;
use s3s::{S3, S3Request};

use s3_cache::{S3Cache, CachingProxy};

use simulated_backend::SimulatedBackend;
use workload::Pattern;

#[derive(Parser, Debug)]
#[command(name = "s3_cache_sim", about = "S3-FIFO cache simulator")]
struct Args {
    /// Number of objects in simulated store
    #[arg(long, default_value_t = 10000)]
    num_objects: usize,

    /// Minimum object size in bytes
    #[arg(long, default_value_t = 1024)]
    min_object_size: usize,

    /// Maximum object size in bytes
    #[arg(long, default_value_t = 65536)]
    max_object_size: usize,

    /// Base round-trip latency per backend operation (ms)
    #[arg(long, default_value_t = 5)]
    latency_ms: u64,

    /// Bandwidth limit in bytes/sec (0 = unlimited)
    #[arg(long, default_value_t = 100_000_000)]
    throughput_bytes_per_sec: u64,

    /// Cache entry limit
    #[arg(long, default_value_t = 1000)]
    cache_max_entries: usize,

    /// Cache byte limit
    #[arg(long, default_value_t = 10_000_000)]
    cache_max_size: usize,

    /// Cache TTL in seconds
    #[arg(long, default_value_t = 300)]
    cache_ttl_secs: u64,

    /// Number of cache shards
    #[arg(long, default_value_t = 4)]
    cache_shards: usize,

    /// Max single object size for caching
    #[arg(long, default_value_t = 1_000_000)]
    max_cacheable_size: usize,

    /// Access pattern
    #[arg(long, value_enum, default_value_t = Pattern::Zipf)]
    pattern: Pattern,

    /// Zipf skew parameter
    #[arg(long, default_value_t = 1.0)]
    zipf_exponent: f64,

    /// Fraction of requests to unique-once objects [0.0, 1.0)
    #[arg(long, default_value_t = 0.0)]
    one_hit_wonder_ratio: f64,

    /// Total requests to issue
    #[arg(long, default_value_t = 100_000)]
    num_requests: usize,

    /// Parallel tokio tasks
    #[arg(long, default_value_t = 16)]
    concurrency: usize,

    /// Print progress every N requests (0 = off)
    #[arg(long, default_value_t = 10_000)]
    progress_interval: usize,

    /// Disable caching (requests go directly to backend)
    #[arg(long, default_value_t = false)]
    no_cache: bool,

    /// RNG seed for reproducibility
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

fn build_get_request(bucket: &str, key: &str) -> S3Request<GetObjectInput> {
    S3Request {
        input: GetObjectInput {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        },
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

fn index_to_key(idx: usize) -> String {
    format!("obj-{idx}")
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Determine default object size for one-hit-wonders (median of range)
    let default_ohw_size = if args.one_hit_wonder_ratio > 0.0 {
        Some((args.min_object_size + args.max_object_size) / 2)
    } else {
        None
    };

    // Build backend
    let backend = SimulatedBackend::new(
        Duration::from_millis(args.latency_ms),
        args.throughput_bytes_per_sec,
        default_ohw_size,
    );

    // Pre-populate
    backend
        .populate(
            args.num_objects,
            args.min_object_size,
            args.max_object_size,
            args.seed,
        )
        .await;

    // Build cache + proxy (or direct backend if --no-cache)
    let proxy: Option<Arc<CachingProxy<_>>> = if args.no_cache {
        None
    } else {
        let cache = Arc::new(S3Cache::new(
            args.cache_max_entries,
            args.cache_max_size,
            Duration::from_secs(args.cache_ttl_secs),
            args.cache_shards,
        ));
        Some(Arc::new(CachingProxy::new(
            backend.clone(),
            Some(cache),
            args.max_cacheable_size,
            false,
        )))
    };

    let service: Arc<dyn S3 + Send + Sync> = if let Some(ref p) = proxy {
        p.clone()
    } else {
        Arc::new(backend.clone())
    };

    // Generate workload
    let workload = workload::generate_workload(
        args.pattern,
        args.num_objects,
        args.num_requests,
        args.zipf_exponent,
        args.one_hit_wonder_ratio,
        args.seed,
    );

    // Print config
    eprintln!("=== S3-FIFO Cache Simulation ===");
    eprintln!(
        "Objects: {} (sizes: {}B - {}B)",
        args.num_objects, args.min_object_size, args.max_object_size
    );
    if args.no_cache {
        eprintln!("Cache: disabled (--no-cache)");
    } else {
        eprintln!(
            "Cache: {} entries, {} bytes, TTL={}s, {} shards",
            args.cache_max_entries, args.cache_max_size, args.cache_ttl_secs, args.cache_shards
        );
    }
    eprintln!(
        "Workload: {:?} (s={}), {} requests, concurrency={}",
        args.pattern, args.zipf_exponent, args.num_requests, args.concurrency
    );
    eprintln!(
        "Backend: latency={}ms, throughput={} B/s",
        args.latency_ms, args.throughput_bytes_per_sec
    );
    if args.one_hit_wonder_ratio > 0.0 {
        eprintln!(
            "One-hit-wonder ratio: {:.1}%",
            args.one_hit_wonder_ratio * 100.0
        );
    }
    eprintln!();

    // Partition workload across tasks
    let chunk_size = (workload.len() + args.concurrency - 1) / args.concurrency;
    let chunks: Vec<Vec<usize>> = workload
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect();

    let completed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let progress_interval = args.progress_interval;
    let total_requests = args.num_requests;

    let start = Instant::now();

    let mut handles = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let service = service.clone();
        let completed = completed.clone();
        let errors = errors.clone();

        handles.push(tokio::spawn(async move {
            let mut latencies = Vec::with_capacity(chunk.len());

            for idx in chunk {
                let key = index_to_key(idx);
                let req = build_get_request("sim", &key);

                let req_start = Instant::now();
                let result = service.get_object(req).await;
                let elapsed = req_start.elapsed();

                latencies.push(elapsed);

                if result.is_err() {
                    errors.fetch_add(1, Ordering::Relaxed);
                }

                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if progress_interval > 0 && done % (progress_interval as u64) == 0 {
                    let wall = start.elapsed().as_secs_f64();
                    let rps = done as f64 / wall;
                    eprintln!(
                        "[{wall:.1}s] {done}/{total_requests} requests ({rps:.0} req/s)"
                    );
                }
            }

            latencies
        }));
    }

    // Collect results
    let mut all_latencies: Vec<Duration> = Vec::with_capacity(total_requests);
    for handle in handles {
        let latencies = handle.await.expect("task panicked");
        all_latencies.extend(latencies);
    }

    let total_duration = start.elapsed();
    let total_errors = errors.load(Ordering::Relaxed);
    let backend_gets = backend.get_count();
    let total = all_latencies.len() as u64;
    let hits = total - backend_gets;
    let hit_rate = if total > 0 {
        hits as f64 / total as f64 * 100.0
    } else {
        0.0
    };

    // Compute latency stats
    all_latencies.sort();
    let p50 = percentile(&all_latencies, 50.0);
    let p99 = percentile(&all_latencies, 99.0);
    let mean = if all_latencies.is_empty() {
        Duration::ZERO
    } else {
        all_latencies.iter().sum::<Duration>() / all_latencies.len() as u32
    };

    let throughput = if total_duration.as_secs_f64() > 0.0 {
        total as f64 / total_duration.as_secs_f64()
    } else {
        0.0
    };

    eprintln!();
    eprintln!("=== Results ===");
    eprintln!("Total requests:  {total}");
    eprintln!("Hits:            {hits} ({hit_rate:.1}%)");
    eprintln!(
        "Misses:          {backend_gets} ({:.1}%)",
        100.0 - hit_rate
    );
    eprintln!("Errors:          {total_errors}");
    eprintln!("Duration:        {:.2}s", total_duration.as_secs_f64());
    eprintln!("Throughput:      {throughput:.0} req/s");
    eprintln!("Latency p50:     {:.2}ms", p50.as_secs_f64() * 1000.0);
    eprintln!("Latency p99:     {:.2}ms", p99.as_secs_f64() * 1000.0);
    eprintln!("Latency mean:    {:.2}ms", mean.as_secs_f64() * 1000.0);

    // Calculate actual unique objects and bytes from workload
    let unique_indices: HashSet<usize> = workload.iter().copied().collect();
    let actual_unique_count = unique_indices.len();

    let mut actual_total_bytes: usize = 0;
    for idx in &unique_indices {
        let key = index_to_key(*idx);
        if let Some(size) = backend.get_object_size("sim", &key).await {
            actual_total_bytes += size;
        }
    }

    // Print cardinality estimation comparison if using proxy
    if let Some(ref proxy) = proxy {
        let estimated_count = proxy.estimated_unique_count();
        let estimated_bytes = proxy.estimated_unique_bytes();

        eprintln!();
        eprintln!("=== Cardinality Estimation (HyperLogLog) ===");
        eprintln!("Actual unique objects:     {actual_unique_count}");
        eprintln!("Estimated unique objects:  {estimated_count}");

        let count_error = if actual_unique_count > 0 {
            let diff = (estimated_count as isize - actual_unique_count as isize).abs();
            (diff as f64 / actual_unique_count as f64) * 100.0
        } else {
            0.0
        };
        eprintln!("Count error:               {count_error:.2}%");

        eprintln!();
        eprintln!("Actual total bytes:        {actual_total_bytes}");
        eprintln!("Estimated total bytes:     {estimated_bytes}");

        let bytes_error = if actual_total_bytes > 0 {
            let diff = (estimated_bytes as isize - actual_total_bytes as isize).abs();
            (diff as f64 / actual_total_bytes as f64) * 100.0
        } else {
            0.0
        };
        eprintln!("Bytes error:               {bytes_error:.2}%");
    }
}

fn percentile(sorted: &[Duration], pct: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((pct / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
