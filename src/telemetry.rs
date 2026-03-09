use std::{
    sync::{
        LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use opentelemetry::metrics::{Counter, Gauge, Histogram};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Compression, WithExportConfig, WithTonicConfig};
use prometheus::{HistogramOpts, IntCounter, IntGauge, Registry};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{CARGO_CRATE_NAME, Config};

static RESOURCE: LazyLock<opentelemetry_sdk::Resource> = LazyLock::new(|| {
    opentelemetry_sdk::Resource::builder()
        .with_service_name(CARGO_CRATE_NAME)
        .build()
});

// Prometheus registry and metrics

pub(crate) static PROMETHEUS_REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    Registry::new_custom(Some("s3_cache".to_string()), None)
        .expect("Failed to create Prometheus registry")
});

const OBJECT_SIZE_BUCKETS: &[f64] = &[
    10_240.0,      // 10 KiB
    51_200.0,      // 50 KiB
    76_800.0,      // 75 KiB
    102_400.0,     // 100 KiB
    128_000.0,     // 125 KiB
    153_600.0,     // 150 KiB
    204_800.0,     // 200 KiB
    512_000.0,     // 500 KiB
    1_024_000.0,   // 1000 KiB
    2_048_000.0,   // 2000 KiB
    5_120_000.0,   // 5000 KiB
    10_240_000.0,  // 10000 KiB
    102_400_000.0, // 100000 KiB
];

// MARK: Cache Hit

static PROM_CACHE_HIT_BYTES_HISTOGRAM: LazyLock<prometheus::Histogram> = LazyLock::new(|| {
    let histogram = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "cache_hit_bytes_histogram",
            "Distribution of object sizes on cache hits",
        )
        .buckets(OBJECT_SIZE_BUCKETS.to_vec()),
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(histogram.clone()))
        .unwrap();
    histogram
});

static PROM_CACHE_HIT_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "cache_hit_bytes_total",
        "Total bytes received from cache hits",
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(counter.clone()))
        .unwrap();
    counter
});

// MARK: Cache Miss

static PROM_CACHE_MISS_BYTES_HISTOGRAM: LazyLock<prometheus::Histogram> = LazyLock::new(|| {
    let histogram = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "cache_miss_bytes_histogram",
            "Distribution of object sizes on cache misses",
        )
        .buckets(OBJECT_SIZE_BUCKETS.to_vec()),
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(histogram.clone()))
        .unwrap();
    histogram
});

static PROM_CACHE_MISS_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "cache_miss_bytes_total",
        "Total bytes received from cache misses",
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(counter.clone()))
        .unwrap();
    counter
});

// MARK: Cache Eviction

static PROM_CACHE_EVICTION_BYTES_HISTOGRAM: LazyLock<prometheus::Histogram> = LazyLock::new(|| {
    let histogram = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "cache_eviction_bytes_histogram",
            "Distribution of object sizes on cache evictions",
        )
        .buckets(OBJECT_SIZE_BUCKETS.to_vec()),
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(histogram.clone()))
        .unwrap();
    histogram
});

static PROM_CACHE_EVICTION_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "cache_eviction_bytes_total",
        "Total bytes evicted from cache",
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(counter.clone()))
        .unwrap();
    counter
});

// MARK: Cache Invalidation

static PROM_CACHE_INVALIDATION_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter =
        IntCounter::new("cache_invalidation_total", "Number of cache invalidations").unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(counter.clone()))
        .unwrap();
    counter
});

// MARK: Cache Oversized

static PROM_CACHE_OVERSIZED_REQUESTS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "cache_oversized_requests_total",
        "Total number of objects encountered exceeding the max cacheable size",
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(counter.clone()))
        .unwrap();
    counter
});

// MARK: Cache Size/Count

static PROM_CACHE_SIZE_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new("cache_size_bytes", "Current cache size in bytes").unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(gauge.clone()))
        .unwrap();
    gauge
});

static PROM_CACHE_SIZE_COUNT: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new("cache_size_count", "Current number of objects in cache").unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(gauge.clone()))
        .unwrap();
    gauge
});

// MARK: Cache Unique

static PROM_CACHE_ESTIMATED_UNIQUE_KEYS_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "cache_estimated_unique_keys_total",
        "Estimated number of unique keys accessed",
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(counter.clone()))
        .unwrap();
    counter
});

static PROM_CACHE_ESTIMATED_UNIQUE_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "cache_estimated_unique_bytes_total",
        "Estimated total bytes for unique keys accessed",
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(counter.clone()))
        .unwrap();
    counter
});

static PROM_CACHE_MEAN_OBJECT_SIZE_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "cache_mean_object_size_bytes",
        "Mean size of unique cached objects in bytes",
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(gauge.clone()))
        .unwrap();
    gauge
});

static PROM_CACHE_VARIANCE_OBJECT_SIZE_BYTES_SQUARED: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge: prometheus::core::GenericGauge<prometheus::core::AtomicI64> = IntGauge::new(
        "cache_variance_object_size_bytes_squared",
        "Population variance of unique cached object sizes in bytes squared",
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(gauge.clone()))
        .unwrap();
    gauge
});

static PROM_CACHE_ESTIMATED_MEDIAN_OBJECT_SIZE_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new(
        "cache_estimated_median_object_size_bytes",
        "Estimated median size of unique cached objects in bytes (P² algorithm)",
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(gauge.clone()))
        .unwrap();
    gauge
});

static PROM_REQUEST_DURATION_MS: LazyLock<prometheus::Histogram> = LazyLock::new(|| {
    let histogram = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "request_duration_ms",
            "Duration of get_object requests in milliseconds",
        )
        .buckets(vec![
            1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
        ]),
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(histogram.clone()))
        .unwrap();
    histogram
});

static PROM_RESPONSE_BODY_SIZE_BYTES: LazyLock<prometheus::Histogram> = LazyLock::new(|| {
    let histogram = prometheus::Histogram::with_opts(
        HistogramOpts::new(
            "response_body_size_bytes",
            "Size of get_object response bodies in bytes",
        )
        .buckets(prometheus::exponential_buckets(1024.0, 4.0, 10).unwrap()),
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(histogram.clone()))
        .unwrap();
    histogram
});

// MARK: Cache Error

static PROM_CACHE_MISMATCH_ERROR_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "cache_mismatch_error_total",
        "Number of cache mismatches detected in dry-run mode",
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(counter.clone()))
        .unwrap();
    counter
});

static PROM_UPSTREAM_ERROR: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter =
        IntCounter::new("cache_upstream_error_total", "Number of upstream S3 errors").unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(counter.clone()))
        .unwrap();
    counter
});

static PROM_BUFFERING_ERROR: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "cache_buffering_error_total",
        "Number of buffering errors (object exceeded size limit during streaming)",
    )
    .unwrap();
    PROMETHEUS_REGISTRY
        .register(Box::new(counter.clone()))
        .unwrap();
    counter
});

pub(crate) fn initialize_telemetry(
    config: &Config,
) -> crate::Result<(
    opentelemetry_sdk::metrics::SdkMeterProvider,
    Option<opentelemetry_sdk::logs::SdkLoggerProvider>,
)> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let logs_provider = init_logs(config.otel_grpc_endpoint_url.as_deref())?;

    match logs_provider.as_ref() {
        None => {
            tracing_subscriber::fmt()
                .with_env_filter(filter)
                .try_init()
                .ok();
        }
        Some(logs_provider) => {
            let otel_layer = OpenTelemetryTracingBridge::new(logs_provider);
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer())
                .with(otel_layer)
                .try_init()
                .ok();
        }
    }

    let metrics_provider = init_metrics(config.otel_grpc_endpoint_url.as_deref())?;

    Ok((metrics_provider, logs_provider))
}

fn init_logs(
    otel_grpc_endpoint_url: Option<&str>,
) -> crate::Result<Option<opentelemetry_sdk::logs::SdkLoggerProvider>> {
    let builder = opentelemetry_sdk::logs::SdkLoggerProvider::builder();

    let Some(otel_grpc_endpoint_url) = otel_grpc_endpoint_url else {
        return Ok(None);
    };

    let otlp_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_compression(Compression::Gzip)
        .with_endpoint(otel_grpc_endpoint_url)
        .with_timeout(Duration::from_secs(5))
        .build()?;

    let provider = builder.with_batch_exporter(otlp_exporter).build();

    Ok(Some(provider))
}

pub(crate) fn shutdown_logs(logs_provider: Option<opentelemetry_sdk::logs::SdkLoggerProvider>) {
    let Some(logs_provider) = logs_provider else {
        return;
    };

    if let Err(error) = logs_provider.shutdown() {
        error!("Error during logs shutdown: {error:?}");
    }
}

fn init_metrics(
    otel_grpc_endpoint_url: Option<&str>,
) -> crate::Result<opentelemetry_sdk::metrics::SdkMeterProvider> {
    let builder =
        opentelemetry_sdk::metrics::SdkMeterProvider::builder().with_resource(RESOURCE.clone());

    let provider = match otel_grpc_endpoint_url {
        None => {
            info!("opentelemetry_stdout initialized");
            builder.with_periodic_exporter(opentelemetry_stdout::MetricExporter::default())
        }
        Some(otel_grpc_endpoint_url) => {
            info!("opentelemetry_otlp initialized");
            let otlp_exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_compression(Compression::Gzip)
                .with_endpoint(otel_grpc_endpoint_url)
                .with_timeout(Duration::from_secs(5))
                .build()?;

            builder.with_periodic_exporter(otlp_exporter)
        }
    }
    .build();

    opentelemetry::global::set_meter_provider(provider.clone());

    Ok(provider)
}

pub(crate) fn shutdown_metrics(metric_provider: opentelemetry_sdk::metrics::SdkMeterProvider) {
    if let Err(error) = metric_provider.shutdown() {
        error!("Error during metric shutdown: {error:?}");
    }
}

// Cache metrics

// MARK: Cache Hit

static CACHE_HIT_BYTES_HISTOGRAM: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_histogram("cache.hit_bytes_histogram")
        .with_description("Distribution of object sizes on cache hits")
        .build()
});

static CACHE_HIT_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.hit_bytes_total")
        .with_description("Total bytes received from cache hits")
        .build()
});

// MARK: Cache Miss

static CACHE_MISS_BYTES_HISTOGRAM: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_histogram("cache.miss_bytes_histogram")
        .with_description("Distribution of object sizes on cache misses")
        .build()
});

static CACHE_MISS_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.miss_bytes_total")
        .with_description("Total bytes received from cache misses")
        .build()
});

// MARK: Cache Eviction

static CACHE_EVICTION_BYTES_HISTOGRAM: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_histogram("cache.eviction_bytes_histogram")
        .with_description("Distribution of object sizes on cache evictions")
        .build()
});

static CACHE_EVICTION_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.eviction_bytes_total")
        .with_description("Total bytes evicted from cache")
        .build()
});

// MARK: Cache Invalidation

static CACHE_INVALIDATION_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.invalidation_total")
        .with_description("Number of cache invalidations")
        .build()
});

// MARK: Cache Oversized

static CACHE_OVERSIZED_REQUESTS_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.oversized_requests_total")
        .with_description("Total number of objects encountered exceeding the max cacheable size")
        .build()
});

// MARK: Cache Mismatch

static CACHE_MISMATCH_ERROR_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.mismatch_error_total")
        .with_description("Number of cache mismatches detected in dry-run mode")
        .build()
});

// MARK: Cache Size/Count

static CACHE_SIZE_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_gauge("cache.size_bytes")
        .with_description("Current cache size in bytes")
        .build()
});

static CACHE_SIZE_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_gauge("cache.size_count")
        .with_description("Current number of objects in cache")
        .build()
});

// MARK: Cache Unique

static CACHE_ESTIMATED_UNIQUE_KEYS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.estimated_unique_keys_total")
        .with_description("Estimated number of unique keys accessed")
        .build()
});

static CACHE_ESTIMATED_UNIQUE_BYTES: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.estimated_unique_bytes_total")
        .with_description("Estimated total bytes for unique keys accessed")
        .build()
});

// MARK: Cache Errors

static UPSTREAM_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.upstream_error")
        .with_description("Number of upstream S3 errors")
        .build()
});

static BUFFERING_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.buffering_error")
        .with_description(
            "Number of buffering errors (object exceeded size limit during streaming)",
        )
        .build()
});

pub(crate) fn record_cache_hit(bytes: u64) {
    CACHE_HIT_BYTES_HISTOGRAM.record(bytes, &[]);
    CACHE_HIT_BYTES_TOTAL.add(bytes, &[]);
    PROM_CACHE_HIT_BYTES_HISTOGRAM.observe(bytes as f64);
    PROM_CACHE_HIT_BYTES_TOTAL.inc_by(bytes);
}

pub(crate) fn record_cache_miss(bytes: u64) {
    CACHE_MISS_BYTES_HISTOGRAM.record(bytes, &[]);
    CACHE_MISS_BYTES_TOTAL.add(bytes, &[]);
    PROM_CACHE_MISS_BYTES_HISTOGRAM.observe(bytes as f64);
    PROM_CACHE_MISS_BYTES_TOTAL.inc_by(bytes);
}

pub(crate) fn record_cache_eviction(bytes: u64) {
    CACHE_EVICTION_BYTES_HISTOGRAM.record(bytes, &[]);
    CACHE_EVICTION_BYTES_TOTAL.add(bytes, &[]);
    PROM_CACHE_EVICTION_BYTES_HISTOGRAM.observe(bytes as f64);
    PROM_CACHE_EVICTION_BYTES_TOTAL.inc_by(bytes);
}

pub(crate) fn record_cache_oversized() {
    CACHE_OVERSIZED_REQUESTS_TOTAL.add(1, &[]);
    PROM_CACHE_OVERSIZED_REQUESTS_TOTAL.inc();
}

pub(crate) fn record_cache_invalidation() {
    CACHE_INVALIDATION_TOTAL.add(1, &[]);
    PROM_CACHE_INVALIDATION_TOTAL.inc();
}

pub(crate) fn record_cache_mismatch() {
    CACHE_MISMATCH_ERROR_TOTAL.add(1, &[]);
    PROM_CACHE_MISMATCH_ERROR_TOTAL.inc();
}

pub(crate) fn record_upstream_error() {
    UPSTREAM_ERROR.add(1, &[]);
    PROM_UPSTREAM_ERROR.inc();
}

pub(crate) fn record_buffering_error() {
    BUFFERING_ERROR.add(1, &[]);
    PROM_BUFFERING_ERROR.inc();
}

pub(crate) fn record_cache_stats(object_count: usize, size_bytes: usize) {
    CACHE_SIZE_BYTES.record(size_bytes as u64, &[]);
    CACHE_SIZE_COUNT.record(object_count as u64, &[]);
    PROM_CACHE_SIZE_BYTES.set(size_bytes as i64);
    PROM_CACHE_SIZE_COUNT.set(object_count as i64);
}

static LAST_UNIQUE_KEYS: AtomicU64 = AtomicU64::new(0);
static LAST_UNIQUE_BYTES: AtomicU64 = AtomicU64::new(0);

pub(crate) fn record_counter_estimates(unique_count: usize, unique_bytes: usize) {
    let new_keys = unique_count as u64;
    let new_bytes = unique_bytes as u64;

    let prev_keys = LAST_UNIQUE_KEYS.swap(new_keys, Ordering::Relaxed);
    let prev_bytes = LAST_UNIQUE_BYTES.swap(new_bytes, Ordering::Relaxed);

    let delta_keys = new_keys.saturating_sub(prev_keys);
    let delta_bytes = new_bytes.saturating_sub(prev_bytes);

    CACHE_ESTIMATED_UNIQUE_KEYS.add(delta_keys, &[]);
    CACHE_ESTIMATED_UNIQUE_BYTES.add(delta_bytes, &[]);
    PROM_CACHE_ESTIMATED_UNIQUE_KEYS_TOTAL.inc_by(delta_keys);
    PROM_CACHE_ESTIMATED_UNIQUE_BYTES_TOTAL.inc_by(delta_bytes);
}

static CACHE_MEAN_OBJECT_SIZE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_gauge("cache.mean_object_size_bytes")
        .with_description("Mean size of unique cached objects in bytes")
        .build()
});

static CACHE_VARIANCE_OBJECT_SIZE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_gauge("cache.variance_object_size_bytes_squared")
        .with_description("Population variance of unique cached object sizes in bytes squared")
        .build()
});

static CACHE_ESTIMATED_MEDIAN_OBJECT_SIZE: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_gauge("cache.estimated_median_object_size_bytes")
        .with_description("Estimated median size of unique cached objects in bytes (P² algorithm)")
        .build()
});

static REQUEST_DURATION_MS: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_histogram("request.duration_ms")
        .with_description("Duration of get_object requests in milliseconds")
        .build()
});

static RESPONSE_BODY_SIZE: LazyLock<Histogram<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_histogram("response.body_size_bytes")
        .with_description("Size of get_object response bodies in bytes")
        .build()
});

pub(crate) fn record_request_duration(duration_ms: u64) {
    REQUEST_DURATION_MS.record(duration_ms, &[]);
    PROM_REQUEST_DURATION_MS.observe(duration_ms as f64);
}

pub(crate) fn record_response_body_size(bytes: u64) {
    RESPONSE_BODY_SIZE.record(bytes, &[]);
    PROM_RESPONSE_BODY_SIZE_BYTES.observe(bytes as f64);
}

pub(crate) fn record_object_size_distribution(
    mean: usize,
    variance: Option<usize>,
    estimated_median: usize,
) {
    CACHE_MEAN_OBJECT_SIZE.record(mean as u64, &[]);
    CACHE_VARIANCE_OBJECT_SIZE.record(variance.unwrap_or(0) as u64, &[]);
    CACHE_ESTIMATED_MEDIAN_OBJECT_SIZE.record(estimated_median as u64, &[]);
    PROM_CACHE_MEAN_OBJECT_SIZE_BYTES.set(mean as i64);
    PROM_CACHE_VARIANCE_OBJECT_SIZE_BYTES_SQUARED.set(variance.unwrap_or(0) as i64);
    PROM_CACHE_ESTIMATED_MEDIAN_OBJECT_SIZE_BYTES.set(estimated_median as i64);
}
