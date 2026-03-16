use std::collections::HashMap;
use std::{sync::LazyLock, time::Duration};

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Compression, WithExportConfig, WithTonicConfig};
use prometheus::{HistogramOpts, IntCounter, IntCounterVec, IntGauge, Opts, Registry};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{CARGO_CRATE_NAME, Config};

static RESOURCE: LazyLock<opentelemetry_sdk::Resource> = LazyLock::new(|| {
    opentelemetry_sdk::Resource::builder()
        .with_service_name("s3_cache")
        .with_service_name(CARGO_CRATE_NAME)
        .build()
});

// Prometheus registry and metrics

pub(crate) static PROMETHEUS_REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    let mut labels = HashMap::default();
    labels.insert("service_name".to_string(), "s3_cache".to_string());

    Registry::new_custom(None, Some(labels)).expect("Failed to create Prometheus registry")
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

const OVERSIZED_OBJECT_SIZE_BUCKETS: &[f64] = &[
    1_024_000.0,     // 1000 KiB
    2_048_000.0,     // 2000 KiB
    5_120_000.0,     // 5000 KiB
    10_240_000.0,    // 10000 KiB
    102_400_000.0,   // 100000 KiB
    204_800_000.0,   // 200000 KiB
    500_000_000.0,   // 500000 KiB
    1_000_000_000.0, // 1000000 KiB
];

const EVICTION_AGE_BUCKETS: &[f64] = &[
    60.0,         // 1 min
    300.0,        // 5 min
    600.0,        // 10 min
    1_800.0,      // 30 min
    3_600.0,      // 1 h
    7_200.0,      // 2 h
    14_400.0,     // 4 h
    28_800.0,     // 8 h
    86_400.0,     // 1 day
    172_800.0,    // 2 days
    604_800.0,    // 1 week
    1_209_600.0,  // 2 weeks
    2_592_000.0,  // 1 month
    5_184_000.0,  // 2 months
    7_776_000.0,  // 3 months
    15_552_000.0, // 6 months
    31_536_000.0, // 1 year
];

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

// MARK: Cache Hits

pub(crate) fn record_cache_hit(bytes: u64) {
    static CACHE_HIT_BYTES_HISTOGRAM: LazyLock<Histogram<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_histogram("s3_cache.hit_bytes_histogram")
            .with_boundaries(OBJECT_SIZE_BUCKETS.to_vec())
            .with_description("Distribution of object sizes on cache hits")
            .build()
    });

    static PROM_CACHE_HIT_BYTES_HISTOGRAM: LazyLock<prometheus::Histogram> = LazyLock::new(|| {
        let histogram = prometheus::Histogram::with_opts(
            HistogramOpts::new(
                "s3_cache_hit_bytes_histogram",
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

    static CACHE_HIT_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.hit_bytes_total")
            .with_description("Total bytes received from cache hits")
            .build()
    });

    static PROM_CACHE_HIT_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
        let counter = IntCounter::new(
            "s3_cache_hit_bytes_total",
            "Total bytes received from cache hits",
        )
        .unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(counter.clone()))
            .unwrap();
        counter
    });

    PROM_CACHE_HIT_BYTES_HISTOGRAM.observe(bytes as f64);
    PROM_CACHE_HIT_BYTES_TOTAL.inc_by(bytes);
    CACHE_HIT_BYTES_HISTOGRAM.record(bytes, &[]);
    CACHE_HIT_BYTES_TOTAL.add(bytes, &[]);
}

// MARK: Cache Misses

pub(crate) fn record_cache_miss(bytes: u64) {
    static CACHE_MISS_BYTES_HISTOGRAM: LazyLock<Histogram<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_histogram("s3_cache.miss_bytes_histogram")
            .with_boundaries(OBJECT_SIZE_BUCKETS.to_vec())
            .with_description("Distribution of object sizes on cache misses")
            .build()
    });

    static PROM_CACHE_MISS_BYTES_HISTOGRAM: LazyLock<prometheus::Histogram> = LazyLock::new(|| {
        let histogram = prometheus::Histogram::with_opts(
            HistogramOpts::new(
                "s3_cache_miss_bytes_histogram",
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

    static CACHE_MISS_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.miss_bytes_total")
            .with_description("Total bytes received from cache misses")
            .build()
    });

    static PROM_CACHE_MISS_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
        let counter = IntCounter::new(
            "s3_cache_miss_bytes_total",
            "Total bytes received from cache misses",
        )
        .unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(counter.clone()))
            .unwrap();
        counter
    });

    PROM_CACHE_MISS_BYTES_HISTOGRAM.observe(bytes as f64);
    PROM_CACHE_MISS_BYTES_TOTAL.inc_by(bytes);
    CACHE_MISS_BYTES_HISTOGRAM.record(bytes, &[]);
    CACHE_MISS_BYTES_TOTAL.add(bytes, &[]);
}

// MARK: Cache Evictions

pub(crate) fn record_cache_eviction(bytes: u64) {
    static CACHE_EVICTION_BYTES_HISTOGRAM: LazyLock<Histogram<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_histogram("s3_cache.eviction_bytes_histogram")
            .with_boundaries(OBJECT_SIZE_BUCKETS.to_vec())
            .with_description("Distribution of object sizes on cache evictions")
            .build()
    });

    static PROM_CACHE_EVICTION_BYTES_HISTOGRAM: LazyLock<prometheus::Histogram> =
        LazyLock::new(|| {
            let histogram = prometheus::Histogram::with_opts(
                HistogramOpts::new(
                    "s3_cache_eviction_bytes_histogram",
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

    static CACHE_EVICTION_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.eviction_bytes_total")
            .with_description("Total bytes evicted from cache")
            .build()
    });

    static PROM_CACHE_EVICTION_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
        let counter = IntCounter::new(
            "s3_cache_eviction_bytes_total",
            "Total bytes evicted from cache",
        )
        .unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(counter.clone()))
            .unwrap();
        counter
    });

    PROM_CACHE_EVICTION_BYTES_HISTOGRAM.observe(bytes as f64);
    PROM_CACHE_EVICTION_BYTES_TOTAL.inc_by(bytes);
    CACHE_EVICTION_BYTES_HISTOGRAM.record(bytes, &[]);
    CACHE_EVICTION_BYTES_TOTAL.add(bytes, &[]);
}

// MARK: Eviction Age

pub(crate) fn record_cache_eviction_age(age_secs: f64) {
    static CACHE_EVICTION_AGE_HISTOGRAM: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .f64_histogram("s3_cache.eviction_age_histogram")
            .with_boundaries(EVICTION_AGE_BUCKETS.to_vec())
            .with_description("Age of objects (in seconds) at the time of eviction, capped at TTL")
            .with_unit("s")
            .build()
    });

    static PROM_CACHE_EVICTION_AGE_HISTOGRAM: LazyLock<prometheus::Histogram> =
        LazyLock::new(|| {
            let histogram = prometheus::Histogram::with_opts(
                HistogramOpts::new(
                    "s3_cache_eviction_age_histogram",
                    "Age of objects (in seconds) at the time of eviction, capped at TTL",
                )
                .buckets(EVICTION_AGE_BUCKETS.to_vec()),
            )
            .unwrap();
            PROMETHEUS_REGISTRY
                .register(Box::new(histogram.clone()))
                .unwrap();
            histogram
        });

    PROM_CACHE_EVICTION_AGE_HISTOGRAM.observe(age_secs);
    CACHE_EVICTION_AGE_HISTOGRAM.record(age_secs, &[]);
}

// MARK: Oversized Objects

pub(crate) fn record_cache_oversized(bytes: u64) {
    static CACHE_OVERSIZED_BYTES_HISTOGRAM: LazyLock<Histogram<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_histogram("s3_cache.oversized_bytes_histogram")
            .with_boundaries(OVERSIZED_OBJECT_SIZE_BUCKETS.to_vec())
            .with_description("Distribution of object sizes that exceeded the max cacheable size")
            .build()
    });

    static PROM_CACHE_OVERSIZED_BYTES_HISTOGRAM: LazyLock<prometheus::Histogram> =
        LazyLock::new(|| {
            let histogram = prometheus::Histogram::with_opts(
                HistogramOpts::new(
                    "s3_cache_oversized_bytes_histogram",
                    "Distribution of object sizes that exceeded the max cacheable size",
                )
                .buckets(OVERSIZED_OBJECT_SIZE_BUCKETS.to_vec()),
            )
            .unwrap();
            PROMETHEUS_REGISTRY
                .register(Box::new(histogram.clone()))
                .unwrap();
            histogram
        });

    static CACHE_OVERSIZED_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.oversized_bytes_total")
            .with_description(
                "Total number of objects encountered exceeding the max cacheable size",
            )
            .build()
    });

    static PROM_CACHE_OVERSIZED_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
        let counter = IntCounter::new(
            "s3_cache_oversized_bytes_total",
            "Total number of objects encountered exceeding the max cacheable size",
        )
        .unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(counter.clone()))
            .unwrap();
        counter
    });

    PROM_CACHE_OVERSIZED_BYTES_HISTOGRAM.observe(bytes as f64);
    PROM_CACHE_OVERSIZED_BYTES_TOTAL.inc_by(bytes);
    CACHE_OVERSIZED_BYTES_HISTOGRAM.record(bytes, &[]);
    CACHE_OVERSIZED_BYTES_TOTAL.add(bytes, &[]);
}

// MARK: Unique Requests

pub(crate) fn record_unique_requested(bytes: u64) {
    static CACHE_UNIQUE_REQUESTED_BYTES_HISTOGRAM: LazyLock<Histogram<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_histogram("s3_cache.estimated_unique_bytes_histogram")
            .with_boundaries(OBJECT_SIZE_BUCKETS.to_vec())
            .with_description("Distribution of estimated unique object sizes")
            .build()
    });

    static PROM_CACHE_UNIQUE_REQUESTED_BYTES_HISTOGRAM: LazyLock<prometheus::Histogram> =
        LazyLock::new(|| {
            let histogram = prometheus::Histogram::with_opts(
                HistogramOpts::new(
                    "s3_cache_estimated_unique_bytes_histogram",
                    "Distribution of estimated unique object sizes",
                )
                .buckets(OBJECT_SIZE_BUCKETS.to_vec()),
            )
            .unwrap();
            PROMETHEUS_REGISTRY
                .register(Box::new(histogram.clone()))
                .unwrap();
            histogram
        });

    static CACHE_UNIQUE_REQUESTED_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.estimated_unique_bytes_total")
            .with_description("Estimated total bytes for unique keys accessed")
            .build()
    });

    static PROM_CACHE_UNIQUE_REQUESTED_BYTES_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
        let counter = IntCounter::new(
            "s3_cache_estimated_unique_bytes_total",
            "Estimated total bytes for unique keys accessed",
        )
        .unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(counter.clone()))
            .unwrap();
        counter
    });

    PROM_CACHE_UNIQUE_REQUESTED_BYTES_HISTOGRAM.observe(bytes as f64);
    PROM_CACHE_UNIQUE_REQUESTED_BYTES_TOTAL.inc_by(bytes);
    CACHE_UNIQUE_REQUESTED_BYTES_HISTOGRAM.record(bytes, &[]);
    CACHE_UNIQUE_REQUESTED_BYTES_TOTAL.add(bytes, &[]);
}

// MARK: Cache Invalidation

pub(crate) fn record_cache_invalidation() {
    static CACHE_INVALIDATION_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.invalidation_total")
            .with_description("Number of cache invalidations")
            .build()
    });

    static PROM_CACHE_INVALIDATION_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
        let counter = IntCounter::new(
            "s3_cache_invalidation_total",
            "Number of cache invalidations",
        )
        .unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(counter.clone()))
            .unwrap();
        counter
    });

    PROM_CACHE_INVALIDATION_TOTAL.inc();
    CACHE_INVALIDATION_TOTAL.add(1, &[]);
}

// MARK: Cache Mismatch

pub(crate) fn record_cache_mismatch() {
    static CACHE_MISMATCH_ERROR_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.mismatch_error_total")
            .with_description("Number of cache mismatches detected in dry-run mode")
            .build()
    });

    static PROM_CACHE_MISMATCH_ERROR_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
        let counter = IntCounter::new(
            "s3_cache_mismatch_error_total",
            "Number of cache mismatches detected in dry-run mode",
        )
        .unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(counter.clone()))
            .unwrap();
        counter
    });

    PROM_CACHE_MISMATCH_ERROR_TOTAL.inc();
    CACHE_MISMATCH_ERROR_TOTAL.add(1, &[]);
}

// MARK: Upstream Errors

pub(crate) fn record_upstream_error() {
    static UPSTREAM_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.upstream_error")
            .with_description("Number of upstream S3 errors")
            .build()
    });

    static PROM_UPSTREAM_ERROR: LazyLock<IntCounter> = LazyLock::new(|| {
        let counter = IntCounter::new(
            "s3_cache_upstream_error_total",
            "Number of upstream S3 errors",
        )
        .unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(counter.clone()))
            .unwrap();
        counter
    });

    PROM_UPSTREAM_ERROR.inc();
    UPSTREAM_ERROR.add(1, &[]);
}

// MARK: Buffering Errors

pub(crate) fn record_buffering_error() {
    static BUFFERING_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.buffering_error")
            .with_description(
                "Number of buffering errors (object exceeded size limit during streaming)",
            )
            .build()
    });

    static PROM_BUFFERING_ERROR: LazyLock<IntCounter> = LazyLock::new(|| {
        let counter = IntCounter::new(
            "s3_cache_buffering_error_total",
            "Number of buffering errors (object exceeded size limit during streaming)",
        )
        .unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(counter.clone()))
            .unwrap();
        counter
    });

    PROM_BUFFERING_ERROR.inc();
    BUFFERING_ERROR.add(1, &[]);
}

// MARK: Size Count

pub(crate) fn record_cache_size_count(size_count: usize) {
    static CACHE_SIZE_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_gauge("s3_cache.size_count")
            .with_description("Current number of objects in cache")
            .build()
    });

    static PROM_CACHE_SIZE_COUNT: LazyLock<IntGauge> = LazyLock::new(|| {
        let gauge =
            IntGauge::new("s3_cache_size_count", "Current number of objects in cache").unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(gauge.clone()))
            .unwrap();
        gauge
    });

    PROM_CACHE_SIZE_COUNT.set(size_count as i64);
    CACHE_SIZE_COUNT.record(size_count as u64, &[]);
}

// MARK: Size Bytes

pub(crate) fn record_cache_size_bytes(size_bytes: usize) {
    static CACHE_SIZE_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_gauge("s3_cache.size_bytes")
            .with_description("Current cache size in bytes")
            .build()
    });

    static PROM_CACHE_SIZE_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
        let gauge = IntGauge::new("s3_cache_size_bytes", "Current cache size in bytes").unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(gauge.clone()))
            .unwrap();
        gauge
    });

    PROM_CACHE_SIZE_BYTES.set(size_bytes as i64);
    CACHE_SIZE_BYTES.record(size_bytes as u64, &[]);
}

// MARK: Request Durations

/// Attributes based on: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/#http-server
pub(crate) struct RequestDuration {
    pub(crate) version: &'static str,
    pub(crate) method: String,
    pub(crate) scheme: Option<String>,
    pub(crate) status_code: u16,
    pub(crate) duration: Duration,
}

pub(crate) fn record_request_duration(data: RequestDuration) {
    static REQUEST_DURATION_BUCKETS: LazyLock<Vec<f64>> = LazyLock::new(|| {
        vec![
            1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
        ]
    });

    static REQUEST_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .f64_histogram("http.server.request.duration")
            .with_boundaries(REQUEST_DURATION_BUCKETS.to_vec())
            .with_description("Duration of the request in milliseconds")
            .with_unit("ms")
            .build()
    });

    static PROM_REQUEST_DURATION_MS: LazyLock<prometheus::Histogram> = LazyLock::new(|| {
        let histogram = prometheus::Histogram::with_opts(
            HistogramOpts::new(
                "http_server_request_duration",
                "Duration of get_object requests in milliseconds",
            )
            .buckets(REQUEST_DURATION_BUCKETS.to_vec()),
        )
        .unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(histogram.clone()))
            .unwrap();
        histogram
    });

    let mut attributes = vec![
        KeyValue::new("network.protocol.version", data.version),
        KeyValue::new("http.request.method", data.method),
        KeyValue::new("network.protocol.name", "http"),
        KeyValue::new("http.response.status_code", i64::from(data.status_code)),
    ];

    match data.scheme {
        None => {
            attributes.push(KeyValue::new("url.scheme", "http"));
        }
        Some(scheme) => {
            attributes.push(KeyValue::new("url.scheme", scheme));
        }
    }

    let milliseconds = 1000.0 * data.duration.as_secs_f64();

    PROM_REQUEST_DURATION_MS.observe(milliseconds);
    REQUEST_DURATION_MS.record(milliseconds, &attributes);
}

// MARK: - Endpoint Calls

pub(crate) fn record_endpoint_call(method: &str) {
    static ENDPOINT_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.endpoint_call_total")
            .with_description("Number of S3 endpoint method calls")
            .build()
    });

    static PROM_ENDPOINT_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
        let counter = IntCounterVec::new(
            Opts::new(
                "s3_cache_endpoint_call_total",
                "Number of S3 endpoint method calls",
            ),
            &["rpc_method"],
        )
        .unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(counter.clone()))
            .unwrap();
        counter
    });

    PROM_ENDPOINT_TOTAL.with_label_values(&[method]).inc();
    ENDPOINT_TOTAL.add(1, &[KeyValue::new("rpc.method", method.to_owned())]);
}

// MARK: Response Body Sizes

/// Attributes based on: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/#http-server
pub(crate) struct ResponseBodySize {
    pub(crate) version: &'static str,
    pub(crate) method: String,
    pub(crate) scheme: Option<String>,
    pub(crate) status_code: u16,
    pub(crate) size: u64,
}

pub(crate) fn record_response_body_size(data: ResponseBodySize) {
    static RESPONSE_BODY_SIZE_BUCKETS: LazyLock<Vec<f64>> =
        LazyLock::new(|| prometheus::exponential_buckets(1024.0, 4.0, 10).unwrap());

    static RESPONSE_BODY_SIZE_BYTES: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .f64_histogram("http.server.response.body.size")
            .with_boundaries(RESPONSE_BODY_SIZE_BUCKETS.to_vec())
            .with_description("Size of the response body in bytes")
            .with_unit("By")
            .build()
    });

    static PROM_RESPONSE_BODY_SIZE_BYTES: LazyLock<prometheus::Histogram> = LazyLock::new(|| {
        let histogram = prometheus::Histogram::with_opts(
            HistogramOpts::new(
                "http_server_response_body_size",
                "Size of get_object response bodies in bytes",
            )
            .buckets(RESPONSE_BODY_SIZE_BUCKETS.to_vec()),
        )
        .unwrap();
        PROMETHEUS_REGISTRY
            .register(Box::new(histogram.clone()))
            .unwrap();
        histogram
    });

    let mut attributes = vec![
        KeyValue::new("network.protocol.version", data.version),
        KeyValue::new("http.request.method", data.method),
        KeyValue::new("network.protocol.name", "http"),
        KeyValue::new("http.response.status_code", i64::from(data.status_code)),
    ];

    match data.scheme {
        None => {
            attributes.push(KeyValue::new("url.scheme", "http"));
        }
        Some(scheme) => {
            attributes.push(KeyValue::new("url.scheme", scheme));
        }
    }

    let bytes = data.size as f64;

    PROM_RESPONSE_BODY_SIZE_BYTES.observe(bytes);
    RESPONSE_BODY_SIZE_BYTES.record(bytes, &attributes);
}
