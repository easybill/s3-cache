use std::{sync::LazyLock, time::Duration};

use opentelemetry::metrics::{Counter, Gauge, Histogram};
use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Compression, WithExportConfig, WithTonicConfig};
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::{Config, CARGO_CRATE_NAME};

static HOSTNAME: LazyLock<String> = LazyLock::new(|| {
    std::env::vars()
        .find(|(key, _)| key == "HOSTNAME")
        .map(|(_, value)| value)
        .unwrap_or_else(|| String::from("unknown"))
});

static RESOURCE: LazyLock<opentelemetry_sdk::Resource> = LazyLock::new(|| {
    opentelemetry_sdk::Resource::builder()
        .with_service_name("s3_cache")
        .with_service_name(CARGO_CRATE_NAME)
        .build()
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
    Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
    Option<opentelemetry_sdk::logs::SdkLoggerProvider>,
)> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let otel_logs_endpoint = config
        .otel_grpc_endpoint_url
        .as_deref()
        .filter(|_| config.otel_export_logs);
    let logs_provider = init_logs(otel_logs_endpoint)?;

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

    let otel_metrics_endpoint = config
        .otel_grpc_endpoint_url
        .as_deref()
        .filter(|_| config.otel_export_metrics);
    let metrics_provider = init_metrics(otel_metrics_endpoint)?;

    register_service_info();

    Ok((metrics_provider, logs_provider))
}

// MARK: Service Info

fn register_service_info() {
    static OTEL_SERVICE_INFO: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_gauge("s3_cache.info")
            .with_description("Service build and runtime information")
            .build()
    });

    OTEL_SERVICE_INFO.record(
        1,
        &[
            KeyValue::new("version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("host.name", HOSTNAME.clone()),
        ],
    );
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
) -> crate::Result<Option<opentelemetry_sdk::metrics::SdkMeterProvider>> {
    let Some(otel_grpc_endpoint_url) = otel_grpc_endpoint_url else {
        info!("OTLP metrics export disabled");
        return Ok(None);
    };

    info!("OTLP metrics export enabled (endpoint: {otel_grpc_endpoint_url})");
    let otlp_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_compression(Compression::Gzip)
        .with_endpoint(otel_grpc_endpoint_url)
        .with_timeout(Duration::from_secs(5))
        .build()?;

    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_resource(RESOURCE.clone())
        .with_periodic_exporter(otlp_exporter)
        .build();

    opentelemetry::global::set_meter_provider(provider.clone());

    Ok(Some(provider))
}

pub(crate) fn shutdown_metrics(
    metric_provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
) {
    let Some(metric_provider) = metric_provider else {
        return;
    };

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

    static CACHE_HIT_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.hit_bytes_total")
            .with_description("Total bytes received from cache hits")
            .build()
    });

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

    static CACHE_MISS_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.miss_bytes_total")
            .with_description("Total bytes received from cache misses")
            .build()
    });

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

    static CACHE_EVICTION_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.eviction_bytes_total")
            .with_description("Total bytes evicted from cache")
            .build()
    });

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

    static CACHE_OVERSIZED_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.oversized_bytes_total")
            .with_description(
                "Total number of objects encountered exceeding the max cacheable size",
            )
            .build()
    });

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

    static CACHE_UNIQUE_REQUESTED_BYTES_TOTAL: LazyLock<Counter<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_counter("s3_cache.estimated_unique_bytes_total")
            .with_description("Estimated total bytes for unique keys accessed")
            .build()
    });

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

    CACHE_INVALIDATION_TOTAL.add(1, &[]);
}

// MARK: Service Errors

static SERVICE_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("service.error")
        .with_description("Internal errors the service can encounter")
        .build()
});

pub(crate) fn record_service_error(
    error_type: &'static str,
    component: &'static str,
    action: &'static str,
) {
    let attributes = &[
        KeyValue::new("error.type", error_type),
        KeyValue::new("service", CARGO_CRATE_NAME),
        KeyValue::new("component", component),
        KeyValue::new("action", action),
        KeyValue::new("host.name", HOSTNAME.clone()),
    ];
    SERVICE_ERROR.add(1, attributes);
}

// MARK: Upstream Errors

static UPSTREAM_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("upstream.error")
        .with_description("Errors received from the upstream service")
        .build()
});

pub(crate) fn record_upstream_error(
    error_type: &'static str,
    component: &'static str,
    action: &'static str,
) {
    let attributes = &[
        KeyValue::new("error.type", error_type),
        KeyValue::new("service", CARGO_CRATE_NAME),
        KeyValue::new("component", component),
        KeyValue::new("action", action),
        KeyValue::new("host.name", HOSTNAME.clone()),
    ];
    UPSTREAM_ERROR.add(1, attributes);
}

// MARK: Size Count

pub(crate) fn record_cache_size_count(size_count: usize) {
    static CACHE_SIZE_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .u64_gauge("s3_cache.size_count")
            .with_description("Current number of objects in cache")
            .build()
    });

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

    CACHE_SIZE_BYTES.record(size_bytes as u64, &[]);
}

// MARK: Request Durations

/// Attributes based on: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/#http-server
pub(crate) struct RequestDuration {
    pub(crate) version: &'static str,
    pub(crate) method: String,
    pub(crate) scheme: Option<String>,
    pub(crate) status_code: Option<u16>,
    pub(crate) duration: Duration,
}

static REQUEST_DURATION_BUCKETS: LazyLock<Vec<f64>> = LazyLock::new(|| {
    vec![
        1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
    ]
});

pub(crate) fn record_server_request_duration(data: RequestDuration, op_name: &str) {
    static REQUEST_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .f64_histogram("http.server.request.duration")
            .with_boundaries(REQUEST_DURATION_BUCKETS.to_vec())
            .with_description("Duration of the request in milliseconds")
            .with_unit("ms")
            .build()
    });

    let http_request_method: String = data.method;
    let network_protocol_version: String = data.version.to_owned();
    let rpc_method: String = op_name.to_owned();
    let url_scheme: String = data.scheme.unwrap_or_else(|| "http".to_owned());

    let mut attributes = vec![
        KeyValue::new("network.protocol.version", network_protocol_version),
        KeyValue::new("http.request.method", http_request_method),
        KeyValue::new("network.protocol.name", "http"),
        KeyValue::new("rpc.method", rpc_method),
        KeyValue::new("url.scheme", url_scheme),
    ];

    if let Some(status_code) = data.status_code {
        attributes.push(KeyValue::new(
            "http.response.status_code",
            i64::from(status_code),
        ));
    }

    REQUEST_DURATION_MS.record(1000.0 * data.duration.as_secs_f64(), &attributes);
}

pub(crate) fn record_client_request_duration(data: RequestDuration, op_name: &str) {
    static REQUEST_DURATION_MS: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .f64_histogram("http.client.request.duration")
            .with_boundaries(REQUEST_DURATION_BUCKETS.to_vec())
            .with_description("Duration of the request in milliseconds")
            .with_unit("ms")
            .build()
    });

    let http_request_method: String = data.method;
    let network_protocol_version: String = data.version.to_owned();
    let rpc_method: String = op_name.to_owned();
    let url_scheme: String = data.scheme.unwrap_or_else(|| "http".to_owned());

    let mut attributes = vec![
        KeyValue::new("network.protocol.version", network_protocol_version),
        KeyValue::new("http.request.method", http_request_method),
        KeyValue::new("network.protocol.name", "http"),
        KeyValue::new("rpc.method", rpc_method),
        KeyValue::new("url.scheme", url_scheme),
    ];

    if let Some(status_code) = data.status_code {
        attributes.push(KeyValue::new(
            "http.response.status_code",
            i64::from(status_code),
        ));
    }

    REQUEST_DURATION_MS.record(1000.0 * data.duration.as_secs_f64(), &attributes);
}

// MARK: Response Body Sizes

/// Attributes based on: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/#http-server
pub(crate) struct ResponseBodySize {
    pub(crate) version: &'static str,
    pub(crate) method: String,
    pub(crate) scheme: Option<String>,
    pub(crate) status_code: Option<u16>,
    pub(crate) size: u64,
}

// Exponential buckets: start 1024, factor 4.0, count 10 (1 KiB to ~256 GiB)
static RESPONSE_BODY_SIZE_BUCKETS: LazyLock<Vec<f64>> = LazyLock::new(|| {
    vec![
        1_024.0,
        4_096.0,
        16_384.0,
        65_536.0,
        262_144.0,
        1_048_576.0,
        4_194_304.0,
        16_777_216.0,
        67_108_864.0,
        268_435_456.0,
    ]
});

pub(crate) fn record_server_response_body_size(data: ResponseBodySize, op_name: &str) {
    static RESPONSE_BODY_SIZE_BYTES: LazyLock<Histogram<f64>> = LazyLock::new(|| {
        opentelemetry::global::meter(CARGO_CRATE_NAME)
            .f64_histogram("http.server.response.body.size")
            .with_boundaries(RESPONSE_BODY_SIZE_BUCKETS.to_vec())
            .with_description("Size of the response body in bytes")
            .with_unit("By")
            .build()
    });

    let http_request_method: String = data.method;
    let network_protocol_version: String = data.version.to_owned();
    let rpc_method: String = op_name.to_owned();
    let url_scheme: String = data.scheme.unwrap_or_else(|| "http".to_owned());

    let mut attributes = vec![
        KeyValue::new("network.protocol.version", network_protocol_version),
        KeyValue::new("http.request.method", http_request_method),
        KeyValue::new("network.protocol.name", "http"),
        KeyValue::new("rpc.method", rpc_method),
        KeyValue::new("url.scheme", url_scheme),
    ];

    if let Some(status_code) = data.status_code {
        attributes.push(KeyValue::new(
            "http.response.status_code",
            i64::from(status_code),
        ));
    }

    RESPONSE_BODY_SIZE_BYTES.record(data.size as f64, &attributes);
}
