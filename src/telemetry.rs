use std::{sync::LazyLock, time::Duration};

use opentelemetry::metrics::{Counter, Gauge};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Compression, WithExportConfig, WithTonicConfig};
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::{CARGO_CRATE_NAME, Config};

static RESOURCE: LazyLock<opentelemetry_sdk::Resource> = LazyLock::new(|| {
    opentelemetry_sdk::Resource::builder()
        .with_service_name(CARGO_CRATE_NAME)
        .build()
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
            tracing_subscriber::fmt().with_env_filter(filter).init();
        }
        Some(logs_provider) => {
            let otel_layer = OpenTelemetryTracingBridge::new(logs_provider);
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer())
                .with(otel_layer)
                .init();
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

static CACHE_HIT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.hit")
        .with_description("Number of cache hits")
        .build()
});

static CACHE_MISS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.miss")
        .with_description("Number of cache misses")
        .build()
});

static CACHE_INVALIDATION: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.invalidation")
        .with_description("Number of cache invalidations")
        .build()
});

static CACHE_SIZE_BYTES: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_gauge("cache.size_bytes")
        .with_description("Current cache size in bytes")
        .build()
});

static CACHE_ENTRY_COUNT: LazyLock<Gauge<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_gauge("cache.entry_count")
        .with_description("Current number of entries in cache")
        .build()
});

pub(crate) fn record_cache_hit() {
    CACHE_HIT.add(1, &[]);
}

pub(crate) fn record_cache_miss() {
    CACHE_MISS.add(1, &[]);
}

pub(crate) fn record_cache_invalidation() {
    CACHE_INVALIDATION.add(1, &[]);
}

static CACHE_MISMATCH: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.mismatch")
        .with_description("Number of cache mismatches detected in dryrun mode")
        .build()
});

pub(crate) fn record_cache_mismatch() {
    CACHE_MISMATCH.add(1, &[]);
}

static UPSTREAM_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.upstream_error")
        .with_description("Number of upstream S3 errors")
        .build()
});

static BUFFERING_ERROR: LazyLock<Counter<u64>> = LazyLock::new(|| {
    opentelemetry::global::meter(CARGO_CRATE_NAME)
        .u64_counter("cache.buffering_error")
        .with_description("Number of buffering errors (object exceeded size limit during streaming)")
        .build()
});

pub(crate) fn record_upstream_error() {
    UPSTREAM_ERROR.add(1, &[]);
}

pub(crate) fn record_buffering_error() {
    BUFFERING_ERROR.add(1, &[]);
}

pub(crate) fn record_cache_stats(entry_count: usize, size_bytes: usize) {
    CACHE_SIZE_BYTES.record(size_bytes as u64, &[]);
    CACHE_ENTRY_COUNT.record(entry_count as u64, &[]);
}
