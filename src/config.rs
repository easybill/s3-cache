use std::fmt::{Display, Formatter};
use std::net::SocketAddr;

use clap::Parser;

/// S3-compatible caching proxy.
///
/// All options can be set as CLI flags (`--upstream-endpoint`) or environment variables
/// (`UPSTREAM_ENDPOINT`). CLI flags take precedence over environment variables.
#[derive(Parser)]
#[command(version, about = "S3-compatible caching proxy")]
pub struct Config {
    /// Proxy listen address
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:8080")]
    pub listen_addr: SocketAddr,

    /// S3-compatible upstream endpoint URL
    #[arg(long, env = "UPSTREAM_ENDPOINT")]
    pub upstream_endpoint: String,

    /// Access key for upstream S3
    #[arg(long, env = "UPSTREAM_ACCESS_KEY_ID")]
    pub upstream_access_key_id: String,

    /// Secret key for upstream S3
    #[arg(long, env = "UPSTREAM_SECRET_ACCESS_KEY")]
    pub upstream_secret_access_key: String,

    /// AWS region for signing upstream requests.
    /// Must match the region your MinIO/S3 backend is configured with, or `us-east-1`
    /// (MinIO accepts `us-east-1` as a backward-compatibility alias for any region).
    #[arg(long, env = "UPSTREAM_REGION", default_value = "us-east-1")]
    pub upstream_region: String,

    /// Access key accepted from proxy clients
    #[arg(long, env = "CLIENT_ACCESS_KEY_ID")]
    pub client_access_key_id: String,

    /// Secret key accepted from proxy clients
    #[arg(long, env = "CLIENT_SECRET_ACCESS_KEY")]
    pub client_secret_access_key: String,

    /// Enable caching
    #[arg(long, env = "CACHE_ENABLED", default_value_t = true, action = clap::ArgAction::Set)]
    pub cache_enabled: bool,

    /// Dry-run mode: serve from cache but do not write new entries
    #[arg(long, env = "CACHE_DRY_RUN", default_value_t = false, action = clap::ArgAction::Set)]
    pub cache_dry_run: bool,

    /// Number of cache shards
    #[arg(long, env = "CACHE_SHARDS", default_value_t = 16)]
    pub cache_shards: usize,

    /// Maximum number of cache entries
    #[arg(long, env = "CACHE_MAX_ENTRIES", default_value_t = 10_000)]
    pub cache_max_entries: usize,

    /// Maximum cache size in bytes (default: 1 GB)
    #[arg(long, env = "CACHE_MAX_SIZE_BYTES", default_value_t = 1_073_741_824)]
    pub cache_max_size_bytes: usize,

    /// Maximum cacheable object size in bytes (default: 10 MB)
    #[arg(
        long,
        env = "CACHE_MAX_OBJECT_SIZE_BYTES",
        default_value_t = 10_485_760
    )]
    pub cache_max_object_size_bytes: usize,

    /// Cache time-to-live in seconds (default: 24 hours)
    #[arg(long, env = "CACHE_TTL_SECONDS", default_value_t = 86_400)]
    pub cache_ttl_seconds: usize,

    /// Tokio worker thread count
    #[arg(long, env = "WORKER_THREADS", default_value_t = 4)]
    pub worker_threads: usize,

    /// OpenTelemetry OTLP gRPC endpoint
    #[arg(long, env = "OTEL_GRPC_ENDPOINT_URL")]
    pub otel_grpc_endpoint_url: Option<String>,

    /// Export metrics via OTLP gRPC (requires otel_grpc_endpoint_url)
    #[arg(long, env = "OTEL_EXPORT_METRICS", default_value_t = false, action = clap::ArgAction::Set)]
    pub otel_export_metrics: bool,

    /// Export logs via OTLP gRPC (requires otel_grpc_endpoint_url)
    #[arg(long, env = "OTEL_EXPORT_LOGS", default_value_t = false, action = clap::ArgAction::Set)]
    pub otel_export_logs: bool,
}

impl Config {
    /// Validates cross-field constraints. Call this after parsing.
    ///
    /// # Panics
    ///
    /// Panics if any constraint is violated.
    pub fn validate(&self) {
        if self.cache_max_size_bytes < self.cache_max_object_size_bytes {
            panic!(
                "Invalid configuration: cache_max_size_bytes ({}) must be >= max_cacheable_object_size ({})",
                self.cache_max_size_bytes, self.cache_max_object_size_bytes
            );
        }

        if self.cache_ttl_seconds == 0 {
            panic!("Invalid configuration: cache_ttl_seconds must be greater than 0");
        }

        if self.cache_max_entries == 0 {
            panic!("Invalid configuration: cache_max_entries must be greater than 0");
        }

        if self.cache_shards == 0 {
            panic!("Invalid configuration: cache_shards must be greater than 0");
        }

        if self.worker_threads == 0 {
            panic!("Invalid configuration: worker_threads must be greater than 0");
        }

        if (self.otel_export_metrics || self.otel_export_logs)
            && self.otel_grpc_endpoint_url.is_none()
        {
            panic!(
                "Invalid configuration: otel_export_metrics and otel_export_logs require otel_grpc_endpoint_url to be set"
            );
        }
    }
}

impl Display for Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Config{{ listen_addr: {}, upstream_endpoint: {}, upstream_region: {}, \
             cache_max_entries: {}, cache_max_size_bytes: {}, cache_ttl_seconds: {}, \
             max_cacheable_object_size: {}, otel_grpc_endpoint_url: {:?}, \
             otel_export_metrics: {}, otel_export_logs: {}, cache_shards: {}, \
             cache_dry_run: {}, worker_threads: {} }}",
            self.listen_addr,
            self.upstream_endpoint,
            self.upstream_region,
            self.cache_max_entries,
            self.cache_max_size_bytes,
            self.cache_ttl_seconds,
            self.cache_max_object_size_bytes,
            self.otel_grpc_endpoint_url,
            self.otel_export_metrics,
            self.otel_export_logs,
            self.cache_shards,
            self.cache_dry_run,
            self.worker_threads,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    fn minimal_config() -> Config {
        Config {
            listen_addr: "0.0.0.0:8080".parse::<SocketAddr>().unwrap(),
            upstream_endpoint: "http://minio:9000".to_string(),
            upstream_access_key_id: "minioadmin".to_string(),
            upstream_secret_access_key: "minioadmin".to_string(),
            upstream_region: "us-east-1".to_string(),
            client_access_key_id: "testclient".to_string(),
            client_secret_access_key: "testclient".to_string(),
            cache_enabled: true,
            cache_dry_run: false,
            cache_shards: 16,
            cache_max_entries: 10_000,
            cache_max_size_bytes: 1_073_741_824,
            cache_max_object_size_bytes: 10_485_760,
            cache_ttl_seconds: 86_400,
            worker_threads: 4,
            otel_grpc_endpoint_url: None,
            otel_export_metrics: false,
            otel_export_logs: false,
        }
    }

    #[test]
    fn config_valid() {
        let config = minimal_config();
        config.validate();
        assert_eq!(config.cache_max_entries, 10_000);
        assert_eq!(config.cache_max_size_bytes, 1_073_741_824);
        assert_eq!(config.cache_max_object_size_bytes, 10_485_760);
    }

    #[test]
    #[should_panic(expected = "cache_max_size_bytes")]
    fn config_max_size_too_small() {
        let mut config = minimal_config();
        config.cache_max_size_bytes = 1000;
        config.cache_max_object_size_bytes = 2000;
        config.validate();
    }

    #[test]
    #[should_panic(expected = "cache_ttl_seconds")]
    fn config_zero_ttl() {
        let mut config = minimal_config();
        config.cache_ttl_seconds = 0;
        config.validate();
    }

    #[test]
    #[should_panic(expected = "cache_max_entries")]
    fn config_zero_max_entries() {
        let mut config = minimal_config();
        config.cache_max_entries = 0;
        config.validate();
    }

    #[test]
    #[should_panic(expected = "worker_threads")]
    fn config_zero_worker_threads() {
        let mut config = minimal_config();
        config.worker_threads = 0;
        config.validate();
    }

    #[test]
    #[should_panic(
        expected = "otel_export_metrics and otel_export_logs require otel_grpc_endpoint_url"
    )]
    fn config_otel_export_metrics_without_endpoint() {
        let mut config = minimal_config();
        config.otel_export_metrics = true;
        config.validate();
    }

    #[test]
    #[should_panic(
        expected = "otel_export_metrics and otel_export_logs require otel_grpc_endpoint_url"
    )]
    fn config_otel_export_logs_without_endpoint() {
        let mut config = minimal_config();
        config.otel_export_logs = true;
        config.validate();
    }

    #[test]
    fn config_otel_export_with_endpoint() {
        let mut config = minimal_config();
        config.otel_grpc_endpoint_url = Some("http://localhost:4317".to_string());
        config.otel_export_metrics = true;
        config.otel_export_logs = true;
        config.validate();
    }
}
