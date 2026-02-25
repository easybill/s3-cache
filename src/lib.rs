//! # s3-cache
//!
//! A high-performance caching proxy for S3-compatible object storage services.
//!
//! This library implements an S3-FIFO cache with sharded async support, designed to
//! reduce latency and bandwidth usage when accessing frequently-requested S3 objects.
//!
//! ## Features
//!
//! - **S3-FIFO Caching**: Uses the S3-FIFO eviction algorithm for improved cache hit rates
//! - **Async/Sharded**: Lock-free sharded cache for high concurrency workloads
//! - **Range Request Support**: Caches partial object reads (byte ranges)
//! - **Cache Invalidation**: Automatic invalidation on PUT/DELETE operations
//! - **Dry-run Mode**: Validate cache correctness without serving cached data
//! - **Telemetry**: OpenTelemetry metrics and Prometheus support
//!
//! ## Example
//!
//! ```no_run
//! use s3_cache::{Config, start_app};
//! use std::collections::HashMap;
//!
//! #[tokio::main]
//! async fn main() -> s3_cache::Result<()> {
//!     let mut env = HashMap::new();
//!     env.insert("UPSTREAM_ENDPOINT".to_string(), "http://s3.amazonaws.com".to_string());
//!     env.insert("UPSTREAM_ACCESS_KEY_ID".to_string(), "your-key".to_string());
//!     env.insert("UPSTREAM_SECRET_ACCESS_KEY".to_string(), "your-secret".to_string());
//!     env.insert("CLIENT_ACCESS_KEY_ID".to_string(), "client-key".to_string());
//!     env.insert("CLIENT_SECRET_ACCESS_KEY".to_string(), "client-secret".to_string());
//!
//!     let config = Config::from_env(&env);
//!     start_app(config).await
//! }
//! ```

use std::sync::Arc;
use std::time::Duration;

use aws_credential_types::Credentials;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
};
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

pub use self::config::Config;
pub use self::error::ApplicationError;
pub use self::fifo_cache::S3FifoCache;
pub use self::proxy_service::{CachingProxy, SharedCachingProxy, range_to_string};
pub use self::s3_cache::{AsyncS3Cache, CacheKey, CachedObject};

mod auth;
mod config;
mod error;
mod fifo_cache;
mod metrics_writer;
mod proxy_service;
mod s3_cache;
mod telemetry;

/// Result type alias using [`ApplicationError`] as the error type.
pub type Result<T> = std::result::Result<T, ApplicationError>;

static CARGO_CRATE_NAME: &str = env!("CARGO_CRATE_NAME");

/// Starts the S3 caching proxy server.
///
/// This function initializes telemetry, connects to the upstream S3 service,
/// creates the cache, and starts an HTTP server to handle S3 requests.
///
/// The server will run until it receives a SIGINT (Ctrl+C) signal, at which
/// point it will perform a graceful shutdown with a 10-second timeout.
///
/// # Arguments
///
/// * `config` - Configuration for the proxy server
///
/// # Returns
///
/// Returns `Ok(())` on successful shutdown, or an error if startup or
/// initialization fails.
///
/// # Example
///
/// ```no_run
/// use s3_cache::{Config, start_app};
/// use std::collections::HashMap;
///
/// # #[tokio::main]
/// # async fn main() -> s3_cache::Result<()> {
/// let mut env = HashMap::new();
/// env.insert("UPSTREAM_ENDPOINT".to_string(), "http://localhost:9000".to_string());
/// env.insert("UPSTREAM_ACCESS_KEY_ID".to_string(), "minioadmin".to_string());
/// env.insert("UPSTREAM_SECRET_ACCESS_KEY".to_string(), "minioadmin".to_string());
/// env.insert("CLIENT_ACCESS_KEY_ID".to_string(), "client".to_string());
/// env.insert("CLIENT_SECRET_ACCESS_KEY".to_string(), "secret".to_string());
///
/// let config = Config::from_env(&env);
/// start_app(config).await?;
/// # Ok(())
/// # }
/// ```
pub async fn start_app(config: Config) -> Result<()> {
    let (metrics_provider, logs_provider) = telemetry::initialize_telemetry(&config)?;

    info!("Starting {CARGO_CRATE_NAME} with {config}");

    // Build AWS SDK config for upstream MinIO
    let credentials = Credentials::new(
        &config.upstream_access_key_id,
        &config.upstream_secret_access_key,
        None,
        None,
        "s3-cache-static",
    );

    let sdk_config = aws_config::from_env()
        .endpoint_url(&config.upstream_endpoint)
        .region(aws_sdk_s3::config::Region::new(
            config.upstream_region.clone(),
        ))
        .credentials_provider(credentials)
        .load()
        .await;

    let s3_client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(true)
            .build(),
    );

    let proxy = s3s_aws::Proxy::from(s3_client);

    // Build cache
    let cache = config.cache_enabled.then(|| {
        Arc::new(AsyncS3Cache::new(
            config.cache_max_entries,
            config.cache_max_size_bytes,
            Duration::from_secs(config.cache_ttl_seconds as u64),
            config.cache_shards,
        ))
    });

    // Build caching proxy (wrapped for sharing with metrics writer)
    let caching_proxy =
        proxy_service::SharedCachingProxy::new(proxy_service::CachingProxy::from_aws_proxy(
            proxy,
            cache,
            config.cache_max_object_size_bytes,
            config.cache_dryrun,
        ));

    // Build S3 service with auth
    let service = {
        let mut b = S3ServiceBuilder::new(caching_proxy.clone());
        b.set_auth(auth::create_auth(&config));
        b.build()
    };

    // Start Prometheus metrics writer if configured
    let metrics_writer_handle = if let Some(textfile_dir) = config.prometheus_textfile_dir.clone() {
        info!(
            "Starting Prometheus textfile writer to {}/s3_cache.prom",
            textfile_dir
        );
        Some(tokio::spawn({
            let proxy_arc = caching_proxy.clone_arc();
            async move {
                if let Err(e) = metrics_writer::start_metrics_writer(textfile_dir, proxy_arc).await
                {
                    error!("Metrics writer failed: {:?}", e);
                }
            }
        }))
    } else {
        info!("Prometheus textfile writer disabled (PROMETHEUS_TEXTFILE_DIR not set)");
        None
    };

    // Start hyper server
    let listener = TcpListener::bind(config.listen_addr).await?;
    let http_server = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    info!("Listening on http://{}/", config.listen_addr);

    loop {
        let (socket, remote_addr) = tokio::select! {
            res = listener.accept() => {
                match res {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!("Error accepting connection: {err}");
                        continue;
                    }
                }
            }
            _ = ctrl_c.as_mut() => { break; }
        };

        debug!("Accepted connection from {remote_addr}");

        let conn = http_server.serve_connection(TokioIo::new(socket), service.clone());
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            if let Err(err) = conn.await {
                debug!("Connection error: {err}");
            }
        });
    }

    info!("Shutting down gracefully...");

    tokio::select! {
        () = graceful.shutdown() => {
            info!("Graceful shutdown complete");
        },
        () = tokio::time::sleep(Duration::from_secs(10)) => {
            info!("Graceful shutdown timed out after 10s, aborting");
        }
    }

    // Abort metrics writer background task
    if let Some(handle) = metrics_writer_handle {
        handle.abort();
        info!("Metrics writer task aborted");
    }

    telemetry::shutdown_metrics(metrics_provider);
    telemetry::shutdown_logs(logs_provider);

    Ok(())
}
