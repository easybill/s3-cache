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

pub use self::async_cache::AsyncS3Cache;
pub use self::cache::{CacheKey, CachedObject, S3FifoCache};
pub use self::config::Config;
pub use self::error::ApplicationError;
pub use self::proxy_service::{CachingProxy, range_to_string};

mod async_cache;
mod auth;
mod cache;
mod config;
mod error;
pub mod proxy_service;
mod telemetry;

pub type Result<T> = std::result::Result<T, ApplicationError>;

static CARGO_CRATE_NAME: &str = env!("CARGO_CRATE_NAME");

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

    // Build caching proxy
    let caching_proxy = proxy_service::CachingProxy::from_aws_proxy(
        proxy,
        cache,
        config.cache_max_object_size_bytes,
    );

    // Build S3 service with auth
    let service = {
        let mut b = S3ServiceBuilder::new(caching_proxy);
        b.set_auth(auth::create_auth(&config));
        b.build()
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

    telemetry::shutdown_metrics(metrics_provider);
    telemetry::shutdown_logs(logs_provider);

    Ok(())
}
