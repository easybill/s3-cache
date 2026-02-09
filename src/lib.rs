use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};

use aws_credential_types::Credentials;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
};
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

pub use config::Config;
pub use error::ApplicationError;

mod auth;
mod cache;
mod cache_entry;
mod cache_key;
mod config;
mod error;
mod proxy_service;
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
        "minio-cache-static",
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
    let cache = Arc::new(cache::AsyncS3Cache::new(
        NonZeroU64::new(config.cache_max_entries).expect("cache_max_entries must be > 0"),
        NonZeroUsize::new(config.cache_max_size_bytes).expect("cache_max_size_bytes must be > 0"),
        Duration::from_secs(config.cache_ttl_seconds),
        config.max_cacheable_object_size,
    ));

    // Build caching proxy
    let caching_proxy = proxy_service::CachingProxy::new(proxy, cache);

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
