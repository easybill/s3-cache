#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::Client as S3Client;
use testcontainers::ContainerAsync;
use testcontainers_modules::minio::MinIO;
use tokio::sync::OnceCell;

// MARK: - Shared MinIO

pub struct MinioHandle {
    /// Kept alive until process exit; dropping it stops the container.
    _container: ContainerAsync<MinIO>,
    pub api_port: u16,
}

static MINIO_HANDLE: OnceCell<MinioHandle> = OnceCell::const_new();

/// Returns the shared MinIO instance, starting the container on first call.
pub async fn shared_minio() -> &'static MinioHandle {
    MINIO_HANDLE
        .get_or_init(|| async {
            use testcontainers::runners::AsyncRunner;

            let container = MinIO::default()
                .start()
                .await
                .expect("failed to start MinIO container — is Docker running?");

            let api_port = container
                .get_host_port_ipv4(9000)
                .await
                .expect("failed to get MinIO host port");

            MinioHandle {
                _container: container,
                api_port,
            }
        })
        .await
}

// MARK: - TestProxy

pub struct TestProxy {
    pub addr: SocketAddr,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    _handle: tokio::task::JoinHandle<()>,
}

impl TestProxy {
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for TestProxy {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// MARK: - Credentials

pub const CLIENT_KEY_ID: &str = "testclient";
pub const CLIENT_SECRET: &str = "testsecret";

// MARK: - start_proxy

pub async fn start_proxy(minio_api_port: u16) -> TestProxy {
    let config = s3_cache::Config {
        listen_addr: "127.0.0.1:0".parse().unwrap(), // Port 0: OS assigns a free port
        upstream_endpoint: format!("http://127.0.0.1:{minio_api_port}"),
        upstream_access_key_id: "minioadmin".to_string(),
        upstream_secret_access_key: "minioadmin".to_string(),
        upstream_region: "us-east-1".to_string(),
        client_access_key_id: CLIENT_KEY_ID.to_string(),
        client_secret_access_key: CLIENT_SECRET.to_string(),
        cache_enabled: true,
        cache_dry_run: false,
        cache_shards: 16,
        // Small limits for test speed; 1 MB max object size enables the large-object test
        cache_max_entries: 100,
        cache_max_size_bytes: 104_857_600,      // 100 MB
        cache_max_object_size_bytes: 1_048_576, // 1 MB
        cache_ttl_seconds: 86_400,
        worker_threads: 4,
        otel_grpc_endpoint_url: None, // no network side effects in tests
        otel_export_logs: false,
        otel_export_metrics: false,
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (addr_tx, addr_rx) = tokio::sync::oneshot::channel::<SocketAddr>();

    let handle = tokio::spawn(async move {
        let shutdown = async move {
            // Shut down when the signal arrives, or immediately if sender was dropped.
            let _ = shutdown_rx.await;
        };
        if let Err(e) = s3_cache::start_app_with_shutdown(config, shutdown, addr_tx).await {
            eprintln!("Proxy server error in test: {e:?}");
        }
    });

    let addr = addr_rx
        .await
        .expect("proxy did not send bound address — startup likely failed");

    TestProxy {
        addr,
        shutdown_tx: Some(shutdown_tx),
        _handle: handle,
    }
}

// MARK: - Bucket Names

static BUCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn unique_bucket() -> String {
    let n = BUCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("test-bucket-{n}")
}

// MARK: - S3 Clients

/// An `aws-sdk-s3` client pointed at the proxy (uses CLIENT_KEY_ID / CLIENT_SECRET).
pub async fn proxy_s3_client(proxy: &TestProxy) -> S3Client {
    let creds = Credentials::new(
        CLIENT_KEY_ID,
        CLIENT_SECRET,
        None,
        None,
        "docker-test-proxy",
    );
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(proxy.endpoint())
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(creds)
        .load()
        .await;
    S3Client::from_conf(
        aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(true)
            .build(),
    )
}

/// An `aws-sdk-s3` client pointed directly at MinIO (bypasses the proxy and its cache).
pub async fn direct_minio_client(minio_api_port: u16) -> S3Client {
    let creds = Credentials::new("minioadmin", "minioadmin", None, None, "docker-test-direct");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{minio_api_port}"))
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .credentials_provider(creds)
        .load()
        .await;
    S3Client::from_conf(
        aws_sdk_s3::config::Builder::from(&sdk_config)
            .force_path_style(true)
            .build(),
    )
}
