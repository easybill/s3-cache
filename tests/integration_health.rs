use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use tokio::net::TcpStream;

async fn start_test_server() -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    start_test_server_with_upstream("http://127.0.0.1:1").await
}

async fn start_test_server_with_upstream(
    upstream: &str,
) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let config = s3_cache::Config {
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        upstream_endpoint: upstream.to_string(),
        upstream_access_key_id: "test".to_string(),
        upstream_secret_access_key: "test".to_string(),
        upstream_region: "us-east-1".to_string(),
        client_access_key_id: "testclient".to_string(),
        client_secret_access_key: "testsecret".to_string(),
        cache_enabled: false,
        cache_shards: 4,
        cache_max_entries: 100,
        cache_max_size_bytes: 1024 * 1024,
        cache_max_object_size_bytes: 1024,
        cache_ttl_seconds: 60,
        worker_threads: 2,
        otel_grpc_endpoint_url: None,
        otel_export_metrics: false,
        otel_export_logs: false,
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let (addr_tx, addr_rx) = tokio::sync::oneshot::channel::<SocketAddr>();

    tokio::spawn(async move {
        let shutdown = async move {
            let _ = shutdown_rx.await;
        };
        let _ = s3_cache::start_app_with_shutdown(config, shutdown, addr_tx).await;
    });

    let addr = addr_rx.await.expect("server startup failed");
    (addr, shutdown_tx)
}

async fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);

    let req = hyper::Request::builder()
        .method(hyper::Method::GET)
        .uri(path)
        .header("Host", "localhost")
        .body(Empty::<Bytes>::new())
        .unwrap();

    let resp = sender.send_request(req).await.unwrap();
    let status = resp.status().as_u16();
    let body = resp.collect().await.unwrap().to_bytes();

    (status, String::from_utf8_lossy(&body).to_string())
}

async fn start_fake_upstream(status: u16) -> (SocketAddr, tokio::sync::oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    tokio::spawn(async move {
        loop {
            let (stream, _) = tokio::select! {
                result = listener.accept() => result.unwrap(),
                _ = &mut shutdown_rx => break,
            };
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |_req| {
                    let resp = hyper::Response::builder()
                        .status(status)
                        .body(http_body_util::Empty::<Bytes>::new())
                        .unwrap();
                    std::future::ready(Ok::<_, std::convert::Infallible>(resp))
                });
                hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await
                    .ok();
            });
        }
    });

    (addr, shutdown_tx)
}

// MARK: - Health

#[tokio::test(flavor = "multi_thread")]
async fn health_check_ok() {
    let (addr, _shutdown) = start_test_server().await;

    let (status, body) = http_get(addr, "/health").await;

    assert_eq!(status, 200);
    assert_eq!(body, "");
}

#[tokio::test(flavor = "multi_thread")]
async fn health_check_does_not_require_auth() {
    let (addr, _shutdown) = start_test_server().await;

    // No Authorization header — must still succeed
    let (status, _) = http_get(addr, "/health").await;

    assert_eq!(status, 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn upstream_health_returns_ok_when_upstream_is_healthy() {
    let (upstream_addr, _upstream_shutdown) = start_fake_upstream(200).await;
    let upstream_url = format!("http://{upstream_addr}");

    let (addr, _shutdown) = start_test_server_with_upstream(&upstream_url).await;
    let (status, _) = http_get(addr, "/upstream-health").await;

    assert_eq!(status, 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn upstream_health_returns_same_status_code_as_upstream() {
    let random_http_status_code = 201;
    let (upstream_addr, _upstream_shutdown) = start_fake_upstream(random_http_status_code).await;
    let upstream_url = format!("http://{upstream_addr}");

    let (addr, _shutdown) = start_test_server_with_upstream(&upstream_url).await;
    let (status, _) = http_get(addr, "/upstream-health").await;

    assert_eq!(status, random_http_status_code);
}

#[tokio::test(flavor = "multi_thread")]
async fn upstream_health_returns_500_when_upstream_is_unreachable() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = listener.local_addr().unwrap();
    drop(listener);

    let upstream_url = format!("http://{dead_addr}");
    let (addr, _shutdown) = start_test_server_with_upstream(&upstream_url).await;
    let (status, _) = http_get(addr, "/upstream-health").await;

    assert_eq!(status, 500);
}
