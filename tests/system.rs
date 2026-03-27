#![cfg(feature = "system_tests")]

use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use bytes::Bytes;
use tokio::sync::Mutex;

// MARK: - Common

#[path = "common/docker.rs"]
mod docker;
use docker::shared_minio;

// MARK: - Constants

const SYSTEM_TEST_PORT: u16 = 18080;
const CLIENT_ACCESS_KEY_ID: &str = "testclient";
const CLIENT_SECRET_ACCESS_KEY: &str = "testsecret";

// MARK: - Static State

static PORT_GUARD: Mutex<bool> = Mutex::const_new(false);
static BUILD_ONCE: std::sync::Once = std::sync::Once::new();

// MARK: - Build

fn ensure_binary_built() {
    BUILD_ONCE.call_once(|| {
        eprintln!("Building s3_cache binary...");
        let status = Command::new("cargo")
            .args(["build", "--bin", "s3_cache"])
            .status()
            .expect("failed to run cargo build");

        if !status.success() {
            panic!("cargo build --bin s3_cache failed");
        }

        eprintln!("s3_cache binary built successfully");
    });
}

// MARK: - Server Lifecycle

fn create_server_command(minio_port: u16) -> Command {
    let mut cmd = Command::new("target/debug/s3_cache");

    cmd.env("LISTEN_ADDR", format!("127.0.0.1:{SYSTEM_TEST_PORT}"))
        .env(
            "UPSTREAM_ENDPOINT",
            format!("http://127.0.0.1:{minio_port}"),
        )
        .env("UPSTREAM_ACCESS_KEY_ID", "minioadmin")
        .env("UPSTREAM_SECRET_ACCESS_KEY", "minioadmin")
        .env("UPSTREAM_REGION", "us-east-1")
        .env("CLIENT_ACCESS_KEY_ID", CLIENT_ACCESS_KEY_ID)
        .env("CLIENT_SECRET_ACCESS_KEY", CLIENT_SECRET_ACCESS_KEY)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    cmd
}

async fn wait_for_server_ready(url: &str, process: &mut Child) -> Result<(), String> {
    let health_url = format!("{url}/health");
    let client = reqwest::Client::new();
    let max_attempts = 50;
    let retry_delay = Duration::from_millis(100);

    for attempt in 1..=max_attempts {
        if let Ok(Some(status)) = process.try_wait() {
            let stdout = process.stdout.take().map_or_else(String::new, |mut out| {
                let mut buf = String::new();
                let _ = std::io::Read::read_to_string(&mut out, &mut buf);
                buf
            });
            let stderr = process.stderr.take().map_or_else(String::new, |mut err| {
                let mut buf = String::new();
                let _ = std::io::Read::read_to_string(&mut err, &mut buf);
                buf
            });
            eprintln!("Server stdout:\n{stdout}");
            eprintln!("Server stderr:\n{stderr}");
            return Err(format!("server process exited early with status: {status}"));
        }

        match client.get(&health_url).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            _ => {
                if attempt == max_attempts {
                    return Err("server failed to start within timeout".to_string());
                }
                tokio::time::sleep(retry_delay).await;
            }
        }
    }

    Err("server failed to start".to_string())
}

fn shutdown_server(process: &mut Child) -> Result<(), String> {
    let pid = process.id();

    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGTERM);
    }

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(5);

    loop {
        match process.try_wait() {
            Ok(Some(_)) => return Ok(()),
            Ok(None) => {
                if start.elapsed() > timeout {
                    let _ = process.kill();
                    return Err("server did not shut down gracefully within 5 s".to_string());
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => return Err(format!("error waiting for server shutdown: {e}")),
        }
    }
}

// MARK: - S3 Client

async fn proxy_client(port: u16) -> S3Client {
    let creds = Credentials::new(
        CLIENT_ACCESS_KEY_ID,
        CLIENT_SECRET_ACCESS_KEY,
        None,
        None,
        "system-test",
    );
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .endpoint_url(format!("http://127.0.0.1:{port}"))
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

// MARK: - Health Check

#[tokio::test(flavor = "multi_thread")]
async fn test_health_check() {
    ensure_binary_built();

    let minio = shared_minio().await;
    let _guard = PORT_GUARD.lock().await;

    let mut server = create_server_command(minio.api_port)
        .spawn()
        .expect("failed to spawn s3_cache");

    let base_url = format!("http://127.0.0.1:{SYSTEM_TEST_PORT}");
    let ready = wait_for_server_ready(&base_url, &mut server).await;

    if let Err(e) = ready {
        let _ = server.kill();
        panic!("server did not become ready: {e}");
    }

    let response = reqwest::get(format!("{base_url}/health"))
        .await
        .expect("health request failed");

    shutdown_server(&mut server).expect("server shutdown failed");

    assert!(
        response.status().is_success(),
        "expected 200 from /health, got {}",
        response.status()
    );
}

// MARK: - PUT/GET

#[tokio::test(flavor = "multi_thread")]
async fn test_put_get_object() {
    ensure_binary_built();

    let minio = shared_minio().await;
    let _guard = PORT_GUARD.lock().await;

    let mut server = create_server_command(minio.api_port)
        .spawn()
        .expect("failed to spawn s3_cache");

    let base_url = format!("http://127.0.0.1:{SYSTEM_TEST_PORT}");
    let ready = wait_for_server_ready(&base_url, &mut server).await;

    if let Err(e) = ready {
        let _ = server.kill();
        panic!("server did not become ready: {e}");
    }

    let client = proxy_client(SYSTEM_TEST_PORT).await;
    let bucket = "sys-test-bucket";
    let key = "hello.txt";
    let body = b"hello world";

    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create_bucket failed");

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .expect("put_object failed");

    let get_resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("get_object failed");

    let got = get_resp
        .body
        .collect()
        .await
        .expect("failed to collect body")
        .into_bytes();

    shutdown_server(&mut server).expect("server shutdown failed");

    assert_eq!(got, Bytes::from_static(body));
}

// MARK: - Graceful Shutdown

#[tokio::test(flavor = "multi_thread")]
async fn test_graceful_shutdown() {
    ensure_binary_built();

    let minio = shared_minio().await;
    let _guard = PORT_GUARD.lock().await;

    let mut server = create_server_command(minio.api_port)
        .spawn()
        .expect("failed to spawn s3_cache");

    let base_url = format!("http://127.0.0.1:{SYSTEM_TEST_PORT}");
    let ready = wait_for_server_ready(&base_url, &mut server).await;

    if let Err(e) = ready {
        let _ = server.kill();
        panic!("server did not become ready: {e}");
    }

    shutdown_server(&mut server).expect("server did not shut down gracefully");
}

// MARK: - Config Validation

#[tokio::test(flavor = "multi_thread")]
async fn test_missing_required_config() {
    ensure_binary_built();

    // Spawn with all required env vars except UPSTREAM_ENDPOINT.
    // clap exits immediately — no MinIO or port binding needed.
    let output = Command::new("target/debug/s3_cache")
        .env("LISTEN_ADDR", format!("127.0.0.1:{SYSTEM_TEST_PORT}"))
        .env("UPSTREAM_ACCESS_KEY_ID", "minioadmin")
        .env("UPSTREAM_SECRET_ACCESS_KEY", "minioadmin")
        .env("CLIENT_ACCESS_KEY_ID", CLIENT_ACCESS_KEY_ID)
        .env("CLIENT_SECRET_ACCESS_KEY", CLIENT_SECRET_ACCESS_KEY)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3_cache");

    assert!(
        !output.status.success(),
        "expected non-zero exit when UPSTREAM_ENDPOINT is missing, got: {}",
        output.status
    );
}
