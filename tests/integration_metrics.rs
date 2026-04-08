#[cfg(feature = "docker-tests")]
#[path = "common/docker.rs"]
mod docker;

#[cfg(feature = "docker-tests")]
mod tests {
    use super::docker::*;
    use aws_sdk_s3::primitives::ByteStream;
    use bytes::Bytes;
    use testcontainers::core::IntoContainerPort;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    const OTEL_COLLECTOR_CONFIG: &[u8] = include_bytes!("../dev/otel-collector-config.yml");

    /// Polls the Prometheus endpoint until `condition` returns true for the response body,
    /// or panics after `timeout`.
    async fn poll_prometheus(
        url: &str,
        timeout: std::time::Duration,
        condition: fn(&str) -> bool,
    ) -> String {
        let client = reqwest::Client::new();
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            if let Ok(resp) = client.get(url).send().await
                && let Ok(body) = resp.text().await
                && condition(&body)
            {
                return body;
            }

            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for metrics at {url}"
            );

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    /// Generates traffic that exercises all observable metric paths:
    /// - cache miss (first GET)
    /// - cache hit (second GET of same object)
    /// - cache invalidation (PUT through proxy)
    /// - oversized object (GET object > max cacheable size)
    async fn generate_traffic(proxy: &TestProxy, minio_port: u16) {
        let direct = direct_minio_client(minio_port).await;
        let via_proxy = proxy_s3_client(proxy).await;
        let bucket = unique_bucket();

        direct.create_bucket().bucket(&bucket).send().await.unwrap();

        // -- Cache miss → miss_bytes, estimated_unique, size_count, size_bytes
        direct
            .put_object()
            .bucket(&bucket)
            .key("small.txt")
            .body(ByteStream::from(Bytes::from("hello metrics")))
            .send()
            .await
            .unwrap();

        via_proxy
            .get_object()
            .bucket(&bucket)
            .key("small.txt")
            .send()
            .await
            .unwrap();

        // -- Cache hit → hit_bytes
        via_proxy
            .get_object()
            .bucket(&bucket)
            .key("small.txt")
            .send()
            .await
            .unwrap();

        // -- Invalidation → invalidation_total
        via_proxy
            .put_object()
            .bucket(&bucket)
            .key("small.txt")
            .body(ByteStream::from(Bytes::from("updated")))
            .send()
            .await
            .unwrap();

        // -- Oversized object → oversized_bytes (2 MB > 1 MB limit)
        direct
            .put_object()
            .bucket(&bucket)
            .key("large.bin")
            .body(ByteStream::from(Bytes::from(vec![b'x'; 2 * 1024 * 1024])))
            .send()
            .await
            .unwrap();

        via_proxy
            .get_object()
            .bucket(&bucket)
            .key("large.bin")
            .send()
            .await
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn all_metrics_exported_with_correct_prometheus_names() {
        // -- OTel Collector --------------------------------------------------
        let collector_container =
            GenericImage::new("otel/opentelemetry-collector-contrib", "0.147.0")
                .with_exposed_port(4317.tcp())
                .with_exposed_port(8889.tcp())
                .with_copy_to(
                    "/etc/otelcol-contrib/config.yaml",
                    OTEL_COLLECTOR_CONFIG.to_vec(),
                )
                .start()
                .await
                .expect("failed to start OTel Collector container");

        let grpc_port = collector_container.get_host_port_ipv4(4317).await.unwrap();
        let prom_port = collector_container.get_host_port_ipv4(8889).await.unwrap();

        // -- MinIO -----------------------------------------------------------
        let minio = shared_minio().await;

        // -- Proxy with OTEL metrics enabled ---------------------------------
        let config = s3_cache::Config {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            upstream_endpoint: format!("http://127.0.0.1:{}", minio.api_port),
            upstream_access_key_id: "minioadmin".to_string(),
            upstream_secret_access_key: "minioadmin".to_string(),
            upstream_region: "us-east-1".to_string(),
            client_access_key_id: CLIENT_KEY_ID.to_string(),
            client_secret_access_key: CLIENT_SECRET.to_string(),
            cache_enabled: true,
            cache_shards: 4,
            cache_max_entries: 100,
            cache_max_size_bytes: 104_857_600,
            cache_max_object_size_bytes: 1_048_576,
            cache_ttl_seconds: 3600,
            worker_threads: 2,
            otel_grpc_endpoint_url: Some(format!("http://127.0.0.1:{grpc_port}")),
            otel_export_metrics: true,
            otel_export_logs: false,
        };

        let proxy = start_proxy_with_config(config).await;

        // -- Generate traffic ------------------------------------------------
        generate_traffic(&proxy, minio.api_port).await;

        // -- Poll until hit + miss metrics have arrived ----------------------
        let prom_url = format!("http://127.0.0.1:{prom_port}/metrics");
        let timeout = std::time::Duration::from_secs(90);
        let body = poll_prometheus(&prom_url, timeout, |b| {
            b.contains("s3_cache_hit_bytes_total") && b.contains("s3_cache_miss_bytes_total")
        })
        .await;

        // == Gauges ==========================================================

        // Info metric with config labels
        assert!(
            body.contains("s3_cache_info"),
            "missing gauge: s3_cache_info"
        );
        assert!(
            body.contains("cache_enabled"),
            "missing label: cache_enabled"
        );
        assert!(
            body.contains("upstream_region"),
            "missing label: upstream_region"
        );

        // Config gauges
        assert!(
            body.contains("s3_cache_config_cache_shards"),
            "missing gauge: s3_cache_config_cache_shards"
        );
        assert!(
            body.contains("s3_cache_config_cache_max_entries"),
            "missing gauge: s3_cache_config_cache_max_entries"
        );
        assert!(
            body.contains("s3_cache_config_cache_max_size_bytes"),
            "missing gauge: s3_cache_config_cache_max_size_bytes"
        );
        assert!(
            body.contains("s3_cache_config_cache_max_object_size_bytes"),
            "missing gauge: s3_cache_config_cache_max_object_size_bytes"
        );
        assert!(
            body.contains("s3_cache_config_cache_ttl_seconds"),
            "missing gauge: s3_cache_config_cache_ttl_seconds"
        );
        assert!(
            body.contains("s3_cache_config_worker_threads"),
            "missing gauge: s3_cache_config_worker_threads"
        );

        // Runtime gauges
        assert!(
            body.contains("s3_cache_size_count"),
            "missing gauge: s3_cache_size_count"
        );
        assert!(
            body.contains("s3_cache_size_bytes"),
            "missing gauge: s3_cache_size_bytes"
        );

        // == Counters ========================================================

        assert!(
            body.contains("s3_cache_hit_bytes_total"),
            "missing counter: s3_cache_hit_bytes_total"
        );
        assert!(
            body.contains("s3_cache_miss_bytes_total"),
            "missing counter: s3_cache_miss_bytes_total"
        );
        assert!(
            body.contains("s3_cache_invalidation_total"),
            "missing counter: s3_cache_invalidation_total"
        );
        assert!(
            body.contains("s3_cache_oversized_bytes_total"),
            "missing counter: s3_cache_oversized_bytes_total"
        );
        assert!(
            body.contains("s3_cache_estimated_unique_bytes_total"),
            "missing counter: s3_cache_estimated_unique_bytes_total"
        );

        // == Histograms (check _bucket suffix = correct histogram type) ======

        assert!(
            body.contains("s3_cache_hit_bytes_histogram_bucket"),
            "missing histogram: s3_cache_hit_bytes_histogram"
        );
        assert!(
            body.contains("s3_cache_miss_bytes_histogram_bucket"),
            "missing histogram: s3_cache_miss_bytes_histogram"
        );
        assert!(
            body.contains("s3_cache_oversized_bytes_histogram_bucket"),
            "missing histogram: s3_cache_oversized_bytes_histogram"
        );
        assert!(
            body.contains("s3_cache_estimated_unique_bytes_histogram_bucket"),
            "missing histogram: s3_cache_estimated_unique_bytes_histogram"
        );

        // == Histograms with unit suffix (OTel unit → Prometheus suffix) ======

        // http.server.request.duration (unit: ms) → _milliseconds
        assert!(
            body.contains("http_server_request_duration_milliseconds_bucket"),
            "missing histogram with unit: http_server_request_duration_milliseconds"
        );

        // http.client.request.duration (unit: ms) → _milliseconds
        assert!(
            body.contains("http_client_request_duration_milliseconds_bucket"),
            "missing histogram with unit: http_client_request_duration_milliseconds"
        );

        // http.server.response.body.size (unit: By) → _bytes
        assert!(
            body.contains("http_server_response_body_size_bytes_bucket"),
            "missing histogram with unit: http_server_response_body_size_bytes"
        );

        // == Not triggered in this test (would need cache eviction / errors) =
        // s3_cache_eviction_bytes_histogram, s3_cache_eviction_bytes_total
        // s3_cache_eviction_age_histogram_seconds
        // service_error_total, upstream_error_total
    }
}
