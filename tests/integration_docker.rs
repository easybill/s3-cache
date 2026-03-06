mod common;

#[cfg(feature = "docker-tests")]
#[path = "common/docker.rs"]
mod docker;

#[cfg(feature = "docker-tests")]
mod tests {
    use super::docker::*;
    use aws_sdk_s3::primitives::ByteStream;
    use bytes::Bytes;

    // MARK: - Basic GET

    /// Verifies the full HTTP path: client → proxy (HTTP) → MinIO → proxy → client.
    /// This is the baseline that everything else builds on.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_basic_get_through_proxy() {
        let minio = shared_minio().await;
        let proxy = start_proxy(minio.api_port).await;
        let bucket = unique_bucket();

        let direct = direct_minio_client(minio.api_port).await;
        let via_proxy = proxy_s3_client(&proxy).await;

        direct.create_bucket().bucket(&bucket).send().await.unwrap();
        direct
            .put_object()
            .bucket(&bucket)
            .key("hello.txt")
            .body(ByteStream::from(Bytes::from("hello world")))
            .send()
            .await
            .unwrap();

        let resp = via_proxy
            .get_object()
            .bucket(&bucket)
            .key("hello.txt")
            .send()
            .await
            .expect("GET through proxy failed");

        let body = resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(body, Bytes::from("hello world"));
    }

    // MARK: - Cache Hit

    /// This scenario is impossible to reproduce with the mock-based tests because
    /// it requires the ability to mutate MinIO state independently of the proxy.
    ///
    /// Scenario:
    ///   1. PUT directly to MinIO
    ///   2. GET through proxy → cache miss, response buffered into cache
    ///   3. DELETE directly from MinIO (bypasses proxy's invalidation logic)
    ///   4. GET through proxy → must still return the cached body, not a 404
    #[tokio::test(flavor = "multi_thread")]
    async fn test_cache_hit_survives_direct_deletion() {
        let minio = shared_minio().await;
        let proxy = start_proxy(minio.api_port).await;
        let bucket = unique_bucket();

        let direct = direct_minio_client(minio.api_port).await;
        let via_proxy = proxy_s3_client(&proxy).await;

        direct.create_bucket().bucket(&bucket).send().await.unwrap();
        direct
            .put_object()
            .bucket(&bucket)
            .key("cached.bin")
            .body(ByteStream::from(Bytes::from("cache-me")))
            .send()
            .await
            .unwrap();

        // First GET: cache miss → proxy fetches from MinIO and stores in cache
        let resp1 = via_proxy
            .get_object()
            .bucket(&bucket)
            .key("cached.bin")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp1.body.collect().await.unwrap().into_bytes(),
            Bytes::from("cache-me")
        );

        // Bypass the proxy: delete directly from MinIO
        direct
            .delete_object()
            .bucket(&bucket)
            .key("cached.bin")
            .send()
            .await
            .unwrap();

        // Second GET: MinIO no longer has the object, but the proxy must serve from cache
        let resp2 = via_proxy
            .get_object()
            .bucket(&bucket)
            .key("cached.bin")
            .send()
            .await
            .expect("proxy should have served from cache after direct deletion from MinIO");

        assert_eq!(
            resp2.body.collect().await.unwrap().into_bytes(),
            Bytes::from("cache-me"),
            "proxy did not serve from cache — the object was not cached or cache was bypassed"
        );
    }

    // MARK: - Cache Invalidation

    /// Tests that PUT *through the proxy* invalidates the cached version so
    /// the next GET returns the updated content.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_put_through_proxy_invalidates_cache() {
        let minio = shared_minio().await;
        let proxy = start_proxy(minio.api_port).await;
        let bucket = unique_bucket();

        let direct = direct_minio_client(minio.api_port).await;
        let via_proxy = proxy_s3_client(&proxy).await;

        direct.create_bucket().bucket(&bucket).send().await.unwrap();

        // PUT v1 directly to MinIO (no proxy involved for setup)
        direct
            .put_object()
            .bucket(&bucket)
            .key("mutable.txt")
            .body(ByteStream::from(Bytes::from("version-1")))
            .send()
            .await
            .unwrap();

        // GET through proxy: cache miss → stores v1
        let resp1 = via_proxy
            .get_object()
            .bucket(&bucket)
            .key("mutable.txt")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp1.body.collect().await.unwrap().into_bytes(),
            Bytes::from("version-1")
        );

        // PUT v2 *through the proxy*: must invalidate the cache entry
        via_proxy
            .put_object()
            .bucket(&bucket)
            .key("mutable.txt")
            .body(ByteStream::from(Bytes::from("version-2")))
            .send()
            .await
            .unwrap();

        // GET through proxy: cache was invalidated → must fetch v2 from MinIO
        let resp2 = via_proxy
            .get_object()
            .bucket(&bucket)
            .key("mutable.txt")
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp2.body.collect().await.unwrap().into_bytes(),
            Bytes::from("version-2"),
            "proxy returned stale cached v1 after PUT v2 through proxy"
        );
    }

    // MARK: - Large Object

    /// Objects exceeding CACHE_MAX_OBJECT_SIZE_BYTES (1 MB in test config) must
    /// be streamed through transparently without being stored in the cache.
    ///
    /// Proof: after a successful GET, delete the object directly from MinIO;
    /// a second GET must fail with NoSuchKey, showing the proxy never cached it.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_large_object_not_cached() {
        let minio = shared_minio().await;
        let proxy = start_proxy(minio.api_port).await;
        let bucket = unique_bucket();

        let direct = direct_minio_client(minio.api_port).await;
        let via_proxy = proxy_s3_client(&proxy).await;

        // 2 MB — exceeds the 1 MB CACHE_MAX_OBJECT_SIZE_BYTES set in start_proxy
        let large_body = Bytes::from(vec![b'x'; 2 * 1024 * 1024]);

        direct.create_bucket().bucket(&bucket).send().await.unwrap();
        direct
            .put_object()
            .bucket(&bucket)
            .key("large.bin")
            .body(ByteStream::from(large_body.clone()))
            .send()
            .await
            .unwrap();

        // First GET: object too large to cache, must stream through successfully
        let resp1 = via_proxy
            .get_object()
            .bucket(&bucket)
            .key("large.bin")
            .send()
            .await
            .expect("large object GET through proxy failed");
        let got = resp1.body.collect().await.unwrap().into_bytes();
        assert_eq!(
            got.len(),
            large_body.len(),
            "large object body length mismatch"
        );

        // Delete directly from MinIO (bypasses proxy)
        direct
            .delete_object()
            .bucket(&bucket)
            .key("large.bin")
            .send()
            .await
            .unwrap();

        // Second GET: must fail because the object was too large to cache
        let resp2 = via_proxy
            .get_object()
            .bucket(&bucket)
            .key("large.bin")
            .send()
            .await;

        assert!(
            resp2.is_err(),
            "large object should NOT have been cached; expected an error after direct deletion"
        );
    }
}
