use async_trait::async_trait;
use bytes::Bytes;
use s3s::dto::*;
use s3s::{Body, S3, S3Request, S3Response, S3Result, s3_error};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod helpers;

/// In-memory S3 backend for testing
#[derive(Clone)]
pub struct MockS3Backend {
    storage: Arc<Mutex<HashMap<String, HashMap<String, Bytes>>>>,
    get_count: Arc<Mutex<u64>>,
    put_count: Arc<Mutex<u64>>,
    delete_count: Arc<Mutex<u64>>,
}

impl MockS3Backend {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
            get_count: Arc::new(Mutex::new(0)),
            put_count: Arc::new(Mutex::new(0)),
            delete_count: Arc::new(Mutex::new(0)),
        }
    }

    /// Pre-populate storage with test data
    pub async fn put_object_sync(&self, bucket: &str, key: &str, data: &[u8]) {
        let mut storage = self.storage.lock().await;
        let bucket_map = storage
            .entry(bucket.to_string())
            .or_insert_with(HashMap::new);
        bucket_map.insert(key.to_string(), Bytes::copy_from_slice(data));
    }

    /// Get request count for cache hit verification
    pub async fn get_request_count(&self) -> u64 {
        *self.get_count.lock().await
    }

    pub async fn put_request_count(&self) -> u64 {
        *self.put_count.lock().await
    }

    pub async fn delete_request_count(&self) -> u64 {
        *self.delete_count.lock().await
    }

    /// Check if an object exists in storage
    pub async fn contains_object(&self, bucket: &str, key: &str) -> bool {
        let storage = self.storage.lock().await;
        storage
            .get(bucket)
            .and_then(|bucket_map| bucket_map.get(key))
            .is_some()
    }
}

#[async_trait]
impl S3 for MockS3Backend {
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        *self.get_count.lock().await += 1;

        let input = req.input;
        let storage = self.storage.lock().await;

        let bucket_map = storage
            .get(&input.bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket, "Bucket not found"))?;

        let data = bucket_map
            .get(&input.key)
            .ok_or_else(|| s3_error!(NoSuchKey, "Key not found"))?;

        // Handle range requests (simplified - just return full data for tests)
        let body_data = data.clone();

        let output = GetObjectOutput {
            body: Some(StreamingBlob::from(Body::from(body_data))),
            content_length: Some(data.len() as i64),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        *self.put_count.lock().await += 1;

        let input = req.input;
        let body_bytes = if let Some(body_blob) = input.body {
            let mut body = Body::from(body_blob);
            // Try to get bytes directly if available, otherwise buffer the stream
            if let Some(bytes) = body.bytes() {
                bytes
            } else {
                body.store_all_limited(100_000_000)
                    .await
                    .map_err(|_| s3_error!(InternalError, "Failed to read body"))?
            }
        } else {
            Bytes::new()
        };

        let mut storage = self.storage.lock().await;
        let bucket_map = storage.entry(input.bucket).or_insert_with(HashMap::new);
        bucket_map.insert(input.key, body_bytes);

        let output = PutObjectOutput::default();
        Ok(S3Response::new(output))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        *self.delete_count.lock().await += 1;

        let input = req.input;
        let mut storage = self.storage.lock().await;

        if let Some(bucket_map) = storage.get_mut(&input.bucket) {
            bucket_map.remove(&input.key);
        }

        let output = DeleteObjectOutput::default();
        Ok(S3Response::new(output))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let input = req.input;
        let mut storage = self.storage.lock().await;

        let mut deleted = Vec::new();

        if let Some(bucket_map) = storage.get_mut(&input.bucket) {
            for object_id in &input.delete.objects {
                if bucket_map.remove(&object_id.key).is_some() {
                    *self.delete_count.lock().await += 1;
                    deleted.push(DeletedObject {
                        key: Some(object_id.key.clone()),
                        ..Default::default()
                    });
                }
            }
        }

        let output = DeleteObjectsOutput {
            deleted: Some(deleted),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let storage = self.storage.lock().await;

        let bucket_map = storage
            .get(&input.bucket)
            .ok_or_else(|| s3_error!(NoSuchBucket, "Bucket not found"))?;

        let data = bucket_map
            .get(&input.key)
            .ok_or_else(|| s3_error!(NoSuchKey, "Key not found"))?;

        let output = HeadObjectOutput {
            content_length: Some(data.len() as i64),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let input = req.input;

        // Parse source bucket/key from copy_source
        let (src_bucket, src_key) = match &input.copy_source {
            CopySource::Bucket { bucket, key, .. } => (bucket.as_ref(), key.as_ref()),
            CopySource::AccessPoint { .. } => {
                return Err(s3_error!(
                    InvalidArgument,
                    "AccessPoint copy not supported in test mock"
                ));
            }
        };

        let mut storage = self.storage.lock().await;

        // Get source data
        let src_data = storage
            .get(src_bucket)
            .and_then(|bm| bm.get(src_key))
            .ok_or_else(|| s3_error!(NoSuchKey, "Source key not found"))?
            .clone();

        // Copy to destination
        let bucket_map = storage.entry(input.bucket).or_insert_with(HashMap::new);
        bucket_map.insert(input.key, src_data);

        let output = CopyObjectOutput::default();
        Ok(S3Response::new(output))
    }
}
