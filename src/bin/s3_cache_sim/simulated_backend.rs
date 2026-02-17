use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use s3s::dto::*;
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct SimulatedBackend {
    /// bucket -> key -> body
    storage: Arc<RwLock<HashMap<String, HashMap<String, Bytes>>>>,
    base_latency: Duration,
    throughput_bps: u64,
    default_object_size: Option<usize>,
    get_count: Arc<AtomicU64>,
}

impl SimulatedBackend {
    pub fn new(
        base_latency: Duration,
        throughput_bps: u64,
        default_object_size: Option<usize>,
    ) -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
            base_latency,
            throughput_bps,
            default_object_size,
            get_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Pre-populate the backend with `num_objects` in bucket "sim" with keys "obj-0", "obj-1", ...
    /// Each object gets a zeroed body with a random size in [min_size, max_size].
    pub async fn populate(&self, num_objects: usize, min_size: usize, max_size: usize, seed: u64) {
        let mut rng = StdRng::seed_from_u64(seed);
        let mut bucket_map = HashMap::with_capacity(num_objects);

        for i in 0..num_objects {
            let size = if min_size == max_size {
                min_size
            } else {
                rng.random_range(min_size..=max_size)
            };
            let body = Bytes::from(vec![0u8; size]);
            bucket_map.insert(format!("obj-{i}"), body);
        }

        let mut storage = self.storage.write().await;
        storage.insert("sim".to_string(), bucket_map);
    }

    pub fn get_count(&self) -> u64 {
        self.get_count.load(Ordering::Relaxed)
    }

    /// Get the size of an object by key, or default size for one-hit-wonders
    pub async fn get_object_size(&self, bucket: &str, key: &str) -> Option<usize> {
        let storage = self.storage.read().await;
        if let Some(body) = storage.get(bucket).and_then(|b| b.get(key)) {
            Some(body.len())
        } else {
            self.default_object_size
        }
    }

    async fn simulate_latency(&self, body_len: usize) {
        let transfer_delay = if self.throughput_bps > 0 {
            Duration::from_secs_f64(body_len as f64 / self.throughput_bps as f64)
        } else {
            Duration::ZERO
        };
        let total = self.base_latency + transfer_delay;
        if !total.is_zero() {
            tokio::time::sleep(total).await;
        }
    }
}

#[async_trait::async_trait]
impl S3 for SimulatedBackend {
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        self.get_count.fetch_add(1, Ordering::Relaxed);

        let bucket = &req.input.bucket;
        let key = &req.input.key;

        let body = {
            let storage = self.storage.read().await;
            storage
                .get(bucket)
                .and_then(|b| b.get(key))
                .cloned()
        };

        let body = match body {
            Some(b) => b,
            None => {
                // One-hit-wonder: return a default-sized zeroed body if configured
                if let Some(size) = self.default_object_size {
                    Bytes::from(vec![0u8; size])
                } else {
                    return Err(s3_error!(NoSuchKey, "Key not found: {key}"));
                }
            }
        };

        self.simulate_latency(body.len()).await;

        let content_length = body.len() as i64;
        let output = GetObjectOutput {
            body: Some(StreamingBlob::from(s3s::Body::from(body))),
            content_length: Some(content_length),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }
}
