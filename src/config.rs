use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    net::SocketAddr,
};

pub struct Config {
    pub listen_addr: SocketAddr,
    pub upstream_endpoint: String,
    pub upstream_access_key_id: String,
    pub upstream_secret_access_key: String,
    pub upstream_region: String,
    pub client_access_key_id: String,
    pub client_secret_access_key: String,
    pub cache_enabled: bool,
    pub cache_dryrun: bool,
    pub cache_shards: usize,
    pub cache_max_entries: usize,
    pub cache_max_size_bytes: usize,
    pub cache_max_object_size_bytes: usize,
    pub cache_ttl_seconds: usize,
    pub worker_threads: usize,
    pub otel_grpc_endpoint_url: Option<String>,
    pub prometheus_textfile_dir: Option<String>,
}

impl Config {
    pub fn from_env(vars: &HashMap<String, String>) -> Self {
        let config = Self {
            listen_addr: vars
                .get("LISTEN_ADDR")
                .map(|s| s.parse().expect("invalid LISTEN_ADDR"))
                .unwrap_or_else(|| "0.0.0.0:8080".parse().unwrap()),
            upstream_endpoint: vars
                .get("UPSTREAM_ENDPOINT")
                .cloned()
                .expect("UPSTREAM_ENDPOINT is required"),
            upstream_access_key_id: vars
                .get("UPSTREAM_ACCESS_KEY_ID")
                .cloned()
                .expect("UPSTREAM_ACCESS_KEY_ID is required"),
            upstream_secret_access_key: vars
                .get("UPSTREAM_SECRET_ACCESS_KEY")
                .cloned()
                .expect("UPSTREAM_SECRET_ACCESS_KEY is required"),
            upstream_region: vars
                .get("UPSTREAM_REGION")
                .cloned()
                .unwrap_or_else(|| "us-east-1".to_string()),
            client_access_key_id: vars
                .get("CLIENT_ACCESS_KEY_ID")
                .cloned()
                .expect("CLIENT_ACCESS_KEY_ID is required"),
            client_secret_access_key: vars
                .get("CLIENT_SECRET_ACCESS_KEY")
                .cloned()
                .expect("CLIENT_SECRET_ACCESS_KEY is required"),
            cache_enabled: vars
                .get("CACHE_ENABLED")
                .map(|s| s.parse().expect("invalid CACHE_ENABLED"))
                .unwrap_or(true),
            cache_dryrun: vars
                .get("CACHE_DRYRUN")
                .map(|s| s.parse().expect("invalid CACHE_DRYRUN"))
                .unwrap_or(false),
            cache_shards: vars
                .get("CACHE_SHARDS")
                .map(|s| s.parse().expect("invalid CACHE_SHARDS"))
                .unwrap_or(16),
            cache_max_entries: vars
                .get("CACHE_MAX_ENTRIES")
                .map(|s| s.parse().expect("invalid CACHE_MAX_ENTRIES"))
                .unwrap_or(10_000),
            cache_max_size_bytes: vars
                .get("CACHE_MAX_SIZE_BYTES")
                .map(|s| s.parse().expect("invalid CACHE_MAX_SIZE_BYTES"))
                .unwrap_or(1_073_741_824),
            cache_max_object_size_bytes: vars
                .get("CACHE_MAX_OBJECT_SIZE_BYTES")
                .map(|s: &String| s.parse().expect("invalid CACHE_MAX_OBJECT_SIZE_BYTES"))
                .unwrap_or(10_485_760),
            cache_ttl_seconds: vars
                .get("CACHE_TTL_SECONDS")
                .map(|s| s.parse().expect("invalid CACHE_TTL_SECONDS"))
                .unwrap_or(86_400),
            worker_threads: vars
                .get("WORKER_THREADS")
                .map(|s| s.parse().expect("invalid WORKER_THREADS"))
                .unwrap_or(4),
            otel_grpc_endpoint_url: vars.get("OTEL_GRPC_ENDPOINT_URL").cloned(),
            prometheus_textfile_dir: vars.get("PROMETHEUS_TEXTFILE_DIR").cloned(),
        };

        config.validate();
        config
    }

    fn validate(&self) {
        if self.cache_max_size_bytes < self.cache_max_object_size_bytes {
            panic!(
                "Invalid configuration: cache_max_size_bytes ({}) must be >= max_cacheable_object_size ({})",
                self.cache_max_size_bytes, self.cache_max_object_size_bytes
            );
        }

        if self.cache_ttl_seconds == 0 {
            panic!("Invalid configuration: cache_ttl_seconds must be greater than 0");
        }

        if self.cache_max_entries == 0 {
            panic!("Invalid configuration: cache_max_entries must be greater than 0");
        }

        if self.cache_shards == 0 {
            panic!("Invalid configuration: cache_shards must be greater than 0");
        }

        if self.worker_threads == 0 {
            panic!("Invalid configuration: worker_threads must be greater than 0");
        }
    }
}

impl Display for Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Config{{ listen_addr: {}, upstream_endpoint: {}, upstream_region: {}, \
             cache_max_entries: {}, cache_max_size_bytes: {}, cache_ttl_seconds: {}, \
             max_cacheable_object_size: {}, otel_grpc_endpoint_url: {:?}, cache_shards: {}, \
             cache_dryrun: {}, worker_threads: {}, prometheus_textfile_dir: {:?} }}",
            self.listen_addr,
            self.upstream_endpoint,
            self.upstream_region,
            self.cache_max_entries,
            self.cache_max_size_bytes,
            self.cache_ttl_seconds,
            self.cache_max_object_size_bytes,
            self.otel_grpc_endpoint_url,
            self.cache_shards,
            self.cache_dryrun,
            self.worker_threads,
            self.prometheus_textfile_dir,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal_env() -> HashMap<String, String> {
        let mut env = HashMap::new();
        env.insert(
            "UPSTREAM_ENDPOINT".to_string(),
            "http://minio:9000".to_string(),
        );
        env.insert(
            "UPSTREAM_ACCESS_KEY_ID".to_string(),
            "minioadmin".to_string(),
        );
        env.insert(
            "UPSTREAM_SECRET_ACCESS_KEY".to_string(),
            "minioadmin".to_string(),
        );
        env.insert("CLIENT_ACCESS_KEY_ID".to_string(), "testclient".to_string());
        env.insert(
            "CLIENT_SECRET_ACCESS_KEY".to_string(),
            "testclient".to_string(),
        );
        env
    }

    #[test]
    fn config_valid() {
        let env = minimal_env();
        let config = Config::from_env(&env);
        assert_eq!(config.cache_max_entries, 10_000);
        assert_eq!(config.cache_max_size_bytes, 1_073_741_824);
        assert_eq!(config.cache_max_object_size_bytes, 10_485_760);
    }

    #[test]
    #[should_panic(expected = "cache_max_size_bytes")]
    fn config_max_size_too_small() {
        let mut env = minimal_env();
        env.insert("CACHE_MAX_SIZE_BYTES".to_string(), "1000".to_string());
        env.insert(
            "CACHE_MAX_OBJECT_SIZE_BYTES".to_string(),
            "2000".to_string(),
        );
        Config::from_env(&env);
    }

    #[test]
    #[should_panic(expected = "cache_ttl_seconds")]
    fn config_zero_ttl() {
        let mut env = minimal_env();
        env.insert("CACHE_TTL_SECONDS".to_string(), "0".to_string());
        Config::from_env(&env);
    }

    #[test]
    #[should_panic(expected = "cache_max_entries")]
    fn config_zero_max_entries() {
        let mut env = minimal_env();
        env.insert("CACHE_MAX_ENTRIES".to_string(), "0".to_string());
        Config::from_env(&env);
    }

    #[test]
    #[should_panic(expected = "worker_threads")]
    fn config_zero_worker_threads() {
        let mut env = minimal_env();
        env.insert("WORKER_THREADS".to_string(), "0".to_string());
        Config::from_env(&env);
    }
}
