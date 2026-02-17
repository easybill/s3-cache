use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use prometheus::{Encoder, TextEncoder};
use tracing::{debug, error, info};

use crate::proxy_service::CachingProxy;
use crate::telemetry;

/// Start the Prometheus metrics writer background task.
///
/// This task periodically writes metrics to a text file in Prometheus format,
/// which can be read by node_exporter's textfile collector.
///
/// # Arguments
/// * `textfile_dir` - Directory where metrics file will be written
/// * `caching_proxy` - Arc reference to the CachingProxy for accessing probabilistic counters
///
/// # Behavior
/// - Writes metrics every 10 seconds
/// - Uses atomic file operations (write to .tmp, then rename)
/// - Continues running even if individual writes fail
/// - Graceful shutdown on Ctrl+C
pub async fn start_metrics_writer(
    textfile_dir: String,
    caching_proxy: Arc<CachingProxy>,
) -> crate::Result<()> {
    info!(
        "Prometheus metrics writer started, writing to {}/s3_cache.prom",
        textfile_dir
    );

    let tmp_path = format!("{}/s3_cache.prom.tmp", textfile_dir);
    let final_path = format!("{}/s3_cache.prom", textfile_dir);

    let mut interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        // Update probabilistic metrics from CachingProxy
        let unique_count = caching_proxy.estimated_unique_count();
        let unique_bytes = caching_proxy.estimated_unique_bytes();
        telemetry::record_counter_estimates(unique_count, unique_bytes);

        debug!(
            unique_count = unique_count,
            unique_bytes = unique_bytes,
            "Updated probabilistic metrics"
        );

        // Gather all metrics
        let metric_families = telemetry::PROMETHEUS_REGISTRY.gather();

        // Encode to Prometheus text format
        let encoder = TextEncoder::new();
        let mut buffer = Vec::new();

        if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
            error!("Failed to encode Prometheus metrics: {}", e);
            continue;
        }

        // Write atomically: write to temp file, then rename
        match write_metrics_atomic(&tmp_path, &final_path, &buffer) {
            Ok(_) => {
                debug!(
                    "Successfully wrote {} bytes to {}",
                    buffer.len(),
                    final_path
                );
            }
            Err(e) => {
                error!("Failed to write metrics file: {}", e);
            }
        }
    }
}

/// Write metrics to file atomically using write-to-temp + rename pattern.
///
/// This ensures node_exporter never reads a partially written file.
fn write_metrics_atomic(
    tmp_path: &str,
    final_path: &str,
    data: &[u8],
) -> std::io::Result<()> {
    // Write to temporary file
    let mut file = File::create(tmp_path)?;
    file.write_all(data)?;
    file.sync_all()?; // Ensure data is written to disk

    // Atomic rename
    std::fs::rename(tmp_path, final_path)?;

    Ok(())
}
