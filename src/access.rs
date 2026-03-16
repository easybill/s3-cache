use s3s::S3Result;
use s3s::access::{S3Access, S3AccessContext};

use crate::telemetry;

pub(crate) struct TelemetryAccess;

#[async_trait::async_trait]
impl S3Access for TelemetryAccess {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // to use this check to record telemetry data is an abuse of the S3Access trait, but we use it here to have a single location where we can record the requested operations
        telemetry::record_endpoint_call(cx.s3_op().name());

        if cx.credentials().is_none() {
            return Err(s3s::s3_error!(AccessDenied, "Signature is required"));
        }

        Ok(())
    }
}
