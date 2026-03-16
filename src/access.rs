use s3s::S3Result;
use s3s::access::{S3Access, S3AccessContext};

use crate::telemetry;

pub(crate) struct TelemetryAccess;

#[async_trait::async_trait]
impl S3Access for TelemetryAccess {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        telemetry::record_endpoint_call(cx.s3_op().name());

        match cx.credentials() {
            Some(_) => Ok(()),
            None => Err(s3s::s3_error!(AccessDenied, "Signature is required")),
        }
    }
}
