use bytes::Bytes;
use http::StatusCode;
use hyper::http::Extensions;
use hyper::{HeaderMap, Method, Uri};
use reqwest::Url;
use s3s::route::S3Route;
use s3s::{Body, S3, S3Error, S3Request, S3Response, S3Result};

#[derive(Clone)]
pub(crate) struct HealthRoute;

#[async_trait::async_trait]
impl S3Route for HealthRoute {
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        _headers: &HeaderMap,
        _extensions: &mut Extensions,
    ) -> bool {
        method == Method::GET && uri.path() == "/health"
    }

    async fn call(&self, _req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        Ok(S3Response::new(Body::empty()))
    }
}

#[derive(Clone)]
pub(crate) struct UpstreamHealthRoute {
    upstream_endpoint: Url,
}

impl UpstreamHealthRoute {
    pub fn new(upstream_endpoint: Url) -> Self {
        Self { upstream_endpoint }
    }
}

#[async_trait::async_trait]
impl S3Route for UpstreamHealthRoute {
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        _headers: &HeaderMap,
        _extensions: &mut Extensions,
    ) -> bool {
        method == Method::GET && uri.path() == "/upstream-health"
    }

    async fn call(&self, _req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        let upstream_status = reqwest::get(self.upstream_endpoint.clone())
            .await
            .map(|res| res.status())
            .unwrap_or_else(|e| {
                tracing::error!("Failed to get upstream status: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            });
        Ok(S3Response::with_status(Body::empty(), upstream_status))
    }
}
