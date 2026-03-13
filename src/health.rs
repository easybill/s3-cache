use bytes::Bytes;
use s3s::route::S3Route;
use s3s::{S3, S3Request, S3Response, S3Result, Body};
use hyper::{HeaderMap, Method, Uri};
use hyper::http::Extensions;
#[derive(Clone)]
pub(crate) struct HealthRoute;

#[async_trait::async_trait]
impl S3Route for HealthRoute {
    fn is_match(&self, method: &Method, uri: &Uri, _headers: &HeaderMap, _extensions: &mut Extensions) -> bool {
        method == Method::GET && (uri.path() == "/" || uri.path() == "/health")
    }

    async fn call(&self, _req: S3Request<Body>) -> S3Result<S3Response<Body>> {
        Ok(S3Response::new(Body::from(Bytes::from_static(b"Status OK"))))
    }
}