use hyper::StatusCode;
use hyper::service::Service;
use hyper::{Method, Request, Response, body::Incoming};
use reqwest::Url;
use s3s::{Body, HttpError};
use std::future::Future;
use std::pin::Pin;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Wraps an S3 service and short-circuits `GET /health` and `GET /upstream-health`
/// requests without forwarding them to the S3 layer or requiring authentication.
#[derive(Clone)]
pub struct S3CachingServiceProxy<S> {
    inner: S,
    upstream_health_endpoint: Url,
}

impl<S> S3CachingServiceProxy<S> {
    pub fn new(inner: S, upstream_health_endpoint: Url) -> Self {
        Self {
            inner,
            upstream_health_endpoint,
        }
    }
}

impl<S> Service<Request<Incoming>> for S3CachingServiceProxy<S>
where
    S: Service<Request<Incoming>, Response = Response<Body>, Error = HttpError>,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = HttpError;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        if req.method() == Method::GET && req.uri().path() == "/health" {
            let response = Response::builder().status(200).body(Body::empty()).unwrap();
            Box::pin(std::future::ready(Ok(response)))
        } else if req.method() == Method::GET && req.uri().path() == "/upstream-health" {
            let upstream_health_endpoint = self.upstream_health_endpoint.clone();
            let upstream_health_request_future = async move {
                let upstream_status = reqwest::get(upstream_health_endpoint)
                    .await
                    .map(|res| res.status())
                    .unwrap_or_else(|e| {
                        tracing::error!("Failed to get upstream status: {}", e);
                        StatusCode::INTERNAL_SERVER_ERROR
                    });
                Ok(Response::builder()
                    .status(upstream_status)
                    .body(Body::empty())
                    .unwrap())
            };
            Box::pin(upstream_health_request_future)
        } else {
            Box::pin(self.inner.call(req))
        }
    }
}
