use hyper::StatusCode;
use hyper::service::Service;
use hyper::{Method, Request, Response, body::Incoming};
use reqwest::Url;
use s3s::{Body, HttpError};
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use crate::telemetry::{RequestDuration, ResponseBodySize};
use crate::{s3_operation_name, telemetry};

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

impl<S> S3CachingServiceProxy<S>
where
    S: Service<Request<Incoming>, Response = Response<Body>, Error = HttpError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    async fn call_with_server_telemetry<F, Fut>(
        req: Request<Incoming>,
        f: F,
    ) -> Result<Response<Body>, HttpError>
    where
        F: FnOnce(Request<Incoming>) -> Fut + Send,
        Fut: Future<Output = Result<Response<Body>, HttpError>> + Send,
    {
        let op_name = s3_operation_name(&req).unwrap_or("Unknown");

        let method = req.method().to_string();
        let scheme = req.uri().scheme_str().map(str::to_owned);

        let start = Instant::now();
        let result = f(req).await;
        let duration = start.elapsed();

        let status_code: Option<u16> = match &result {
            Ok(res) => Some(res.status().as_u16()),
            Err(_) => None,
        };

        let body_size = result
            .as_ref()
            .ok()
            .and_then(|res| res.body().bytes())
            .map(|bytes| bytes.len())
            .unwrap_or(0) as u64;

        let response_body_size = ResponseBodySize {
            version: "1.1",
            method: method.clone(),
            scheme: scheme.clone(),
            status_code,
            size: body_size,
        };

        telemetry::record_server_response_body_size(response_body_size, op_name);

        let request_duration = RequestDuration {
            version: "1.1",
            method,
            scheme,
            status_code,
            duration,
        };

        telemetry::record_server_request_duration(request_duration, op_name);

        result
    }
}

impl<S> Service<Request<Incoming>> for S3CachingServiceProxy<S>
where
    S: Service<Request<Incoming>, Response = Response<Body>, Error = HttpError>
        + Clone
        + Send
        + 'static,
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
            let s3 = self.inner.clone();
            Box::pin(Self::call_with_server_telemetry(req, move |req| {
                s3.call(req)
            }))
        }
    }
}
