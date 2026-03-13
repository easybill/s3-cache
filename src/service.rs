use bytes::Bytes;
use hyper::service::Service;
use hyper::{Method, Request, Response, body::Incoming};
use s3s::{Body, HttpError};
use std::future::Future;
use std::pin::Pin;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// Wraps an S3 service and short-circuits `GET /` and `GET /health` requests,
/// returning `200 OK` with a plain-text `"Status OK"` body without forwarding
/// them to the S3 layer or requiring authentication.
pub struct S3CachingServiceProxy<S> {
    inner: S,
}

impl<S> S3CachingServiceProxy<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: Clone> Clone for S3CachingServiceProxy<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
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
        if req.method() == Method::GET && (req.uri().path() == "/" || req.uri().path() == "/health")
        {
            let response = Response::builder()
                .status(200)
                .body(Body::from(Bytes::from_static(b"Status OK")))
                .unwrap();
            Box::pin(std::future::ready(Ok(response)))
        } else {
            Box::pin(self.inner.call(req))
        }
    }
}
