use hyper::Error;
use hyper::header::ContentLength;
use hyper::server::{Request, Response, Service};
use futures;

pub struct RequestHandler;

const PHRASE: &'static str = "Welcome to RAIN Dashboard";

impl Service for RequestHandler {
    type Request = Request;
    type Response = Response;
    type Error = Error;

    type Future = Box<futures::Future<Item=Self::Response, Error=Self::Error>>;

    fn call(&self, _req: Request) -> Self::Future {
        Box::new(futures::future::ok(
            Response::new()
                .with_header(ContentLength(PHRASE.len() as u64))
                .with_body(PHRASE)
        ))
    }
}