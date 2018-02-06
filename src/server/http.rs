use hyper::{StatusCode, Error};
use hyper::header::{AccessControlAllowOrigin, ContentLength, ContentEncoding, Encoding};
use hyper::server::{Request, Response, Service};
use futures::Stream;
use futures;
use futures::Future;
use server::state::StateRef;
use common::logger::logger::SearchCriteria;
use errors::Result;

pub struct RequestHandler {
    state: ::server::state::StateRef
}

fn wrap_elements<I>(open_tag: &str, close_tag: &str, elements: I) -> String where I: IntoIterator<Item=String>
{
    let mut result = String::new();
    for e in elements.into_iter() {
        result.push_str(open_tag);
        result.push_str(&e);
        result.push_str(close_tag);
    }
    result
}

impl RequestHandler {

    pub fn new(state: &::server::state::StateRef) -> Self {
        Self {
            state: state.clone()
        }
    }
}


fn get_events(state: &StateRef, body: &str) -> Result<String> {
    let search_criteria: SearchCriteria = ::serde_json::from_str(body)?;
    let events = state.get().logger.get_events(&search_criteria);
    let chunks : Vec<String> = events.unwrap().iter().map(|&(id, time, ref event)| format!("{{\"id\":{}, \"time\":\"{}\", \"event\":{}}}", id, time, event)).collect();
    Ok(format!("[{}]", chunks.join(",")))
}

fn lite_dashboard(state: &StateRef) -> Result<String> {
    Ok(format!("<html>
    <style>
        table, th, td {{
            border: 1px solid black;
            border-collapse: collapse;
        }}
    </style>
    <body>
    <h1>Rain / Dashboard Lite</h1>
    <p>{time}</p>
    <h2>Workers</h2>
    <table>
    <thead><tr><th>ID<th>cpus</tr>
    </thead>
    {worker_tab}
    </table>
    </body>
    </html>",
    time=::chrono::Utc::now(),
    worker_tab=wrap_elements("<tr>", "</tr>",
        state.get().graph.workers.iter().map(|(id, ref wref)|
            format!("<td>{}</td><td>{}</td>", id, wref.get().resources.cpus))
    )))
}

pub fn make_text_response(data: Result<String>) -> Response {
    match data {
        Ok(data) => Response::new()
            .with_header(ContentLength(data.len() as u64))
            .with_header(AccessControlAllowOrigin::Any)
            .with_body(data),
        Err(e) => {
            warn!("Http request error: {}", e.description());
            Response::new()
                .with_status(StatusCode::InternalServerError)
                .with_header(AccessControlAllowOrigin::Any)
        }
    }
}

pub fn static_data_response(data: &'static [u8]) -> Response {
    Response::new()
        .with_header(ContentLength(data.len() as u64))
        .with_body(data)
}

pub fn static_gzipped_response(data: &'static [u8]) -> Response {
    Response::new()
        .with_header(ContentEncoding(vec![Encoding::Gzip]))
        .with_header(ContentLength(data.len() as u64))
        .with_body(data)
}


impl Service for RequestHandler {
    type Request = Request;
    type Response = Response;
    type Error = Error;

    type Future = Box<futures::Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let state_ref = self.state.clone();
        debug!("HTTP request: {}", req.path());
        let path = req.path().to_string();
        Box::new(req.body().concat2()
            .and_then(move |body| {
                let body = ::std::str::from_utf8(&body).unwrap();
                Ok(match path.as_str() {
                    "/events" => make_text_response(get_events(&state_ref, &body)),
                    "/lite" => make_text_response(lite_dashboard(&state_ref)),
                    // to protect against caching, .js contain hash in index.html, the same for .css file
                    path if path.starts_with("/static/js/main.") && path.ends_with(".js") =>
                        static_gzipped_response(&include_bytes!("./../../dashboard/dist/main.js.gz")[..]),
                    path if path.starts_with("/static/css/main.") && path.ends_with(".css") =>
                        static_gzipped_response(&include_bytes!("./../../dashboard/dist/main.css.gz")[..]),
                    path => static_data_response(&include_bytes!("./../../dashboard/build/index.html")[..]),
                    /*path =>  {
                        warn!("Invalid HTTP request: {}", path);
                        Response::new().with_status(StatusCode::NotFound)
                    }*/
                })
            }))
    }
}
