use hyper::Error;
use hyper::header::{AccessControlAllowOrigin, ContentLength};
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

fn lite_dashboard(state: &StateRef) -> String {
    format!("<html>
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
    ))
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
                let output = match path.as_str() {
                    "/events" => get_events(&state_ref, &body).unwrap(),
                    "/" => lite_dashboard(&state_ref),
                    _ =>  { "FIXME".to_string() }
                };
                Ok(Response::new()
                        .with_header(ContentLength(output.len() as u64))
                        .with_header(AccessControlAllowOrigin::Any)
                        .with_body(output))
            }))
    }
}
