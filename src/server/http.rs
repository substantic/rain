use hyper::Error;
use hyper::header::ContentLength;
use hyper::server::{Request, Response, Service};
use futures;

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

    pub fn lite_dashboard(&self) -> String {
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
            self.state.get().graph.workers.iter().map(|(id, ref wref)|
                format!("<td>{}</td><td>{}</td>", id, wref.get().resources.cpus))
        ))
    }
}


impl Service for RequestHandler {
    type Request = Request;
    type Response = Response;
    type Error = Error;

    type Future = Box<futures::Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, _req: Request) -> Self::Future {
        let output = self.lite_dashboard();
        Box::new(futures::future::ok(
            Response::new()
                .with_header(ContentLength(output.len() as u64))
                .with_body(output),
        ))
    }
}
