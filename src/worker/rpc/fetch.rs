use futures::{future, Future};
use worker::data::{Data, DataBuilder};
use errors::Error;

// TODO: Remove box when impl Trait
pub fn fetch_from_reader(
    reader: ::datastore_capnp::reader::Client,
    size: Option<usize>,
) -> Box<Future<Item = Data, Error = Error>> {
    let builder = DataBuilder::new();
    let fetch_size = size.unwrap_or(1 << 20 /* 1 MB */);
    Box::new(future::loop_fn(builder, move |mut builder| {
        let mut req = reader.read_request();
        req.get().set_size(fetch_size as u64);
        req.send()
            .promise
            .map_err(|e| Error::with_chain(e, "Read failed"))
            .and_then(move |r| {
                let read = r.get().unwrap();
                builder.write(read.get_data().unwrap());
                match read.get_status().unwrap() {
                    ::datastore_capnp::read_reply::Status::Ok => {
                        Ok(future::Loop::Continue(builder))
                    }
                    ::datastore_capnp::read_reply::Status::Eof => {
                        Ok(future::Loop::Break(builder.build()))
                    }
                }
            })
    }))
}
