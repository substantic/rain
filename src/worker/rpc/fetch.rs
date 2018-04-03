use futures::{future, Future};
use worker::data::{Data, DataBuilder};
use worker::State;
use errors::Error;

// TODO: Remove box when impl Trait
pub fn fetch_from_reader(
    state: &State,
    reader: ::datastore_capnp::reader::Client,
    builder: DataBuilder,
    size: Option<usize>,
) -> Box<Future<Item = Data, Error = Error>> {
    let state_ref = state.self_ref();
    let fetch_size = size.unwrap_or(1 << 20 /* 1 MB */);
    Box::new(future::loop_fn(
        (state_ref, builder),
        move |(state_ref, mut builder)| {
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
                            Ok(future::Loop::Continue((state_ref, builder)))
                        }
                        ::datastore_capnp::read_reply::Status::Eof => {
                            let state = state_ref.get();
                            Ok(future::Loop::Break(builder.build(state.work_dir())))
                        }
                    }
                })
        },
    ))
}
