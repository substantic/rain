
use futures::{Future, future};

use common::convert::ToCapnp;
use worker::graph::DataObjectRef;
use worker::data::{DataBuilder, Data};
use errors::{ErrorKind, Error};


pub fn fetch_from_datastore(
    dataobj: &DataObjectRef,
    datastore: &::datastore_capnp::data_store::Client,
) -> Box<Future<Item = Data, Error = Error>> {
    let mut req = datastore.create_reader_request();

    {
        let mut params = req.get();
        params.set_offset(0);
        dataobj.get().id.to_capnp(&mut params.get_id().unwrap());
    }

    // TODO: Extend also to directory builder

    debug!("Fetching object id={}", dataobj.get().id);

    Box::new(
        req.send()
            .promise
            .map_err(|e| Error::with_chain(e, "Send failed"))
            .and_then(|r| {
                {
                    let response = r.get().unwrap();

                    match response.which().unwrap() {
                        ::datastore_capnp::reader_response::Which::Ok(()) => { /* just continue */ }
                        _ => bail!("Reader not obtaind"),
                    }
                }
                Ok(r)
            })
            .and_then(|r| {
                let response = r.get().unwrap();
                let size = response.get_size();
                let reader = response.get_reader().unwrap();
                let fetch_size;

                let mut builder = DataBuilder::new();
                // TODO: If size is too big, do not download everything at once
                if size == -1 {
                    // TODO: Do some benchmark for this constant
                    fetch_size = 1 << 20; // 1 MB
                } else {
                    fetch_size = size as usize;
                    builder.set_size(size as usize);
                }

                future::loop_fn(builder, move |mut builder| {
                    let mut req = reader.read_request();
                    req.get().set_size(fetch_size as u64);
                    req.send()
                        .promise
                        .map_err(|e| Error::with_chain(e, "Read failed"))
                        .and_then(move |r| {
                            let read = r.get().unwrap();
                            builder.write(read.get_data().unwrap());
                            match read.get_status().unwrap() {
                                ::datastore_capnp::read_reply::Status::Ok => Ok(
                                    future::Loop::Continue(builder),
                                ),
                                ::datastore_capnp::read_reply::Status::Eof => Ok(
                                    future::Loop::Break(
                                        builder.build(),
                                    ),
                                ),
                            }
                        })
                })
            }),
    )
}
