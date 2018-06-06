use std::rc::Rc;

use common::convert::FromCapnp;
use common::convert::ToCapnp;
use common::id::{GovernorId};
use errors::{Error, ErrorKind};
use futures::{future, Future};
use governor::{StateRef};
use governor::graph::DataObjectRef;
use governor::data::{Data, DataBuilder};

use futures::IntoFuture;
use futures::future::Either;

pub struct FetchContext {
    pub state_ref: StateRef,
    pub dataobj_ref: DataObjectRef,
    pub remote: Option<Rc<::governor_capnp::governor_bootstrap::Client>>,
    pub builder: Option<DataBuilder>,
    pub offset: usize,
    pub size: usize,
    pub n_redirects: i32,
}

pub fn fetch(context: FetchContext) -> Box<Future<Item = Data, Error = Error>> {
    Box::new(future::lazy(move || {
        future::loop_fn(context, |mut context| {
            let id = context.dataobj_ref.get().spec.id;
            let fetch_size = 4 << 20; // 4 MB
            let state_ref = context.state_ref.clone();
            let state = state_ref.get();
            let send = match context.remote {
                Some(ref r) => {
                    // fetch from governor
                    let mut req = r.fetch_request();
                    {
                        let mut request = req.get();
                        request.set_offset(context.offset as u64);
                        request.set_size(fetch_size as u64);
                        request.set_include_info(context.builder.is_none());
                        id.to_capnp(&mut request.get_id().unwrap());
                    }
                    req.send()
                }
                None => {
                    // fetch from server
                    state
                        .upstream()
                        .as_ref()
                        .map(|upstream| {
                            let mut req = upstream.fetch_request();
                            {
                                let mut request = req.get();
                                request.set_offset(context.offset as u64);
                                request.set_size(fetch_size as u64);
                                request.set_include_info(context.builder.is_none());
                                id.to_capnp(&mut request.get_id().unwrap());
                            }
                            req.send()
                        })
                        .unwrap()
                }
            };
            send.promise
                .map_err(|e| Error::with_chain(e, "Fetch failed"))
                .and_then(move |r| {
                    let response = r.get().unwrap();
                    let state_ref = context.state_ref.clone();
                    let mut state = state_ref.get_mut();
                    match response.get_status().which().unwrap() {
                        ::common_capnp::fetch_result::status::Ok(()) => {
                            if context.builder.is_none() {
                                let mut dataobj = context.dataobj_ref.get_mut();
                                dataobj.info = ::serde_json::from_str(response.get_info().unwrap()).unwrap();
                                context.size = dataobj.info.size.unwrap();
                                context.builder =
                                    Some(DataBuilder::new(state.work_dir(), dataobj.spec.data_type, dataobj.info.size))
                            };
                            let result = {
                                let builder = context.builder.as_mut().unwrap();
                                let data = response.get_data().unwrap().into();
                                builder.write(data);
                                context.offset += data.len();
                                if context.offset < context.size {
                                    None
                                } else {
                                    Some(builder.build(state.work_dir()))
                                }
                            };
                            Either::A(
                                Ok(result
                                    .map(future::Loop::Break)
                                    .unwrap_or_else(|| future::Loop::Continue(context)))
                                    .into_future(),
                            )
                        }
                        ::common_capnp::fetch_result::status::NotHere(()) => {
                            assert!(context.remote.is_some()); // The response is NOT from server
                                                               // Let us ask server
                            context.remote = None;
                            Either::A(Ok(future::Loop::Continue(context)).into_future())
                        }
                        ::common_capnp::fetch_result::status::Redirect(w) => {
                            assert!(context.remote.is_none()); // The response is from the server

                            context.n_redirects += 1;
                            if context.n_redirects > 32 {
                                panic!("Too many redirections of fetch");
                            }
                            let governor_id = GovernorId::from_capnp(&w.unwrap());
                            Either::B(state.wait_for_remote_governor(&governor_id).and_then(
                                move |remote_governor| {
                                    context.remote = Some(remote_governor);
                                    Ok(future::Loop::Continue(context))
                                },
                            ))
                        }
                        ::common_capnp::fetch_result::status::Ignored(()) => {
                            assert!(context.remote.is_none()); // The response is from the server
                            debug!("Datastore ignore occured; id={}", id);
                            Either::A(
                                Err(Error::from(ErrorKind::Ignored))
                                    .into_future()
                                    .into_future(),
                            )
                        }
                        _ => {
                            panic!(
                                "Invalid status of fetch received, dataobject id={}",
                                id
                            );
                        }
                    }
                })
        })
    }))
}
