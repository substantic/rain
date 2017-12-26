use common::convert::ToCapnp;
use futures::{Future, future, IntoFuture};
use capnp::capability::Promise;
use common::convert::FromCapnp;
use common::id::DataObjectId;
use common::id::SId;

use server::graph::{WorkerRef, DataObjectRef, DataObjectState};
use datastore_capnp::{reader, data_store, read_reply};
use server::state::StateRef;

use errors::{Error, ErrorKind};

/// Data store provided for clients
pub struct ClientDataStoreImpl {
    state: StateRef,
}

impl ClientDataStoreImpl {
    pub fn new(state: &StateRef) -> Self {
        Self { state: state.clone() }
    }
}

impl data_store::Server for ClientDataStoreImpl {
    fn create_reader(
        &mut self,
        params: data_store::CreateReaderParams,
        mut results: data_store::CreateReaderResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let id = DataObjectId::from_capnp(&pry!(params.get_id()));
        let object = match self.state.get().object_by_id_check_session(id) {
            Ok(t) => t,
            Err(Error(ErrorKind::SessionErr(ref e), _)) => {
                e.to_capnp(&mut results.get().init_error());
                return Promise::ok(());
            }
            Err(e) => return Promise::err(::capnp::Error::failed(e.description().to_string())),
        };
        let offset = params.get_offset();
        if object.get().state == DataObjectState::Removed {
            return Promise::err(::capnp::Error::failed(format!(
                "create_reader on removed object {:?}",
                object.get()
            )));
        }

        let state = self.state.clone();
        let state2 = state.clone();
        let object1 = object.clone();
        let object2 = object.clone();
        let object3 = object.clone();
        let object4 = object.clone();
        let session_id = id.get_session_id();
        let mut obj = object2.get_mut();
        let session = obj.session.clone();


        Promise::from_future(obj.wait()
            .then(move |r| -> future::Either<_, _> {
                if r.is_err() {
                    let session = session.get();
                    session.get_error().as_ref().unwrap().to_capnp(&mut results.get().init_error());
                    return future::Either::A(future::result(Ok(())));
                }
                let obj = object4.get();
                trace!("create_reader at server on {:?}", obj);
                if obj.state == DataObjectState::Removed {
                    let session = session.get();
                    session.get_error().as_ref().unwrap().to_capnp(&mut results.get().init_error());
                    return future::Either::A(future::result(Ok(())));
                }

                future::Either::B(future::lazy(move || {
                    let obj = object.get();
                    assert_eq!(obj.state, DataObjectState::Finished,
                               "triggered finish hook on unfinished object");
                    if obj.data.is_some() {
                        unimplemented!();
                    }
                    let worker = obj.located.iter().next().unwrap().clone();
                    let worker2 = worker.clone();
                    let handle = state.get().handle().clone();
                    let future = worker.get_mut().wait_for_datastore(&worker, &handle).map(move |()| worker2);
                    future
                }).and_then(move |worker| {
                    let worker = worker.get();
                    let datastore = worker.get_datastore();
                    let mut req = datastore.create_reader_request();
                    {
                        let mut params = req.get();
                        params.set_offset(offset);
                        id.to_capnp(&mut params.get_id().unwrap());
                    }
                    req.send().promise.map_err(|e| e.into())
                }).and_then(move |response| {
                    // TODO: Here we simply resend response from worker to client
                    // and fully utilize capnp. For resilience, we will probably need
                    // Some more sophisticated solution to cover worker crashes
                    let response = pry!(response.get());
                    pry!(results.set(response));
                    Promise::ok(())
                }))
            }).map_err(|e| panic!("Fetch failed: {:?}", e)))
    }
}

// Datastore provided for workers
pub struct WorkerDataStoreImpl {
    state_ref: StateRef,
    worker_ref: WorkerRef,
}

impl WorkerDataStoreImpl {
    pub fn new(state: &StateRef, worker_ref: &WorkerRef) -> Self {
        Self { state_ref: state.clone(), worker_ref: worker_ref.clone()  }
    }
}


impl data_store::Server for WorkerDataStoreImpl {
    fn create_reader(
        &mut self,
        params: data_store::CreateReaderParams,
        mut results: data_store::CreateReaderResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let id = DataObjectId::from_capnp(&pry!(params.get_id()));

        if self.worker_ref.get().is_object_ignored(&id) {
            results.get().set_ignored(());
            return Promise::ok(());
        }

        let object = if let Ok(o) = self.state_ref.get().object_by_id(id) {
            o
        } else {
            results.get().set_removed(());
            return Promise::ok(());
        };
        let size = object.get().size.map(|s| s as i64).unwrap_or(-1i64);

        let offset = params.get_offset();
        let reader = reader::ToClient::new(LocalReaderImpl::new(object, offset as usize))
            .from_server::<::capnp_rpc::Server>();

        let mut results = results.get();
        results.set_reader(reader);
        results.set_size(size);
        results.set_ok(());
        Promise::ok(())
    }
}



/// The implementation of reader that reads object
/// that is localy stored in server
pub struct LocalReaderImpl {
    object: DataObjectRef,
    offset: usize,
    size: usize,
}

impl LocalReaderImpl {
    pub fn new(object: DataObjectRef, offset: usize) -> Self {
        let size = object.get().size.unwrap();
        Self {
            object,
            offset,
            size,
        }
    }
}


impl reader::Server for LocalReaderImpl {
    fn read(
        &mut self,
        params: reader::ReadParams,
        mut results: reader::ReadResults,
    ) -> Promise<(), ::capnp::Error> {
        let param_size = pry!(params.get()).get_size() as usize;
        let mut results = results.get();
        let start = self.offset;
        let (end, size, status) = if start + param_size < self.size {
            (start + param_size, param_size, read_reply::Status::Ok)
        } else {
            (self.size, self.size - start, read_reply::Status::Eof)
        };

        results.set_data(&self.object.get().data.as_ref().unwrap()[start..end]);
        results.set_status(if end < self.size {
            read_reply::Status::Ok
        } else {
            read_reply::Status::Eof
        });

        self.offset = end;
        Promise::ok(())
    }
}
