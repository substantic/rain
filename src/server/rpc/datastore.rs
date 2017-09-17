use common::convert::ToCapnp;
use futures::Future;
use capnp::capability::Promise;
use common::convert::FromCapnp;
use common::id::DataObjectId;

use server::graph::{DataObjectRef};
use datastore_capnp::{reader, data_store, read_reply};
use server::state::StateRef;

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
        let object = pry!(self.state.get().object_by_id(id));
        let offset = params.get_offset();

        let state = self.state.clone();
        let object1 = object.clone();
        let object2 = object.clone();
        let object3 = object.clone();
        let mut obj = object2.get_mut();
        Promise::from_future(obj.wait()
            .map_err(|_| "Cancelled".into())
            .and_then(move |()| {
                let obj = object.get();
                //assert!(obj.is_finished());
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
               results.set(response);
               Promise::ok(())
            }).map_err(|e| panic!("Fetch failed: {:?}", e)))
    }

}

// Datastore provided for workers
pub struct WorkerDataStoreImpl {
    state: StateRef,
}

impl WorkerDataStoreImpl {
    pub fn new(state: &StateRef) -> Self {
        Self { state: state.clone() }
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
        let object = pry!(self.state.get().object_by_id(id));
        let size = object.get().size.map(|s| s as i64).unwrap_or(-1i64);

        let offset = params.get_offset();
        let reader = reader::ToClient::new(
            LocalReaderImpl::new(object, offset as usize)).from_server::<::capnp_rpc::Server>();

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
    size: usize
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