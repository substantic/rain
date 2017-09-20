use std::sync::Arc;
use common::convert::ToCapnp;
use futures::Future;
use capnp::capability::Promise;
use common::convert::FromCapnp;
use common::id::DataObjectId;

use worker::graph::{Data};
use datastore_capnp::{reader, data_store, read_reply};
use worker::state::StateRef;


pub struct DataStoreImpl {
    state: StateRef,
}

impl DataStoreImpl {
    pub fn new(state: &StateRef) -> Self {
        Self { state: state.clone() }
    }
}


impl data_store::Server for DataStoreImpl {

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
            ReaderImpl::new(object.get().data().clone(), offset as usize)).from_server::<::capnp_rpc::Server>();

        let mut results = results.get();
        results.set_reader(reader);
        results.set_size(size);
        results.set_ok(());
        Promise::ok(())
    }

}


pub struct ReaderImpl {
    data: Arc<Data>,
    offset: usize,
}

impl ReaderImpl {
    pub fn new(data: Arc<Data>, offset: usize) -> Self {
        Self {
            data,
            offset,
        }
    }
}


impl reader::Server for ReaderImpl {

   fn read(
        &mut self,
        params: reader::ReadParams,
        mut results: reader::ReadResults,
    ) -> Promise<(), ::capnp::Error> {
       let param_size = pry!(params.get()).get_size() as usize;
       let mut results = results.get();
       let start = self.offset;
       let data_size = self.data.size();
       let (end, size, status) = if start + param_size < data_size {
           (start + param_size, param_size, read_reply::Status::Ok)
       } else {
           (data_size, data_size - start, read_reply::Status::Eof)
       };

       results.set_data(&self.data.as_slice().unwrap()[start..end]);
       results.set_status(if end < data_size {
           read_reply::Status::Ok
       } else {
           read_reply::Status::Eof
       });

       self.offset = end;
       Promise::ok(())
    }
}