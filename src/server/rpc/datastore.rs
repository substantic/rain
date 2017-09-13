use capnp::capability::Promise;
use common::convert::FromCapnp;
use common::id::DataObjectId;

use server::graph::{DataObjectRef};
use datastore_capnp::{reader, data_store, read_reply};
use server::state::StateRef;


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
/// This is counter-part of RemoteReaderImpl
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

pub struct RemoteReaderImpl {
    // TODO
}