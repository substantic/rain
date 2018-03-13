use capnp::capability::Promise;
use common::convert::FromCapnp;
use common::id::DataObjectId;
use worker::data::{new_pack_stream, PackStream};

use datastore_capnp::{data_store, read_reply, reader};
use worker::state::StateRef;

pub struct DataStoreImpl {
    state: StateRef,
}

impl DataStoreImpl {
    pub fn new(state: &StateRef) -> Self {
        Self {
            state: state.clone(),
        }
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
        let state = self.state.get_mut();
        let object = match state.object_by_id(id) {
            Ok(o) => o,
            Err(_) => {
                debug!("Worker responding 'not here' for id={}", id);
                let mut results = results.get();
                results.set_not_here(());
                return Promise::ok(());
            }
        };
        let size = object.get().size.map(|s| s as i64).unwrap_or(-1i64);

        let offset = params.get_offset();

        assert!(offset == 0); // TODO: implement for different offset

        let data = object.get().data().clone();
        let data_type = data.data_type();
        let pack_stream = new_pack_stream(&state, data).unwrap();
        let reader = reader::ToClient::new(ReaderImpl::new(pack_stream))
            .from_server::<::capnp_rpc::Server>();

        let mut results = results.get();
        results.set_reader(reader);
        results.set_size(size);
        results.set_ok(());
        results.set_data_type(data_type.to_capnp());
        Promise::ok(())
    }
}

pub struct ReaderImpl {
    pack_stream: Box<PackStream>,
}

impl ReaderImpl {
    pub fn new(pack_stream: Box<PackStream>) -> Self {
        Self { pack_stream }
    }
}

impl reader::Server for ReaderImpl {
    fn read(
        &mut self,
        params: reader::ReadParams,
        mut results: reader::ReadResults,
    ) -> Promise<(), ::capnp::Error> {
        let param_size = pry!(params.get()).get_size() as usize;
        let (slice, eof) = self.pack_stream.read(param_size);
        let mut results = results.get();
        results.set_data(slice);
        results.set_status(if eof {
            read_reply::Status::Eof
        } else {
            read_reply::Status::Ok
        });

        Promise::ok(())
    }
}
