use std::path::Path;

use worker::StateRef;
use worker::graph::SubworkerRef;
use worker::data::{Data, DataType, Storage};
use worker::fs::workdir::WorkDir;
use subworker_capnp::subworker_upstream;
use futures::Future;
use capnp;
use capnp::capability::Promise;


use errors::Result;

use SUBWORKER_PROTOCOL_VERSION;

pub struct SubworkerUpstreamImpl {
    state: StateRef,
}

impl SubworkerUpstreamImpl {
    pub fn new(state: &StateRef) -> Self {
        Self { state: state.clone() }
    }
}

impl Drop for SubworkerUpstreamImpl {
    fn drop(&mut self) {
        panic!("Lost connection to subworker");
    }
}

impl subworker_upstream::Server for SubworkerUpstreamImpl {

    fn register(&mut self,
              params: subworker_upstream::RegisterParams,
              mut _results: subworker_upstream::RegisterResults)
              -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());

        if params.get_version() != SUBWORKER_PROTOCOL_VERSION {
            return Promise::err(capnp::Error::failed(
                format!("Invalid subworker protocol; expected = {}",
                        SUBWORKER_PROTOCOL_VERSION)));
        }

        let subworker_type = pry!(params.get_subworker_type());
        let control = pry!(params.get_control());

        let subworker = SubworkerRef::new(params.get_subworker_id(),
                                          subworker_type.to_string(),
                                          control);

        pry!(self.state.get_mut()
            .add_subworker(subworker)
            .map_err(|e| ::capnp::Error::failed(e.description().into())));
        Promise::ok(())
    }
}

pub fn data_from_capnp(work_dir: &WorkDir, reader: &::capnp_gen::subworker_capnp::local_data::Reader) -> Result<Data>
{
    let data_type = reader.get_type()?;
    assert!(data_type == ::capnp_gen::common_capnp::DataObjectType::Blob);
    match reader.get_storage().which()? {
        ::capnp_gen::subworker_capnp::local_data::storage::Memory(data) =>
            Ok(Data::new(DataType::Blob, Storage::Memory(data?.into()))),
        ::capnp_gen::subworker_capnp::local_data::storage::Path(data) => {
            let target_path = work_dir.new_path_for_dataobject();
            Ok(Data::new_by_fs_move(&Path::new(data?), target_path)?)
        },
        _ => unimplemented!()
    }
}