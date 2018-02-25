use std::path::Path;

use std::sync::Arc;
use std::rc::Rc;
use std::cell::Cell;

use common::id::{DataObjectId, SubworkerId};
use common::convert::FromCapnp;
use worker::{State, StateRef};
use worker::data::{Data, Storage};
use subworker_capnp::subworker_upstream;
use capnp;
use capnp::capability::Promise;

use errors::Result;

use SUBWORKER_PROTOCOL_VERSION;

pub struct SubworkerUpstreamImpl {
    state: StateRef,
    subworker_id: Rc<Cell<Option<SubworkerId>>>,
}

impl SubworkerUpstreamImpl {
    pub fn new(state: &StateRef) -> Self {
        Self {
            state: state.clone(),
            subworker_id: Rc::new(Cell::new(None)),
        }
    }

    pub fn subworker_id_rc(&self) -> Rc<Cell<Option<SubworkerId>>> {
        self.subworker_id.clone()
    }
}

impl Drop for SubworkerUpstreamImpl {
    fn drop(&mut self) {
        debug!("SubworkerUpstream closed");
    }
}

impl subworker_upstream::Server for SubworkerUpstreamImpl {
    fn register(
        &mut self,
        params: subworker_upstream::RegisterParams,
        mut _results: subworker_upstream::RegisterResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());

        if params.get_version() != SUBWORKER_PROTOCOL_VERSION {
            return Promise::err(capnp::Error::failed(format!(
                "Invalid subworker protocol; expected = {}",
                SUBWORKER_PROTOCOL_VERSION
            )));
        }

        let subworker_id = params.get_subworker_id();
        self.subworker_id.set(Some(subworker_id));
        let subworker_type = pry!(params.get_subworker_type());
        let control = pry!(params.get_control());

        pry!(
            self.state
                .get_mut()
                .add_subworker(subworker_id, subworker_type.to_string(), control)
                .map_err(|e| ::capnp::Error::failed(e.description().into()))
        );
        Promise::ok(())
    }
}

pub fn data_from_capnp(
    state: &State,
    subworker_dir: &Path,
    reader: &::subworker_capnp::local_data::Reader,
) -> Result<Arc<Data>> {
    match reader.get_storage().which()? {
        ::subworker_capnp::local_data::storage::Memory(data) => {
            Ok(Arc::new(Data::new(Storage::Memory(data?.into()))))
        }
        ::subworker_capnp::local_data::storage::Path(data) => {
            let source_path = Path::new(data?);
            if !source_path.is_absolute() {
                bail!("Path of dataobject is not absolute");
            }
            if !source_path.starts_with(subworker_dir) {
                bail!("Path of dataobject is not in subworker dir");
            }
            let target_path = state.work_dir().new_path_for_dataobject();
            Ok(Arc::new(Data::new_by_fs_move(
                &Path::new(source_path),
                target_path,
            )?))
        }
        ::subworker_capnp::local_data::storage::InWorker(data) => {
            let object_id = DataObjectId::from_capnp(&data?);
            let object = state.object_by_id(object_id)?;
            let data = object.get().data().clone();
            Ok(data)
        }
        _ => unimplemented!(),
    }
}
