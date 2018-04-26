use std::path::Path;

use std::sync::Arc;
use std::rc::Rc;
use std::cell::Cell;

use common::id::{DataObjectId, SubworkerId, TaskId};
use common::convert::FromCapnp;
use common::{Attributes, DataType};
use worker::{State, StateRef};
use worker::data::{Data, Storage};
use worker::rpc::subworker_serde::{DataObjectSpec, DataLocation};

use errors::Result;

pub fn data_output_from_spec(state: &State, subworker_dir: &Path, spec: DataObjectSpec) -> Result<Arc<Data>>
{
    let data_type = DataType::Blob; // TODO: Load data type
    match spec.location.unwrap() {
        DataLocation::Memory(data) => Ok(Arc::new(Data::new(
            Storage::Memory(data),
            data_type,
        ))),
        DataLocation::Path(data) => {
            let source_path = Path::new(&data);
            if !source_path.is_absolute() {
                bail!("Path of dataobject is not absolute");
            }
            if !source_path.starts_with(subworker_dir) {
                bail!("Path of dataobject is not in subworker dir");
            }
            let work_dir = state.work_dir();
            let target_path = work_dir.new_path_for_dataobject();
            Ok(Arc::new(Data::new_by_fs_move(
                &Path::new(source_path),
                &::std::fs::metadata(source_path)?,
                target_path,
                work_dir.data_path(),
            )?))
        }
        DataLocation::OtherObject(object_id) => {
            let object = state.object_by_id(object_id)?;
            let data = object.get().data().clone();
            Ok(data)
        },
        DataLocation::Cached => {
            bail!("Cached result occured in result")
        }
    }
}