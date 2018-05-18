use std::path::Path;

use std::sync::Arc;

use bytes::BytesMut;
use common::DataType;
use common::id::SubworkerId;
use worker::State;
use worker::data::{Data, Storage};
use worker::rpc::subworker_serde::SubworkerToWorkerMessage;
use worker::rpc::subworker_serde::{DataLocation, DataObjectSpec};

use errors::Result;

static PROTOCOL_VERSION: &'static str = "cbor-1";

pub fn data_output_from_spec(
    state: &State,
    subworker_dir: &Path,
    spec: DataObjectSpec,
    data_type: DataType,
) -> Result<Arc<Data>> {
    match spec.location.unwrap() {
        DataLocation::Memory(data) => Ok(Arc::new(Data::new(Storage::Memory(data), data_type))),
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
        }
        DataLocation::Cached => bail!("Cached result occured in result"),
    }
}

pub fn check_registration(
    data: Option<BytesMut>,
    subworker_id: SubworkerId,
    subworker_type: &str,
) -> Result<()> {
    match data {
        Some(data) => {
            let message: SubworkerToWorkerMessage = ::serde_cbor::from_slice(&data).unwrap();
            if let SubworkerToWorkerMessage::Register(msg) = message {
                debug!(
                    "Subworker id={} registered: protocol={} id={} type={}",
                    subworker_id, msg.protocol, msg.subworker_id, msg.subworker_type
                );
                if msg.protocol != PROTOCOL_VERSION {
                    bail!(
                        "Subworker error: registered with invalid protocol; expected = {}",
                        PROTOCOL_VERSION
                    );
                }
                if msg.subworker_id != subworker_id {
                    bail!("Subworker error: registered with invalid id");
                }
                if msg.subworker_type != subworker_type {
                    bail!("Subworker error: registered with invalid type");
                }
            } else {
                bail!("Subworker error: Not registered by first message");
            }
        }
        None => bail!("Subworker error: Closed connection without registration"),
    };
    Ok(())
}
