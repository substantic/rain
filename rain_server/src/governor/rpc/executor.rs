use std::path::Path;

use std::sync::Arc;

use bytes::BytesMut;
use common::id::ExecutorId;
use common::DataType;
use governor::data::{Data, Storage};
use governor::rpc::executor_serde::ExecutorToGovernorMessage;
use governor::rpc::executor_serde::{DataLocation, LocalObjectOut};
use governor::State;

use errors::Result;

static PROTOCOL_VERSION: &'static str = "cbor-1";

pub fn data_output_from_spec(
    state: &State,
    executor_dir: &Path,
    lo: LocalObjectOut,
    data_type: DataType,
) -> Result<Arc<Data>> {
    match lo.location.unwrap() {
        DataLocation::Memory(data) => Ok(Arc::new(Data::new(Storage::Memory(data), data_type))),
        DataLocation::Path(data) => {
            let source_path = Path::new(&data);
            if !source_path.is_absolute() {
                bail!("Path of dataobject is not absolute");
            }
            if !source_path.starts_with(executor_dir) {
                bail!("Path of dataobject is not in executor dir");
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
    executor_id: ExecutorId,
    executor_type: &str,
) -> Result<()> {
    match data {
        Some(data) => {
            let message: ExecutorToGovernorMessage = ::serde_cbor::from_slice(&data).unwrap();
            if let ExecutorToGovernorMessage::Register(msg) = message {
                debug!(
                    "Executor id={} registered: protocol={} id={} type={}",
                    executor_id, msg.protocol, msg.executor_id, msg.executor_type
                );
                if msg.protocol != PROTOCOL_VERSION {
                    bail!(
                        "Executor error: registered with invalid protocol; expected = {}",
                        PROTOCOL_VERSION
                    );
                }
                if msg.executor_id != executor_id {
                    bail!("Executor error: registered with invalid id");
                }
                if msg.executor_type != executor_type {
                    bail!("Executor error: registered with invalid type");
                }
            } else {
                bail!("Executor error: Not registered by first message");
            }
        }
        None => bail!("Executor error: Closed connection without registration"),
    };
    Ok(())
}
