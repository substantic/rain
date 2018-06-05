use super::{Graph, TaskRef};
use common::id::{GovernorId};
use common::wrapped::WrappedRcRefCell;
use common::{RcSet, ObjectSpec, ObjectInfo};
use errors::{ErrorKind, Result};
use governor::WorkDir;
use governor::data::Data;
use governor::graph::ExecutorRef;
use governor::rpc::executor_serde::{DataLocation, LocalObjectSpec};

use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;


#[derive(Debug)]
pub enum DataObjectState {
    Assigned,
    Remote(SocketAddr),

    /// Data transfer is in progress; if oneshot is finished or dropped then pulling is
    /// canceled
    Pulling((SocketAddr, ::futures::unsync::oneshot::Sender<()>)),
    Finished(Arc<Data>),
    Removed,
}

#[derive(Debug)]
pub struct DataObject {
    pub(in super::super) spec: ObjectSpec,

    pub(in super::super) state: DataObjectState,

    pub(in super::super) info: ObjectInfo,

    /// Task that produces data object; it is None if task not computed in this governor
    // producer: Option<Task>,
    /// Consumer set, e.g. to notify of completion.
    pub(in super::super) consumers: RcSet<TaskRef>,

    pub(in super::super) assigned: bool,

    /// Where are data object cached
    pub(in super::super) executor_cache: RcSet<ExecutorRef>,
}

pub type DataObjectRef = WrappedRcRefCell<DataObject>;

impl DataObject {
    pub fn set_as_removed(&mut self) {
        self.state = DataObjectState::Removed;
    }

    pub fn set_data(&mut self, data: Arc<Data>) -> Result<()> {
        if self.is_removed() {
            return Ok(());
        }

        assert!(!self.is_finished());

        if self.spec.data_type != data.data_type() {
            bail!(
                "Output '{}' (content_type={}) expects type {}, but {} is provided",
                self.spec.label,
                self.display_content_type(),
                self.spec.data_type,
                data.data_type(),
            )
        }
        self.info.size = Some(data.size());
        self.state = DataObjectState::Finished(data);
        Ok(())
    }

    pub fn set_info(&mut self, info: ObjectInfo) {
        self.info = info;
    }

    pub fn display_content_type(&self) -> String {
        self.content_type().clone().unwrap_or_else(|| "<None>".to_string())
    }

    pub fn content_type(&self) -> &Option<String> {
        &self.spec.content_type
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        match self.state {
            DataObjectState::Finished(_) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_removed(&self) -> bool {
        match self.state {
            DataObjectState::Removed => true,
            _ => false,
        }
    }

    pub fn data(&self) -> &Arc<Data> {
        match self.state {
            DataObjectState::Finished(ref data) => data,
            _ => panic!("DataObject is not finished"),
        }
    }

    pub fn remote(&self) -> Option<GovernorId> {
        match self.state {
            DataObjectState::Remote(ref addr) | DataObjectState::Pulling((ref addr, _)) => {
                Some(*addr)
            }
            _ => None,
        }
    }

    pub fn set_data_by_fs_move(
        &mut self,
        source_path: &Path,
        info_path: Option<&str>,
        work_dir: &WorkDir,
    ) -> Result<()> {
        let metadata = ::std::fs::metadata(source_path).map_err(|_| {
            ErrorKind::Msg(format!(
                "Path '{}' now found.",
                info_path.unwrap_or_else(|| source_path.to_str().unwrap())
            ))
        })?;
        let target_path = work_dir.new_path_for_dataobject();
        let data = Data::new_by_fs_move(source_path, &metadata, target_path, work_dir.data_path())?;
        self.set_data(Arc::new(data))
    }

    pub fn create_input_spec(&self, executor_ref: &ExecutorRef) -> LocalObjectSpec {
        LocalObjectSpec {
            spec: self.spec.clone(),
            location: if self.executor_cache.contains(executor_ref) {
                Some(DataLocation::Cached)
            } else {
                Some(self.data().create_location())
            },
            cache_hint: true,
        }
    }

    pub fn create_output_spec(&self) -> LocalObjectSpec {
        LocalObjectSpec {
            spec: self.spec.clone(),
            location: None,
            cache_hint: true,
        }
    }
}

impl DataObjectRef {
    pub fn new(
        graph: &mut Graph,
        spec: ObjectSpec,
        state: DataObjectState,
        assigned: bool,
    ) -> Self {
        debug!("New object id={}", spec.id);

        match graph.objects.entry(spec.id) {
            ::std::collections::hash_map::Entry::Vacant(e) => {
                let dataobj = Self::wrap(DataObject {
                    spec,
                    info: Default::default(),
                    state,
                    assigned,
                    consumers: Default::default(),
                    executor_cache: Default::default(),
                });
                e.insert(dataobj.clone());
                dataobj
            }
            ::std::collections::hash_map::Entry::Occupied(e) => {
                let dataobj = e.get().clone();
                {
                    let obj = dataobj.get();
                    // TODO: If object is remote and not finished and new remote obtained,
                    // then update remote
                    assert!(obj.spec.id == spec.id);
                }
                dataobj
            }
        }
    }
}

impl fmt::Debug for DataObjectRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DataObjectRef {}", self.get().spec.id)
    }
}
