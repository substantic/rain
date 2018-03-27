use common::id::{DataObjectId, WorkerId};
use common::wrapped::WrappedRcRefCell;
use common::{Attributes, RcSet};
use super::{Graph, TaskRef};
use worker::data::Data;
use worker::graph::SubworkerRef;
use worker::WorkDir;
use errors::{ErrorKind, Result};

use std::path::Path;
use std::net::SocketAddr;
use std::sync::Arc;
use std::fmt;

#[derive(Deserialize)]
pub struct DataObjectAttributeSpec {
    pub content_type: Option<String>,
}

#[derive(Debug)]
pub enum DataObjectState {
    Assigned,
    Remote(SocketAddr),

    /// Data object is remote, but we currently don't know where they are placed; however
    /// server was asked for real data placement
    /// This state can happen when remote worker replied by "notHere"
    RemoteRedirecting,

    /// Data transfer is in progress
    Pulling(SocketAddr),

    Finished(Arc<Data>),
}

#[derive(Debug)]
pub struct DataObject {
    pub(in super::super) id: DataObjectId,

    pub(in super::super) state: DataObjectState,

    /// Task that produces data object; it is None if task not computed in this worker
    // producer: Option<Task>,
    /// Consumer set, e.g. to notify of completion.
    pub(in super::super) consumers: RcSet<TaskRef>,

    pub(in super::super) assigned: bool,

    /// Where are data object cached
    pub(in super::super) subworker_cache: RcSet<SubworkerRef>,

    /// ??? Is this necessary for worker?
    pub(in super::super) size: Option<usize>,

    /// Label may be the role that the output has in the `producer`, or it may be
    /// the name of the initial uploaded object.
    pub(in super::super) label: String,

    pub(in super::super) attributes: Attributes,

    pub(in super::super) new_attributes: Attributes,
}

pub type DataObjectRef = WrappedRcRefCell<DataObject>;

impl DataObject {

    pub fn set_data(&mut self, data: Arc<Data>) -> Result<()> {
        assert!(!self.is_finished());

        let is_dir = self.content_type().map(|c| c == "dir");
        match is_dir {
            Some(true) => {
                if !data.is_directory() {
                    bail!("Output '{}' has content 'dir', but blob is provided", self.label)
                }
            },
            Some(false) => {
                if !data.is_blob() {
                    let ct = self.content_type().unwrap_or_else(|| "<None>".to_string());
                    bail!("Output '{}' has content '{}', but directory is provided", self.label, ct)
                }
            }
            None => { /* No check */ }
        }
        self.size = Some(data.size());
        self.state = DataObjectState::Finished(data);
        Ok(())
    }

    pub fn set_attributes(&mut self, attributes: Attributes) {
        // TODO Check content type
        self.new_attributes = attributes;
    }

    pub fn content_type(&self) -> Option<String> {
        self.attributes
            .get("spec")
            .map(|spec: DataObjectAttributeSpec| spec.content_type)
            .unwrap_or(None)
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        match self.state {
            DataObjectState::Finished(_) => true,
            _ => false,
        }
    }

    pub fn data(&self) -> &Arc<Data> {
        match self.state {
            DataObjectState::Finished(ref data) => data,
            _ => panic!("DataObject is not finished"),
        }
    }

    pub fn remote(&self) -> Option<WorkerId> {
        match self.state {
            DataObjectState::Remote(ref addr) | DataObjectState::Pulling(ref addr) => Some(*addr),
            _ => None,
        }
    }

    pub fn set_data_by_fs_move(&mut self, source_path: &Path, info_path: Option<&str>, work_dir: &WorkDir) -> Result<()> {
        let metadata = ::std::fs::metadata(source_path).map_err(|_| {
            ErrorKind::Msg(format!("Path '{}' now found.", info_path.unwrap_or_else(|| source_path.to_str().unwrap())))
        })?;
        let target_path = work_dir.new_path_for_dataobject();
        let data = Data::new_by_fs_move(source_path, &metadata, target_path, work_dir.data_path())?;
        self.set_data(Arc::new(data))
    }
}

impl DataObjectRef {
    pub fn new(
        graph: &mut Graph,
        id: DataObjectId,
        state: DataObjectState,
        assigned: bool,
        size: Option<usize>,
        label: String,
        attributes: Attributes,
    ) -> Self {
        debug!("New object id={}", id);

        match graph.objects.entry(id) {
            ::std::collections::hash_map::Entry::Vacant(e) => {
                let dataobj = Self::wrap(DataObject {
                    id,
                    state,
                    size,
                    assigned,
                    consumers: Default::default(),
                    label,
                    attributes,
                    new_attributes: Attributes::new(),
                    subworker_cache: Default::default(),
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
                    assert!(obj.id == id);
                }
                dataobj
            }
        }
    }
}

impl fmt::Debug for DataObjectRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DataObjectRef {}", self.get().id)
    }
}
