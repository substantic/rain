
use common::id::{DataObjectId, WorkerId};
use common::keeppolicy::KeepPolicy;
use common::wrapped::WrappedRcRefCell;
use common::{Attributes, RcSet};
use super::{TaskRef, Graph};
use worker::data::{Data};
use worker::fs::workdir::WorkDir;

use std::net::SocketAddr;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::path::Path;
use std::fmt;


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

    pub fn set_data(&mut self, data: Arc<Data>) {
        assert!(!self.is_finished());
        self.size = Some(data.size());
        self.state = DataObjectState::Finished(data);
    }

    pub fn set_attributes(&mut self, attributes: Attributes) {
        // TODO Check content type
        self.new_attributes = attributes;
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
            DataObjectState::Remote(ref addr) => Some(*addr),
            DataObjectState::Pulling(ref addr) => Some(*addr),
            _ => None,
        }
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

        match graph.objects.entry(id.clone()) {
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
