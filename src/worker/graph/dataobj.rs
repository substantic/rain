
use common::id::{DataObjectId};
use common::keeppolicy::KeepPolicy;
use common::wrapped::WrappedRcRefCell;
use common::RcSet;
use super::{TaskRef, Graph};

use std::net::SocketAddr;
use std::cell::RefCell;
use std::rc::Rc;


pub enum DataObjectState {
    Assigned,
    Remote(SocketAddr),

    /// Data object is remote, but we currently don't know where they are placed; however
    /// server was asked for real data placement
    /// This state can happen when remote worker replied by "notHere"
    RemoteRedirecting,

    /// Data transfer is in progress
    Pulling(SocketAddr),

    FinishedInFile,
    // FinishedMmaped(XXX),
    FinishedInMem(Vec<u8>),
}

pub enum DataObjectType {
    Blob,
    Directory,
    Stream
}

pub struct DataObject {
    id: DataObjectId,

    state: DataObjectState,

    /// Task that produces data object; it is None if task not computed in this worker
    // producer: Option<Task>,

    /// Consumer set, e.g. to notify of completion.
    consumers: RcSet<TaskRef>,

    obj_type: DataObjectType,

    keep: KeepPolicy,

    size: Option<usize>,

    /// Label may be the role that the output has in the `producer`, or it may be
    /// the name of the initial uploaded object.
    label: String
}

pub type DataObjectRef = WrappedRcRefCell<DataObject>;


impl DataObjectRef {

    pub fn new(graph: &mut Graph,
               id: DataObjectId,
               state: DataObjectState,
               obj_type: DataObjectType,
               keep: KeepPolicy,
               size: Option<usize>,
               label: String) -> Self {
        let dataobj = Self::wrap(DataObject {
            id,
            state,
            size,
            keep,
            obj_type,
            consumers: Default::default(),
            label
        });
        graph.objects.insert(dataobj.get().id, dataobj.clone());
        dataobj
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        match self.get().state {
            DataObjectState::FinishedInFile => true,
            DataObjectState::FinishedInMem(_) => true,
            _ => false
        }
    }

    #[inline]
    pub fn id(&self) -> DataObjectId {
        self.get().id
    }

    #[inline]
    pub fn size(&self) -> Option<usize> {
        self.get().size
    }
}