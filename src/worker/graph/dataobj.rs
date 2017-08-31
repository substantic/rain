
use common::id::{DataObjectId};
use common::keeppolicy::KeepPolicy;
use common::wrapped::WrappedRcRefCell;
use common::RcSet;
use super::{TaskRef, Graph};

use std::net::SocketAddr;
use std::cell::RefCell;
use std::rc::Rc;


pub enum DataObjState {
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

    state: DataObjState,

    /// Task that produces data object; it is None if task not computed in this worker
    // producer: Option<Task>,

    /// Consumer set, e.g. to notify of completion.
    consumers: RcSet<TaskRef>,


    obj_type: DataObjectType,

    keep: KeepPolicy,

    size: usize,

    /// Label may be the role that the output has in the `producer`, or it may be
    /// the name of the initial uploaded object.
    label: String
}

pub type DataObjectRef = WrappedRcRefCell<DataObject>;


impl DataObjectRef {

    pub fn new(id: DataObjectId,
               state: DataObjState,
               obj_type: DataObjectType,
               keep: KeepPolicy,
               size: usize,
               label: String) -> Self {
        Self::wrap(DataObject {
            id,
            state,
            size,
            keep,
            obj_type,
            consumers: Default::default(),
            label
        })
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        match self.get().state {
            DataObjState::FinishedInFile => true,
            DataObjState::FinishedInMem(_) => true,
            _ => false
        }
    }

    #[inline]
    pub fn id(&self) -> DataObjectId {
        self.get().id
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.get().size
    }

}