use futures::unsync::oneshot::Sender;
use std::fmt;

use common::wrapped::WrappedRcRefCell;
use common::id::{DataObjectId, SId};
use common::{RcSet, Additional};
use common::keeppolicy::KeepPolicy;
use super::{TaskRef, WorkerRef, SessionRef, Graph};
pub use common_capnp::{DataObjectState, DataObjectType};
use errors::Result;

#[derive(Debug)]
pub struct DataObject {
    /// Unique ID within a `Session`
    pub(in super::super) id: DataObjectId,

    /// Producer task, if any.
    pub(in super::super) producer: Option<TaskRef>,

    /// Label may be the role that the output has in the `producer`, or it may be
    /// the name of the initial uploaded object.
    pub(in super::super) label: String,

    /// Current state.
    pub(in super::super) state: DataObjectState,

    /// The type of this object.
    pub(in super::super) object_type: DataObjectType,

    /// Consumer set, e.g. to notify of completion.
    pub(in super::super) consumers: RcSet<TaskRef>,

    /// Workers with full copy of this object.
    pub(in super::super) located: RcSet<WorkerRef>,

    /// Workers that have been instructed to pull this object or already have it.
    /// Superset of `located`.
    pub(in super::super) assigned: RcSet<WorkerRef>,

    /// Assigned session. Must match SessionId.
    pub(in super::super) session: SessionRef,

    /// Reasons to keep the object alive
    pub(in super::super) keep: KeepPolicy,

    /// Hooks executed when the task is finished
    pub(in super::super) finish_hooks: Vec<Sender<()>>,

    /// Final size if known. Must match `data` size when `data` present.
    pub(in super::super) size: Option<usize>,

    /// Optinal *final* data when submitted from client or downloaded
    /// by the server (for any reason thinkable).
    pub(in super::super) data: Option<Vec<u8>>,

    /// Additional attributes (WIP)
    pub(in super::super) additional: Additional,
}

pub type DataObjectRef = WrappedRcRefCell<DataObject>;

impl DataObjectRef {
    pub fn new(graph: &mut Graph,
               session: &SessionRef,
               id: DataObjectId,
               object_type: DataObjectType,
               keep: KeepPolicy,
               label: String,
               data: Option<Vec<u8>>,
               additional: Additional) -> Result<Self> {
        assert_eq!(id.get_session_id(), session.get_id());
        if graph.objects.contains_key(&id) {
            bail!("Object {} was already in the graph", id);
        }
        let s = DataObjectRef::wrap(DataObject {
            id: id,
            producer: Default::default(),
            label: label,
            state: if data.is_none() {
                DataObjectState::NotAssigned
            } else {
                DataObjectState::Finished
            } ,
            object_type: object_type,
            consumers: Default::default(),
            located: Default::default(),
            assigned: Default::default(),
            session: session.clone(),
            keep: keep,
            finish_hooks: Vec::new(),
            size: data.as_ref().map(|v| v.len()),
            data: data,
            additional: additional,
        });
        // add to graph
        graph.objects.insert(s.get().id, s.clone());
        // add to session
        session.get_mut().objects.insert(s.clone());
        Ok(s)
    }

    pub fn delete(self, graph: &mut Graph) {
        let inner = self.get_mut();
        assert!(inner.consumers.is_empty(), "Can only remove objects without consumers.");
        assert!(inner.producer.is_none(), "Can only remove objects without a producer.");
        // remove from workers
        for w in inner.assigned.iter() {
            assert!(w.get_mut().assigned_objects.remove(&self));
        }
        for w in inner.located.iter() {
            assert!(w.get_mut().located_objects.remove(&self));
        }
        // remove from owner
        assert!(inner.session.get_mut().objects.remove(&self));
        // remove from graph
        graph.objects.remove(&inner.id).unwrap();
        // assert that we hold the last reference, , then drop it
        assert_eq!(self.get_num_refs(), 1);
    }

    /// Return the object ID in graph.
    pub fn get_id(&self) -> DataObjectId { self.get().id }
}

impl ::std::fmt::Debug for DataObjectRef {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "DataObjectRef {}", self.get_id())
    }
}

impl fmt::Debug for DataObjectState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", match *self {
            DataObjectState::NotAssigned => "NotAssigned",
            DataObjectState::Assigned => "Assigned",
            DataObjectState::Running => "Running",
            DataObjectState::Finished => "Finished",
            DataObjectState::Removed => "Removed",
            _ => panic!("Unknown DataObjectState"),
        })
    }
}

impl fmt::Debug for DataObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", match *self {
            DataObjectType::Blob => "Blob",
            DataObjectType::Directory => "Directory",
            DataObjectType::Stream => "Stream",
            _ => panic!("Unknown DataObjectType"),
        })
    }
}
