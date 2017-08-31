use futures::unsync::oneshot::Sender;

use common::wrapped::WrappedRcRefCell;
use common::id::DataObjectId;
use common::RcSet;
use common::keeppolicy::KeepPolicy;
use super::{TaskRef, WorkerRef, SessionRef, Graph};

pub enum DataObjectState {
    NotAssigned,
    Assigned,
    Finished(usize),
    Removed(usize),
}

pub struct DataObject {
    /// Unique ID within a `Session`
    id: DataObjectId,

    /// Producer task, if any.
    pub(super) producer: Option<TaskRef>,

    /// Label may be the role that the output has in the `producer`, or it may be
    /// the name of the initial uploaded object.
    label: String,

    /// Current state.
    state: DataObjectState,

    /// Consumer set, e.g. to notify of completion.
    pub(super) consumers: RcSet<TaskRef>,

    /// Workers with full copy of this object.
    pub(super) located: RcSet<WorkerRef>,

    /// Workers that have been instructed to pull this object or already have it.
    /// Superset of `located`.
    pub(super) assigned: RcSet<WorkerRef>,

    /// Assigned session. Must match SessionId.
    session: SessionRef,

    /// Reasons to keep the object alive
    keep: KeepPolicy,

    /// Hooks executed when the task is finished
    finish_hooks: Vec<Sender<()>>,
}

pub type DataObjectRef = WrappedRcRefCell<DataObject>;

impl DataObjectRef {
    pub fn new(graph: &mut Graph, session: &SessionRef, id: DataObjectId /* TODO: more */) -> Self {
        let s = DataObjectRef::wrap(DataObject {
            id: id,
            producer: Default::default(),
            label: Default::default(),
            state: DataObjectState::NotAssigned,
            consumers: Default::default(),
            located: Default::default(),
            assigned: Default::default(),
            session: session.clone(),
            keep: Default::default(),
            finish_hooks: Default::default(),
        });
        // add to graph
        graph.objects.insert(s.get().id, s.clone());
        // add to session
        session.get_mut().objects.insert(s.clone());
        s
    }

    pub fn delete(self, graph: &mut Graph) {
        let mut inner = self.get_mut();
        assert!(inner.consumers.is_empty(), "Can only remove objects without consumers.");
        assert!(inner.producer.is_none(), "Can only remove objects without a producer.");
        // remove from workers
        for w in inner.assigned.iter() {
            assert!(w.get_mut().assigned.remove(&self));
        }
        for w in inner.located.iter() {
            assert!(w.get_mut().located.remove(&self));
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