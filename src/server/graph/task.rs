use futures::unsync::oneshot::Sender;

use common::wrapped::WrappedRcRefCell;
use common::RcSet;
use common::id::TaskId;
use super::{DataObjectRef, WorkerRef, SessionRef, Graph};

pub enum TaskState {
    NotAssigned,
    Ready,
    Assigned(WorkerRef),
    AssignedReady(WorkerRef),
    Running(WorkerRef),
    Finished(WorkerRef),
}

pub struct TaskInput {
    /// Input data object.
    object: DataObjectRef,
    /// Label may indicate the role the object plays for this task.
    label: String,
    // TODO: add any input params or flags
}

pub struct Task {
    /// Unique ID within a `Session`
    id: TaskId,

    /// Current state.
    state: TaskState,

    /// Ordered inputs for the task. Note that one object can be present as multiple inputs!
    inputs: Vec<TaskInput>,

    /// Ordered outputs for the task. Every object in the list must be distinct.
    outputs: RcSet<DataObjectRef>,

    /// Unfinished objects that we wait for. These must be a subset of `inputs`,
    /// but multiplicities in `inputs` are here represented only once.
    waiting_for: RcSet<DataObjectRef>,

    /// Worker with the scheduled task.
    pub(super) assigned: Option<WorkerRef>,

    /// Owning session. Must match `SessionId`.
    session: SessionRef,

    /// Task type
    // TODO: specify task types or make a better type ID system
    procedure_key: String,

    /// Task configuration - task type dependent
    procedure_config: Vec<u8>,

    /// Hooks executed when the task is finished
    finish_hooks: Vec<Sender<()>>,
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl TaskRef {
    pub fn new(graph: &mut Graph, session: &SessionRef, id: TaskId /* TODO: more */) -> Self {
        let s = TaskRef::wrap(Task {
            id: id,
            state: TaskState::NotAssigned,
            inputs: Default::default(),
            outputs: Default::default(),
            waiting_for: Default::default(),
            assigned: Default::default(),
            session: session.clone(),
            procedure_key: Default::default(),
            procedure_config: Default::default(),
            finish_hooks: Default::default(),
        });
        // add to graph
        graph.tasks.insert(s.get().id, s.clone());
        // add to session
        session.get_mut().tasks.insert(s.clone());
        s
    }

    pub fn delete(self, graph: &mut Graph) {
        let mut inner = self.get_mut();
        // remove from outputs
        for o in inner.outputs.iter() {
            debug_assert!(o.get_mut().producer == Some(self.clone()));
            o.get_mut().producer = None;
        }
        // remove from inputs
        for i in inner.inputs.iter() {
            // Note that self may have been removed by another input
            i.object.get_mut().consumers.remove(&self);
        }
        // remove from workers
        if let Some(ref w) = inner.assigned {
            assert!(w.get_mut().tasks.remove(&self));
        }
        // remove from owner
        assert!(inner.session.get_mut().tasks.remove(&self));
        // remove from graph
        graph.tasks.remove(&inner.id).unwrap();
        // assert that we hold the last reference, then drop it
        assert_eq!(self.get_num_refs(), 1);
    }

    /// Return the object ID in graph.
    pub fn get_id(&self) -> TaskId { self.get().id }
}