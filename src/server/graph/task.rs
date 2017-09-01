use futures::unsync::oneshot::Sender;

use common::wrapped::WrappedRcRefCell;
use common::{RcSet, Additional};
use common::id::{TaskId, SId};
use super::{DataObjectRef, WorkerRef, SessionRef, Graph, DataObjectState, DataObjectType};
pub use common_capnp::TaskState;
use errors::Result;

pub struct TaskInput {
    /// Input data object.
    pub object: DataObjectRef,
    /// Label may indicate the role the object plays for this task.
    pub label: String,
    /// Optional path within the object
    pub path: String,
    // TODO: add any input params or flags
}

pub struct Task {
    /// Unique ID within a `Session`
    pub(in super::super) id: TaskId,

    /// Current state.
    pub(in super::super) state: TaskState,

    /// Ordered inputs for the task. Note that one object can be present as multiple inputs!
    pub(in super::super) inputs: Vec<TaskInput>,

    /// Ordered outputs for the task. Every object in the list must be distinct.
    pub(in super::super) outputs: RcSet<DataObjectRef>,

    /// Unfinished objects that we wait for. These must be a subset of `inputs`,
    /// but multiplicities in `inputs` are here represented only once.
    pub(in super::super) waiting_for: RcSet<DataObjectRef>,

    /// Worker with the scheduled task.
    pub(in super::super) assigned: Option<WorkerRef>,

    /// Owning session. Must match `SessionId`.
    pub(in super::super) session: SessionRef,

    /// Task type
    // TODO: specify task types or make a better type ID system
    pub(in super::super) task_type: String,

    /// Task configuration - task type dependent
    pub(in super::super) task_config: Vec<u8>,

    /// Hooks executed when the task is finished
    pub(in super::super) finish_hooks: Vec<Sender<()>>,

    /// Additional attributes
    pub(in super::super) additional: Additional,
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl TaskRef {
    pub fn new(
        graph: &mut Graph,
        session: &SessionRef,
        id: TaskId,
        inputs: Vec<TaskInput>,
        outputs: Vec<DataObjectRef>,
        task_type: String,
        task_config: Vec<u8>,
        additional: Additional,
    ) -> Result<Self> {
        assert_eq!(id.get_session_id(), session.get_id());
        let mut waiting = RcSet::new();
        for i in inputs.iter() {
            let o = i.object.get();
            match o.state {
                DataObjectState::Removed => {
                    bail!("Can't create Task {} with Finished input object {}",
                        id, o.id);
                }
                DataObjectState::Finished => (),
                _ => { waiting.insert(i.object.clone()); }
            }
            if o.object_type == DataObjectType::Stream &&
                o.state != DataObjectState::NotAssigned {
                bail!("Can't create Task {} with active input stream object {}",
                    id, o.id);
            }
            if o.id.get_session_id() != id.get_session_id() {
                bail!("Input object {} for task {} is from a different session",
                    o.id, id);
            }
        }
        for out in outputs.iter() {
            let o = out.get();
            if let Some(ref prod) = o.producer {
                bail!("Object {} already has producer (task {}) when creating task {}",
                    o.id, prod.get().id, id);
            }
            if o.id.get_session_id() != id.get_session_id() {
                bail!("Output object {} for task {} is from a different session",
                    o.id, id);
            }
        }
        if graph.tasks.contains_key(&id) {
            bail!("Task {} already in the graph", id);
        }
        let sref = TaskRef::wrap(Task {
            id: id,
            state: TaskState::NotAssigned,
            inputs: inputs,
            outputs: outputs.into_iter().collect(),
            waiting_for: waiting,
             /// TODO
            assigned: Default::default(),
            session: session.clone(),
            task_type: task_type,
            task_config: task_config,
            finish_hooks: Default::default(),
            additional: additional,
        });
        { // to capture `s`
            let s = sref.get_mut();
            // add to graph
            graph.tasks.insert(s.id, sref.clone());
            // add to session
            session.get_mut().tasks.insert(sref.clone());
            // add to the DataObjects
            for i in s.inputs.iter() {
                let mut o = i.object.get_mut();
                o.consumers.insert(sref.clone());
            }
            for out in s.outputs.iter() {
                let mut o = out.get_mut();
                o.producer = Some(sref.clone());
            }
        }
        Ok(sref)
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
    pub fn get_id(&self) -> TaskId {
        self.get().id
    }
}
