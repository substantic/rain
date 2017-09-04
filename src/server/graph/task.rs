use futures::unsync::oneshot::Sender;
use std::fmt;

use common::wrapped::WrappedRcRefCell;
use common::{RcSet, Additional};
use common::id::{TaskId, SId};
use super::{DataObjectRef, WorkerRef, SessionRef, Graph, DataObjectState, DataObjectType};
pub use common_capnp::TaskState;
use errors::Result;

#[derive(Debug, Clone)]
pub struct TaskInput {
    /// Input data object.
    pub object: DataObjectRef,
    /// Label may indicate the role the object plays for this task.
    pub label: String,
    /// Optional path within the object
    pub path: String,
    // TODO: add any input params or flags
}

#[derive(Debug)]
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


    // Worker o task: vi / nevi      W
    // Task je ready: ano / ne       R
    // Task je naplanovan: ano / ne  S
    // ()-NotAss
    // (S)-NotAss - sched
    // (R)-Ready -
    // (SR)-Ready -
    // (WRS)-Assign/Run
    // () - assigned

    /// The task was already sent to the following Worker as Ready.
    /// The state of the Task should be Ready or NotAssigned
    pub(in super::super) assigned: Option<WorkerRef>,

    /// The Worker scheduled to receive the task.
    pub(in super::super) scheduled: Option<WorkerRef>,

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
                o.state != DataObjectState::Unfinished {
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
            state: if waiting.is_empty() { TaskState::Ready } else { TaskState::NotAssigned },
            inputs: inputs,
            outputs: outputs.into_iter().collect(),
            waiting_for: waiting,
            assigned: None,
            scheduled: None,
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
        sref.check_consistency_opt()?;
        Ok(sref)
    }

    /// Optionally call `check_consistency` depending on global `DEBUG_CHECK_CONSISTENCY`.
    #[inline]
    pub fn check_consistency_opt(&self) -> Result<()> {
        if ::DEBUG_CHECK_CONSISTENCY.load(::std::sync::atomic::Ordering::Relaxed) {
            self.check_consistency()
        } else {
            Ok(())
        }
    }

    /// Check for state and relationships consistency. Only explores adjacent objects but still
    /// may be slow.
    pub fn check_consistency(&self) -> Result<()> {
        let s = self.get();
        // ID consistency
        if s.id.get_session_id() != s.session.get_id() {
            bail!("ID and Session ID mismatch in {:?}", s);
        }
        // reference symmetries
        if let Some(ref wr) = s.assigned {
            if !wr.get().assigned_tasks.contains(self) {
                bail!("assigned asymmetry in {:?}", s);
            }
        }
        if let Some(ref wr) = s.scheduled {
            let w = wr.get();
            if !w.scheduled_tasks.contains(self) {
                bail!("scheduled asymmetry in {:?}", s);
                if w.scheduled_ready_tasks.contains(self) !=
                    (s.state == TaskState::Ready) {
                    bail!("scheduled_ready_task inconsistency in {:?} at {:?}", s, w);
                }
            }
        }
        if !s.session.get().tasks.contains(self) {
            bail!("session assymetry in {:?}", s);
        }
        // waiting_for and inputs consistency
        for i in s.inputs.iter() {
            let o = i.object.get();
            if o.state == DataObjectState::Removed && s.state != TaskState::Finished {
                bail!("waiting for removed object {:?} in {:?}", o, s);
            }
            if (o.state == DataObjectState::Finished) ==
                (s.waiting_for.contains(&i.object)) {
                bail!("waiting_for all unfinished inputs invalid woth {:?} in {:?}", o, s);
            }
        }
        // outputs consistency
        for or in s.outputs.iter() {
            let o = or.get();
            if o.producer != Some(self.clone()) {
                bail!("output/producer incosistency of {:?} in {:?}", o, s);
            }
            if (o.state == DataObjectState::Finished || o.state == DataObjectState::Removed) &&
                s.state != TaskState::Finished {
                bail!("data object {:?} done/removed before the task has finished in {:?}", or, s);
            }
        }
        // state constraints
        if ! (match s.state {
            TaskState::NotAssigned =>
                s.assigned.is_none() && (!s.waiting_for.is_empty() || s.inputs.is_empty()),
            TaskState::Ready =>
                s.assigned.is_none() && s.waiting_for.is_empty(),
            TaskState::Assigned =>
                s.assigned.is_some() && s.waiting_for.is_empty(),
            TaskState::Running =>
                s.assigned.is_some() && s.waiting_for.is_empty(),
            TaskState::Finished =>
                s.assigned.is_none() && s.waiting_for.is_empty(),
        }) {
            bail!("state/assigned/waiting_for inconsistency in {:?}", s);
        }
        if s.state == TaskState::Finished && !s.finish_hooks.is_empty() {
            bail!("nonempty finish_hooks in Finished {:?}", s);
        }

        if s.assigned.is_some() && s.scheduled.is_none() {
            bail!("assigned/scheduled inconsistency in {:?}", s);
        }
        Ok(())
    }

    pub fn delete(self, graph: &mut Graph) {
        let inner = self.get_mut();
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
            assert!(w.get_mut().assigned_tasks.remove(&self));
        }
        if let Some(ref w) = inner.scheduled {
            assert!(w.get_mut().scheduled_tasks.remove(&self));
            if inner.state == TaskState::Ready {
                assert!(w.get_mut().scheduled_ready_tasks.remove(&self));
            }
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

impl fmt::Debug for TaskRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TaskRef {}", self.get_id())
    }
}

/*
impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Task")
            .field("id", &self.id)
            .field("session", &self.id)
            .field("assigned", &self.id)
            .field("state", &self.state)
            .field("inputs", &self.inputs)
            .field("outputs", &self.outputs)
            .field("waiting_for", &self.waiting_for)
            .field("task_type", &self.task_type)
            .field("task_config", &self.task_config)
            .field("finish_hooks", &format!("[{} Senders]", self.finish_hooks.len()))
            .field("additional", &self.additional)
            .finish()
    }
}
*/

impl fmt::Debug for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", match *self {
            TaskState::NotAssigned => "NotAssigned",
            TaskState::Assigned => "Assigned",
            TaskState::Ready => "Ready",
            TaskState::Running => "Running",
            TaskState::Finished => "Finished",
            _ => panic!("Unknown TaskState"),
        })
    }
}

