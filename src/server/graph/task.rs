use futures::unsync::oneshot;
use std::fmt;

use common::resources::Resources;
use common::convert::ToCapnp;
use common::wrapped::WrappedRcRefCell;
use common::{RcSet, Attributes, FinishHook, ConsistencyCheck};
use common::id::{TaskId, SId};
use super::{DataObjectRef, WorkerRef, SessionRef, Graph, DataObjectState, DataObjectType};
pub use common_capnp::TaskState;
use errors::{Result, Error};

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

    /// Current state. Do not modify directly but use "set_state"
    pub(in super::super) state: TaskState,

    /// Ordered inputs for the task. Note that one object can be present as multiple inputs!
    pub(in super::super) inputs: Vec<TaskInput>,

    /// Ordered outputs for the task. Every object in the list must be distinct.
    pub(in super::super) outputs: Vec<DataObjectRef>,

    /// Unfinished objects that we wait for. These must be a subset of `inputs`,
    /// but multiplicities in `inputs` are here represented only once.
    pub(in super::super) waiting_for: RcSet<DataObjectRef>,

    /// The task was already sent to the following Worker as Ready.
    /// The state of the Task should be Ready or NotAssigned.
    pub(in super::super) assigned: Option<WorkerRef>,

    /// The Worker scheduled to receive the task.
    pub(in super::super) scheduled: Option<WorkerRef>,

    /// Owning session. Must match `SessionId`.
    pub(in super::super) session: SessionRef,

    /// Task type
    // TODO: specify task types or make a better type ID system
    pub(in super::super) task_type: String,

    /// Hooks executed when the task is finished
    pub(in super::super) finish_hooks: Vec<FinishHook>,

    /// Task attributes
    pub(in super::super) attributes: Attributes,

    /// Task resources
    pub(in super::super) resources: Resources,
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl Task {
    // To capnp for worker message
    pub fn to_worker_capnp(&self, builder: &mut ::worker_capnp::task::Builder) {
        self.id.to_capnp(&mut builder.borrow().get_id().unwrap());
        {
            let mut cinputs = builder.borrow().init_inputs(self.inputs.len() as u32);
            for (i, input) in self.inputs.iter().enumerate() {
                let mut ci = cinputs.borrow().get(i as u32);
                ci.set_label(&input.label);
                ci.set_path(&input.path);
                input.object.get().id.to_capnp(&mut ci.get_id().unwrap());
            }
        }

        {
            let mut coutputs = builder.borrow().init_outputs(self.outputs.len() as u32);
            for (i, output) in self.outputs.iter().enumerate() {
                let mut co = coutputs.borrow().get(i as u32);
                output.get().id.to_capnp(&mut co);
            }
        }

        self.attributes.to_capnp(&mut builder
            .borrow()
            .get_attributes()
            .unwrap());

        builder.set_task_type(&self.task_type);
    }

    #[inline]
    pub fn is_finished(&self) -> bool {
        match self.state {
            TaskState::Finished => true,
            _ => false,
        }
    }

    /// Inform observers that task is finished
    pub fn trigger_finish_hooks(&mut self) {
        assert!(self.is_finished());
        for sender in ::std::mem::replace(&mut self.finish_hooks, Vec::new()) {
            match sender.send(()) {
                Ok(()) => { /* Do nothing */}
                Err(e) => {
                    /* Just log error, it is non fatal */
                    debug!("Failed to inform about finishing task");
                }
            }
        }
    }

    /*    pub fn set_state(&mut self, new_state: TaskState) {
        self.state = new_state;
        match new_state {
            TaskState::Finished => self.trigger_finish_hooks(),
            _ => { /* do nothing */ }
        };
    }
*/
    /// Create future that finishes until the task is finished
    pub fn wait(&mut self) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        match self.state {
            TaskState::Finished => sender.send(()).unwrap(),
            _ => self.finish_hooks.push(sender),
        };
        receiver
    }
}

impl TaskRef {
    /// Create new Task with given inputs and outputs, connecting them together.
    /// Checks the input and output object states and sessions.
    pub fn new(
        session: &SessionRef,
        id: TaskId,
        inputs: Vec<TaskInput>,
        outputs: Vec<DataObjectRef>,
        task_type: String,
        attributes: Attributes,
        resources: Resources,
    ) -> Result<Self> {
        assert_eq!(id.get_session_id(), session.get_id());
        let mut waiting = RcSet::new();
        for i in inputs.iter() {
            let inobj = i.object.get();
            match inobj.state {
                DataObjectState::Removed => {
                    bail!(
                        "Can't create Task {} with Finished input object {}",
                        id,
                        inobj.id
                    );
                }
                DataObjectState::Finished => {
                    if inobj.object_type == DataObjectType::Stream {
                        bail!(
                            "Can't create Task {} with done input stream {}",
                            id,
                            inobj.id
                        );
                    }
                    // Finished objects are assigned and located somewhere
                }
                DataObjectState::Unfinished => {
                    if inobj.object_type == DataObjectType::Stream {
                        if let Some(ref prod) = inobj.producer {
                            if prod.get().state != TaskState::NotAssigned &&
                                prod.get().state != TaskState::Ready
                            {
                                bail!(
                                    "Can't create Task {} with running input stream {}",
                                    id,
                                    inobj.id
                                );
                            }
                        }
                    }
                    waiting.insert(i.object.clone());
                }
            }
            if inobj.object_type == DataObjectType::Stream &&
                inobj.state != DataObjectState::Unfinished
            {
                bail!(
                    "Can't create Task {} with active input stream object {}",
                    id,
                    inobj.id
                );
            }
            if inobj.id.get_session_id() != id.get_session_id() {
                bail!(
                    "Input object {} for task {} is from a different session",
                    inobj.id,
                    id
                );
            }
        }
        for out in outputs.iter() {
            let o = out.get();
            if let Some(ref prod) = o.producer {
                bail!(
                    "Object {} already has producer (task {}) when creating task {}",
                    o.id,
                    prod.get().id,
                    id
                );
            }
            if o.id.get_session_id() != id.get_session_id() {
                bail!(
                    "Output object {} for task {} is from a different session",
                    o.id,
                    id
                );
            }
        }
        let sref = TaskRef::wrap(Task {
            id: id,
            state: if waiting.is_empty() {
                TaskState::Ready
            } else {
                TaskState::NotAssigned
            },
            inputs: inputs,
            outputs: outputs.into_iter().collect(),
            waiting_for: waiting,
            assigned: None,
            scheduled: None,
            session: session.clone(),
            task_type: task_type,
            finish_hooks: Default::default(),
            attributes: attributes,
            resources: resources,
        });
        {
            // add to session
            let mut s = session.get_mut();
            s.tasks.insert(sref.clone());
            s.unfinished_tasks += 1;
        }
        {
            let s = sref.get_mut();
            // add to the DataObjects
            for i in s.inputs.iter() {
                let mut o = i.object.get_mut();
                o.consumers.insert(sref.clone());
                o.need_by.insert(sref.clone());
            }
            for out in s.outputs.iter() {
                let mut o = out.get_mut();
                o.producer = Some(sref.clone());
            }
        }
        Ok(sref)
    }

    pub fn unschedule(&self) {
        let mut inner = self.get_mut();
        if let Some(ref w) = inner.scheduled {
            assert!(w.get_mut().scheduled_tasks.remove(&self));
            if inner.state == TaskState::Ready {
                assert!(w.get_mut().scheduled_ready_tasks.remove(&self));
            }
        }
        inner.scheduled = None;
    }

    /// Remove the task from outputs, inputs, from workers if scheduled, and the owner.
    /// Clears (and fails) any finish_hooks. Leaves the unlinked Task in in consistent state.
    pub fn unlink(&self) {
        self.unschedule();
        let mut inner = self.get_mut();
        assert!(
            inner.assigned.is_none(),
            "Can only unlink non-assigned tasks."
        );
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

        // remove from owner
        assert!(inner.session.get_mut().tasks.remove(&self));
        // clear and fail finish_hooks
        inner.finish_hooks.clear();
    }

    /// Return the object ID in graph.
    pub fn get_id(&self) -> TaskId {
        self.get().id
    }
}

impl ConsistencyCheck for TaskRef {
    /// Check for state and relationships consistency. Only explores adjacent objects but still
    /// may be slow.
    fn check_consistency(&self) -> Result<()> {
        debug!("Checking Task {:?} consistency", self);
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
                bail!("scheduled asymmetry with {:?} in {:?}", wr, s);
            }
            if w.scheduled_ready_tasks.contains(self) != (s.state == TaskState::Ready) {
                bail!("scheduled_ready_task inconsistency in {:?} at {:?}", s, w);
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
            if (o.state == DataObjectState::Finished || o.state == DataObjectState::Removed) ==
                (s.waiting_for.contains(&i.object))
            {
                bail!(
                    "waiting_for all unfinished inputs invalid woth {:?} in {:?}",
                    o,
                    s
                );
            }
        }
        // outputs consistency
        for or in s.outputs.iter() {
            let o = or.get();
            if o.producer != Some(self.clone()) {
                bail!("output/producer incosistency of {:?} in {:?}", o, s);
            }
            if (o.state == DataObjectState::Finished || o.state == DataObjectState::Removed) &&
                s.state != TaskState::Finished
            {
                bail!(
                    "data object {:?} done/removed before the task has finished in {:?}",
                    or,
                    s
                );
            }
        }
        // state constraints
        if !(match s.state {
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
            TaskState::Failed =>
                /* ??? s.assigned.is_none() && */ s.waiting_for.is_empty(),
        })
        {
            bail!("state/assigned/waiting_for inconsistency in {:?}", s);
        }
        if s.state == TaskState::Finished && !s.finish_hooks.is_empty() {
            bail!("nonempty finish_hooks in Finished {:?}", s);
        }

        if s.assigned.is_some() && s.scheduled.is_none() && s.state != TaskState::Failed {
            bail!("assigned/scheduled inconsistency in {:?}", s);
        }
        Ok(())
    }
}

impl fmt::Debug for TaskRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TaskRef {}", self.get_id())
    }
}

impl fmt::Debug for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                TaskState::NotAssigned => "NotAssigned",
                TaskState::Assigned => "Assigned",
                TaskState::Ready => "Ready",
                TaskState::Running => "Running",
                TaskState::Finished => "Finished",
                TaskState::Failed => "Failed",
            }
        )
    }
}
