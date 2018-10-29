use futures::unsync::oneshot;
pub use rain_core::common_capnp::TaskState;
use rain_core::{errors::*, types::*, utils::*};
use std::fmt;
use error_chain::bail;

use super::{DataObjectRef, DataObjectState, GovernorRef, SessionRef};
use wrapped::WrappedRcRefCell;

#[derive(Debug)]
pub struct Task {
    /// Current state. Do not modify directly but use "set_state"
    pub(in super::super) state: TaskState,

    /// Ordered inputs for the task. Note that one object can be present as multiple inputs!
    pub(in super::super) inputs: Vec<DataObjectRef>,

    /// Ordered outputs for the task. Every object in the list must be distinct.
    pub(in super::super) outputs: Vec<DataObjectRef>,

    /// Unfinished objects that we wait for. These must be a subset of `inputs`,
    /// but multiplicities in `inputs` are here represented only once.
    pub(in super::super) waiting_for: RcSet<DataObjectRef>,

    /// The task was already sent to the following Governor as Ready.
    /// The state of the Task should be Ready or NotAssigned.
    pub(in super::super) assigned: Option<GovernorRef>,

    /// The Governor scheduled to receive the task.
    pub(in super::super) scheduled: Option<GovernorRef>,

    /// Owning session. Must match `SessionId`.
    pub(in super::super) session: SessionRef,

    /// Hooks executed when the task is finished
    pub(in super::super) finish_hooks: Vec<FinishHook>,

    /// Task specs
    pub(in super::super) spec: TaskSpec,

    /// Task info
    pub(in super::super) info: TaskInfo,
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl Task {
    // To capnp for governor message
    pub fn to_governor_capnp(&self, builder: &mut ::rain_core::governor_capnp::task::Builder) {
        builder.set_spec(&::serde_json::to_string(&self.spec).unwrap());
    }

    #[inline]
    pub fn id(&self) -> TaskId {
        self.spec.id
    }

    #[inline]
    pub fn spec(&self) -> &TaskSpec {
        &self.spec
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
                Ok(()) => { /* Do nothing */ }
                Err(_) => {
                    /* Just log error, it is non fatal */
                    log::debug!("Failed to inform about finishing task");
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
        spec: TaskSpec,
        inputs: Vec<DataObjectRef>,
        outputs: Vec<DataObjectRef>,
    ) -> Result<Self> {
        assert_eq!(spec.id.get_session_id(), session.get_id());
        let mut waiting = RcSet::new();
        for i in inputs.iter() {
            let inobj = i.get();
            match inobj.state {
                DataObjectState::Removed => {
                    bail!(
                        "Can't create Task {} with Finished input object {}",
                        spec.id,
                        inobj.spec.id
                    );
                }
                DataObjectState::Finished => {}
                DataObjectState::Unfinished => {
                    waiting.insert(i.clone());
                }
            }
            if inobj.spec.id.get_session_id() != spec.id.get_session_id() {
                bail!(
                    "Input object {} for task {} is from a different session",
                    inobj.spec.id,
                    spec.id
                );
            }
        }
        for out in outputs.iter() {
            let o = out.get();
            if let Some(ref prod) = o.producer {
                bail!(
                    "Object {} already has producer (task {}) when creating task {}",
                    o.spec.id,
                    prod.get().spec.id,
                    spec.id
                );
            }
            if o.spec.id.get_session_id() != spec.id.get_session_id() {
                bail!(
                    "Output object {} for task {} is from a different session",
                    o.spec.id,
                    spec.id
                );
            }
        }
        let sref = TaskRef::wrap(Task {
            spec: spec,
            info: Default::default(),
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
            finish_hooks: Default::default(),
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
                let mut o = i.get_mut();
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

            if inner.state != TaskState::NotAssigned {
                w.get_mut().active_resources -= inner.spec().resources.cpus();
            }
        }
        inner.scheduled = None;
    }

    /// Remove the task from outputs, inputs, from governors if scheduled, and the owner.
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
            i.get_mut().consumers.remove(&self);
        }

        // remove from owner
        assert!(inner.session.get_mut().tasks.remove(&self));
        // clear and fail finish_hooks
        inner.finish_hooks.clear();
    }
}

impl ConsistencyCheck for TaskRef {
    /// Check for state and relationships consistency. Only explores adjacent objects but still
    /// may be slow.
    fn check_consistency(&self) -> Result<()> {
        log::debug!("Checking Task {:?} consistency", self);
        let s = self.get();
        // ID consistency
        if s.spec.id.get_session_id() != s.session.get_id() {
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
            let o = i.get();
            if o.state == DataObjectState::Removed && s.state != TaskState::Finished {
                bail!("waiting for removed object {:?} in {:?}", o, s);
            }
            if (o.state == DataObjectState::Finished || o.state == DataObjectState::Removed)
                == (s.waiting_for.contains(&i))
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
            if (o.state == DataObjectState::Finished || o.state == DataObjectState::Removed)
                && s.state != TaskState::Finished
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
        }) {
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
        write!(f, "TaskRef {}", self.get().id())
    }
}
