use futures::unsync::oneshot;
use std::fmt;

use super::{GovernorRef, SessionRef, TaskRef, TaskState};
use common::id::{DataObjectId, SId};
use common::wrapped::WrappedRcRefCell;
use common::{ObjectSpec, ObjectInfo};
use common::{ConsistencyCheck, FinishHook, RcSet};
pub use common_capnp::DataObjectState;
use errors::Result;

#[derive(Debug)]
pub struct DataObject {

    pub(in super::super) spec: ObjectSpec,

    pub(in super::super) info: ObjectInfo,

    /// Producer task, if any.
    pub(in super::super) producer: Option<TaskRef>,

    /// Current state.
    pub(in super::super) state: DataObjectState,

    /// Consumer set, e.g. to notify of completion.
    pub(in super::super) consumers: RcSet<TaskRef>,

    /// Consumer set, e.g. to notify of completion.
    pub(in super::super) need_by: RcSet<TaskRef>,

    /// Governors scheduled to have a full copy of this object.
    pub(in super::super) scheduled: RcSet<GovernorRef>,

    /// Governors that have been instructed to pull this object or already have it.
    /// Superset of `located`.
    pub(in super::super) assigned: RcSet<GovernorRef>,

    /// Governors with full copy of this object.
    pub(in super::super) located: RcSet<GovernorRef>,

    /// Assigned session. Must match SessionId.
    pub(in super::super) session: SessionRef,

    /// The object is requested to be kept by the client.
    pub(in super::super) client_keep: bool,

    /// Hooks executed when the task is finished
    pub(in super::super) finish_hooks: Vec<FinishHook>,

    /// Optinal *final* data when submitted from client or downloaded
    /// by the server (for any reason thinkable).
    pub(in super::super) data: Option<Vec<u8>>,
}

impl DataObject {
    /// To capnp for governor message
    /// It does not fill `placement` and `assigned`, that must be done by caller
    pub fn to_governor_capnp(&self, builder: &mut ::governor_capnp::data_object::Builder) {
        builder.set_state(self.state);
        builder.set_spec(&::serde_json::to_string(&self.spec).unwrap());
    }

    /// Inform observers that task is finished
    pub fn trigger_finish_hooks(&mut self) {
        debug!("trigger_finish_hooks for {:?}", self);
        for sender in ::std::mem::replace(&mut self.finish_hooks, Vec::new()) {
            //        for sender in self.finish_hooks, Vec::new()) {
            match sender.send(()) {
                Ok(()) => { /* Do nothing */ }
                Err(e) => {
                    /* Just log error, it is non fatal */
                    debug!("Failed to inform about finishing dataobject: {:?}", e);
                }
            }
        }
        assert!(self.finish_hooks.is_empty());
    }

    /// Wait until the given dataobject is finished
    pub fn wait(&mut self) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        match self.state {
            DataObjectState::Finished => sender.send(()).unwrap(),
            DataObjectState::Removed => panic!("waiting on Removed object"),
            _ => self.finish_hooks.push(sender),
        };
        receiver
    }

    #[inline]
    pub fn state(&self) -> DataObjectState {
        self.state
    }

    /// Is the Finished object data still needed by client (keep flag) or future tasks?
    /// Scheduling is not accounted here.
    /// Asserts the object is finished.
    #[inline]
    pub fn is_needed(&self) -> bool {
        self.client_keep || !self.need_by.is_empty()
    }

    #[inline]
    pub fn id(&self) -> DataObjectId {
        self.spec.id
    }

    #[inline]
    pub fn producer(&self) -> &Option<TaskRef> {
        &self.producer
    }
}

pub type DataObjectRef = WrappedRcRefCell<DataObject>;

impl DataObjectRef {
    /// Create new data object and link it to the owning session.
    pub fn new(
        session: &SessionRef,
        spec: ObjectSpec,
        client_keep: bool,
        data: Option<Vec<u8>>,
    ) -> Self {
        assert_eq!(spec.id.get_session_id(), session.get_id());
        let size = data.as_ref().map(|d| d.len());
        let obj = DataObjectRef::wrap(DataObject {
            spec: spec,
            info: Default::default(),
            producer: Default::default(),
            state: if data.is_none() {
                DataObjectState::Unfinished
            } else {
                DataObjectState::Finished
            },
            consumers: Default::default(),
            need_by: Default::default(),
            scheduled: Default::default(),
            located: Default::default(),
            assigned: Default::default(),
            session: session.clone(),
            client_keep: client_keep,
            finish_hooks: Vec::new(),
            data: data,
        });

        if size.is_some() {
            obj.get_mut().info.size = size;
        }
        // add to session
        session.get_mut().objects.insert(obj.clone());
        obj
    }

    pub fn unschedule(&self) {
        let mut inner = self.get_mut();
        for w in &inner.scheduled {
            w.get_mut().scheduled_objects.remove(&self);
        }
        inner.scheduled.clear();
    }

    /// Check that no compulsory links exist and remove from owner.
    /// Clears (and fails) any finish_hooks. Leaves the unlinked object in in consistent state.
    pub fn unlink(&self) {
        self.unschedule();
        let mut inner = self.get_mut();
        assert!(
            inner.assigned.is_empty(),
            "Can only remove non-assigned objects."
        );
        assert!(
            inner.located.is_empty(),
            "Can only remove non-located objects."
        );
        assert!(
            inner.consumers.is_empty(),
            "Can only remove objects without consumers."
        );
        assert!(
            inner.producer.is_none(),
            "Can only remove objects without a producer."
        );
        // remove from owner
        assert!(inner.session.get_mut().objects.remove(&self));
        // clear finish_hooks
        inner.finish_hooks.clear();
    }
}

impl ConsistencyCheck for DataObjectRef {
    /// Check for state and relationships consistency. Only explores adjacent objects but still
    /// may be slow.
    fn check_consistency(&self) -> Result<()> {
        let s = self.get();
        // ID consistency
        if s.spec.id.get_session_id() != s.session.get_id() {
            bail!("ID and Session ID mismatch in {:?}", s);
        }
        // reference symmetries
        for wr in s.assigned.iter() {
            if !wr.get().assigned_objects.contains(self) {
                bail!("assigned asymmetry in {:?}", s);
            }
        }
        for wr in s.scheduled.iter() {
            if !wr.get().scheduled_objects.contains(self) {
                bail!("scheduled asymmetry in {:?}", s);
            }
        }
        for wr in s.located.iter() {
            if !wr.get().located_objects.contains(self) {
                bail!("located asymmetry in {:?}", s);
            }
            if !s.assigned.contains(wr) {
                bail!("located at not-assigned governor in {:?}", s);
            }
        }
        if !s.session.get().objects.contains(self) {
            bail!("session assymetry in {:?}", s);
        }
        if let Some(ref tr) = s.producer {
            if !tr.get().outputs.contains(self) {
                bail!("object missing in producer {:?} outputs in {:?}", tr, s);
            }
        }
        // producer consistency
        if let Some(ref pr) = s.producer {
            let p = pr.get();
            if s.state == DataObjectState::Unfinished && p.state == TaskState::Finished {
                bail!("producer finished state inconsistency in {:?}", s);
            }
            if s.state == DataObjectState::Finished && p.state != TaskState::Finished {
                bail!("producer not finished state inconsistency in {:?}", s);
            }
            // Not relevant anyomre:
/*            if let Some(ref swr) = p.scheduled {
                if !s.scheduled.contains(swr) {
                    bail!("not scheduled to producer governor in {:?}");
                }
            }
  */            if let Some(ref swr) = p.assigned {
                if !s.assigned.contains(swr) {
                    bail!("not assigned to producer governor in {:?}");
                }
            }
        } else {
            /* When session is cleared, the following invariant is not true
            if s.state == DataObjectState::Finished {
                if s.data.is_none() {
                    bail!("no data present for object without producer in {:?}", s);
                }
            }*/
        }
        // state consistency
        if !match s.state {
            DataObjectState::Unfinished => s.scheduled.len() <= 1 && s.assigned.len() <= 1,
            // NOTE: Can't check s.producer.is_some() in case the session is being destroyed,
            DataObjectState::Finished => {
                s.data.is_some() || (s.located.len() >= 1 && s.assigned.len() >= 1)
            }
            DataObjectState::Removed => {
                s.located.is_empty() && s.scheduled.is_empty() && s.assigned.is_empty()
                    && s.finish_hooks.is_empty()
            } /* &&  Why this?? s.size.is_some()*/
              /* This is not true when session failed && s.data.is_none()*/
        } {
            bail!("state inconsistency in {:?}", s);
        }
        // data consistency
        if let Some(ref d) = s.data {
            let size = s.info.size;
            if size != Some(d.len()) {
                bail!("Info size and data len mismatch in {:?} ({:?} vs {:?})", s, size, Some(d.len()));
            }
        }
        // finish hooks
        if !s.finish_hooks.is_empty() && s.state != DataObjectState::Unfinished {
            bail!("finish hooks for finished/removed object in {:?}", s);
        }
        // keepflag and empty assigned (via Removed state)
        // NOTE: Finished state already requires nonemplty locations
        if s.client_keep && s.state == DataObjectState::Removed {
            bail!("client_keep flag on removed object {:?}", s);
        }

        // used or kept objects must be assigned when their producers are
        if (s.client_keep || !s.consumers.is_empty()) && s.assigned.is_empty()
            && s.state == DataObjectState::Unfinished
        {
            if let Some(ref prod) = s.producer {
                let p = prod.get();
                if p.state == TaskState::Assigned || p.state == TaskState::Running {
                    bail!(
                        "Unfinished object is not assigned when it's producer task is in {:?}",
                        s
                    );
                }
            }
        }
        Ok(())
    }
}

impl ::std::fmt::Debug for DataObjectRef {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "DataObjectRef {}", self.get().spec.id)
    }
}

impl fmt::Debug for DataObjectState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                DataObjectState::Unfinished => "Unfinished",
                DataObjectState::Finished => "Finished",
                DataObjectState::Removed => "Removed",
            }
        )
    }
}
