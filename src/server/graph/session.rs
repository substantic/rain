use futures::unsync::oneshot::{Receiver};
use std::fmt;

use common::wrapped::WrappedRcRefCell;
use common::{RcSet, FinishHook, ConsistencyCheck};
use common::id::SessionId;
use super::{ClientRef, DataObjectRef, TaskRef, TaskState, DataObjectState};
use errors::{Result};

#[derive(Debug)]
pub struct Session {
    /// Unique ID
    pub(in super::super) id: SessionId,

    /// State of the Session and an optional cause of the error.
    pub(in super::super) error: Option<SessionError>,

    /// Contained tasks.
    /// NB: These are owned by the Session and are cleaned up by it.
    pub(in super::super) tasks: RcSet<TaskRef>,

    /// Contained objects.
    /// NB: These are owned by the Session and are cleaned up by it.
    pub(in super::super) objects: RcSet<DataObjectRef>,

    /// Client holding the session alive.
    pub(in super::super) client: ClientRef,

    /// Number of unfinished tasks
    pub(in super::super) unfinished_tasks: usize,

    /// Hooks executed when all tasks are finished.
    pub(in super::super) finish_hooks: Vec<FinishHook>,
}

pub type SessionRef = WrappedRcRefCell<Session>;

impl Session {
    /// Return the state of the session with optional error
    pub fn get_error(&self) -> &Option<SessionError> {
        &self.error
    }

    #[inline]
    pub fn is_failed(&self) -> bool {
        self.error.is_some()
    }
}

impl Session {
    /// Returns a future that is triggered when session has no unfinished tasks
    pub fn wait(&mut self) -> Receiver<()> {
        let (sender, receiver) = ::futures::unsync::oneshot::channel();
        if self.unfinished_tasks == 0 {
            sender.send(()).unwrap();
        } else {
            self.finish_hooks.push(sender);
        }
        receiver
    }

    /// This should be called task is finished in session
    pub fn task_finished(&mut self) {
        assert!(self.unfinished_tasks > 0);
        self.unfinished_tasks -= 1;
        if self.unfinished_tasks == 0 {
            for sender in ::std::mem::replace(&mut self.finish_hooks, Vec::new()) {
                sender.send(()).unwrap();
            }
        }
    }
}

impl SessionRef {
    /// Create new session object and link it to the owning client.
    pub fn new(id: SessionId, client: &ClientRef) -> Self {
        let s = SessionRef::wrap(Session {
            id: id,
            tasks: Default::default(),
            objects: Default::default(),
            client: client.clone(),
            unfinished_tasks: 0,
            finish_hooks: Default::default(),
            error: None,
        });
        // add to client
        client.get_mut().sessions.insert(s.clone());
        s
    }

    /// Return the object ID in graph.
    pub fn get_id(&self) -> SessionId {
        self.get().id
    }

    /// Check that no objects or tasks exist and remove from owner.
    /// Clears (and fails) any finish_hooks. Leaves the unlinked object in in consistent state.
    pub fn unlink(&self) {
        let mut inner = self.get_mut();
        assert!(inner.objects.is_empty(), "Can only unlink empty session.");
        assert!(inner.tasks.is_empty(), "Can only unlink empty session.");
        // remove from owner
        assert!(inner.client.get_mut().sessions.remove(&self));
        // clear finish_hooks
        inner.finish_hooks.clear();
    }
}

impl ConsistencyCheck for SessionRef {
    /// Check for state and relationships consistency. Only explores adjacent objects but still
    /// may be slow.
    fn check_consistency(&self) -> Result<()> {
        let s = self.get();
        // refs
        for oref in s.objects.iter() {
            if oref.get().session != *self {
                bail!("session ref {:?} inconsistency in {:?}", oref, s)
            }
        }
        for tref in s.tasks.iter() {
            if tref.get().session != *self {
                bail!("session ref {:?} inconsistency in {:?}", tref, s)
            }
        }
        if !s.client.get().sessions.contains(self) {
            bail!("owning client does not contain {:?}", s);
        }
        // finished?
        if !s.finish_hooks.is_empty() &&
            s.tasks.iter().all(
                |tr| tr.get().state == TaskState::Finished,
            ) &&
            s.objects.iter().all(|or| {
                or.get().state != DataObjectState::Unfinished
            })
        {
            bail!("finish_hooks on all-finished session {:?}", s);
        }
        // in case of error, the session should be cleared
        if s.error.is_some() &&
            !(s.finish_hooks.is_empty() && s.tasks.is_empty() && s.objects.is_empty())
        {
            bail!("Session with error is not cleared: {:?}", s);
        }
        Ok(())
    }
}

impl fmt::Debug for SessionRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SessionRef {}", self.get_id())
    }
}

#[derive(Debug, Clone)]
pub struct SessionError {
    message: String,
}

impl SessionError {
    pub fn new(message: String) -> Self {
        SessionError { message }
    }

    pub fn to_capnp(&self, builder: &mut ::common_capnp::error::Builder) {
        builder.borrow().set_message(&self.message);
    }
}

impl ::std::error::Error for SessionError {
    fn description(&self) -> &str {
        &self.message
    }

    fn cause(&self) -> Option<&::std::error::Error> {
        None
    }
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SessionError({:?})", self.message)
    }
}

/*
impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Session")
            .field("id", &self.id)
            .field("tasks", &self.tasks)
            .field("objects", &self.objects)
            .field("client", &self.client)
            .field("finish_hooks", &format!("[{} Senders]", self.finish_hooks.len()))
            .finish()
    }
}

*/
