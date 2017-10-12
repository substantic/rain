use futures::unsync::oneshot::Sender;
use std::fmt;

use common::wrapped::WrappedRcRefCell;
use common::{RcSet, FinishHook, ConsistencyCheck};
use common::id::SessionId;
use common::events::Event;
use super::{ClientRef, DataObjectRef, TaskRef, Graph, TaskState, DataObjectState};
use errors::{Result, Error};

#[derive(Debug)]
pub struct Session {
    /// Unique ID
    pub (in super::super) id: SessionId,

    /// State of the Session and an optional cause of the error.
    pub (in super::super) error: Option<Event>,

    /// Contained tasks.
    /// NB: These are owned by the Session and are cleaned up by it.
    pub (in super::super) tasks: RcSet<TaskRef>,

    /// Contained objects.
    /// NB: These are owned by the Session and are cleaned up by it.
    pub (in super::super) objects: RcSet<DataObjectRef>,

    /// Client holding the session alive.
    pub (in super::super) client: ClientRef,

    /// Hooks executed when all tasks are finished.
    pub (in super::super) finish_hooks: Vec<FinishHook>,
}

pub type SessionRef = WrappedRcRefCell<Session>;

impl SessionRef {

    /// Create new session object and link it to the owning client.
    pub fn new(id: SessionId, client: &ClientRef) -> Self {
        let s = SessionRef::wrap(Session {
            id: id,
            tasks: Default::default(),
            objects: Default::default(),
            client: client.clone(),
            finish_hooks: Default::default(),
            error: None,
        });
        // add to client
        client.get_mut().sessions.insert(s.clone());
        s
    }

    /// Return the state of the session with optional error
    pub fn get_error(&self) -> Option<Event> {
        self.get().error.clone()
    }

    /// Return the object ID in graph.
    pub fn get_id(&self) -> SessionId {
        self.get().id
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
            s.tasks.iter().all(|tr| tr.get().state == TaskState::Finished) &&
            s.objects.iter().all(|or| or.get().state != DataObjectState::Unfinished) {
            bail!("finish_hooks on all-finished session {:?}", s);
        }
        // in case of error, the session should be cleared
        if s.error.is_some() &&
            !(s.finish_hooks.is_empty() && s.tasks.is_empty() && s.objects.is_empty()) {
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