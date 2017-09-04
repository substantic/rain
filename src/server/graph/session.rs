use futures::unsync::oneshot::Sender;
use std::fmt;

use common::wrapped::WrappedRcRefCell;
use common::RcSet;
use common::id::SessionId;
use super::{ClientRef, DataObjectRef, TaskRef, Graph, TaskState, DataObjectState};
use errors::Result;

#[derive(Debug)]
pub struct Session {
    /// Unique ID
    pub (in super::super) id: SessionId,

    /// Contained tasks.
    /// NB: These are owned by the Session and are cleaned up by it.
    pub (in super::super) tasks: RcSet<TaskRef>,

    /// Contained objects.
    /// NB: These are owned by the Session and are cleaned up by it.
    pub (in super::super) objects: RcSet<DataObjectRef>,

    /// Client holding the session alive.
    pub (in super::super) client: ClientRef,

    /// Hooks executed when all tasks are finished.
    pub (in super::super) finish_hooks: Vec<Sender<()>>,
}

pub type SessionRef = WrappedRcRefCell<Session>;

impl SessionRef {
    pub fn new(graph: &mut Graph, client: &ClientRef) -> Result<Self> {
        let s = SessionRef::wrap(Session {
            id: graph.new_session_id(),
            tasks: Default::default(),
            objects: Default::default(),
            client: client.clone(),
            finish_hooks: Default::default(),
        });
        debug!("Creating session {} for client {}", s.get_id(), s.get().client.get_id());
        // add to graph
        assert!(graph.sessions.insert(s.get().id, s.clone()).is_none());
        // add to client
        client.get_mut().sessions.insert(s.clone());
        Ok(s)
    }

    /// Check for state and relationships consistency. Only explores adjacent objects but still
    /// may be slow.
    pub fn check_consistency(&self) -> Result<()> {
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
            bail!("finish_hooks on all-finished session");
        }
        Ok(())
    }

    pub fn delete(self, graph: &mut Graph) {
        debug!("Deleting session {} for client {}", self.get_id(), self.get().client.get_id());
        // delete owned children
        let tasks = self.get_mut().tasks.iter().map(|x| x.clone()).collect::<Vec<_>>();
        for t in tasks { t.delete(graph); }
        let objects = self.get_mut().objects.iter().map(|x| x.clone()).collect::<Vec<_>>();
        for o in objects { o.delete(graph); }
        // remove from owner
        let inner = self.get_mut();
        assert!(inner.client.get_mut().sessions.remove(&self));
        // remove from graph
        graph.sessions.remove(&inner.id).unwrap();
        // assert that we hold the last reference, then drop it
        assert_eq!(self.get_num_refs(), 1);
    }

    /// Return the object ID in graph.
    pub fn get_id(&self) -> SessionId { self.get().id }
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