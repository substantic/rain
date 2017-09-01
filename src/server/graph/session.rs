use futures::unsync::oneshot::Sender;
//use std::default::Default;

use common::wrapped::WrappedRcRefCell;
use common::RcSet;
use common::id::SessionId;
use super::{ClientRef, DataObjectRef, TaskRef, Graph};
use errors::Result;

pub struct Session {
    /// Unique ID
    id: SessionId,

    /// Contained tasks.
    /// NB: These are owned by the Session and are cleaned up by it.
    pub(super) tasks: RcSet<TaskRef>,

    /// Contained objects.
    /// NB: These are owned by the Session and are cleaned up by it.
    pub(super) objects: RcSet<DataObjectRef>,

    /// Client holding the session alive.
    client: ClientRef,

    /// Hooks executed when all tasks are finished.
    finish_hooks: Vec<Sender<()>>,
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

    pub fn delete(self, graph: &mut Graph) {
        debug!("Deleting session {} for client {}", self.get_id(), self.get().client.get_id());
        // delete owned children
        let mut tasks = self.get_mut().tasks.iter().map(|x| x.clone()).collect::<Vec<_>>();
        for t in tasks { t.delete(graph); }
        let mut objects = self.get_mut().objects.iter().map(|x| x.clone()).collect::<Vec<_>>();
        for o in objects { o.delete(graph); }
        // remove from owner
        let mut inner = self.get_mut();
        assert!(inner.client.get_mut().sessions.remove(&self));
        // remove from graph
        graph.sessions.remove(&inner.id).unwrap();
        // assert that we hold the last reference, then drop it
        assert_eq!(self.get_num_refs(), 1);
    }

    /// Return the object ID in graph.
    pub fn get_id(&self) -> SessionId { self.get().id }
}

