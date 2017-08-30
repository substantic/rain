use futures::unsync::oneshot::Sender;
//use std::default::Default;

use common::wrapped::WrappedRcRefCell;
use common::RcSet;
use common::id::SessionId;
use super::{Client, DataObject, Task, Graph};

pub struct Inner {
    /// Unique ID
    id: SessionId,

    /// Contained tasks.
    /// NB: These are owned by the Session and are cleaned up by it.
    pub(super) tasks: RcSet<Task>,

    /// Contained objects.
    /// NB: These are owned by the Session and are cleaned up by it.
    pub(super) objects: RcSet<DataObject>,

    /// Client holding the session alive.
    client: Client,

    /// Hooks executed when all tasks are finished.
    finish_hooks: Vec<Sender<()>>,
}

pub type Session = WrappedRcRefCell<Inner>;

impl Session {
    pub fn new(graph: &Graph, client: &Client) -> Self {
        let s = Session::wrap(Inner {
            id: graph.new_session_id(),
            tasks: Default::default(),
            objects: Default::default(),
            client: client.clone(),
            finish_hooks: Default::default(),
        });
        debug!("Creating session {} for client {}", s.get_id(), s.get().client.get_id());
        // add to graph
        graph.get_mut().sessions.insert(s.get().id, s.clone());
        // add to client
        client.get_mut().sessions.insert(s.clone());
        s
    }

    pub fn delete(self, graph: &Graph) {
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
        graph.get_mut().sessions.remove(&inner.id).unwrap();
        // assert that we hold the last reference, then drop it
        assert_eq!(self.get_num_refs(), 1);
    }

    /// Return the object ID in graph.
    pub fn get_id(&self) -> SessionId { self.get().id }
}

