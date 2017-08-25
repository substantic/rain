use std::net::SocketAddr;

use server::graph::{Session, Graph};
use common::wrapped::WrappedRcRefCell;
use common::id::ClientId;
use common::RcSet;

pub struct Inner {
    id: ClientId,
    pub(super) sessions: RcSet<Session>,
}

pub type Client = WrappedRcRefCell<Inner>;

impl Client {
    pub fn new(graph: &Graph,
               address: SocketAddr) -> Self {
        let c = Client::wrap(Inner {
            id: address.clone(),
            sessions: Default::default(),
        });
        debug!("Creating client {}", c.get_id());
        // add to graph
        graph.get_mut().clients.insert(c.get().id, c.clone());
        c
    }

    pub fn delete(self, graph: &Graph) {
        debug!("Deleting client {}", self.get_id());
        // delete sessions
        let mut sessions = self.get_mut().sessions.iter().map(|x| x.clone()).collect::<Vec<_>>();
        for s in sessions { s.delete(graph); }
        // remove from graph
        graph.get_mut().clients.remove(&self.get().id).unwrap();
        // assert that we hold the last reference, then drop it
        assert_eq!(self.get_num_refs(), 1);
    }

    /// Return the object ID in graph.
    pub fn get_id(&self) -> ClientId { self.get().id }
}
