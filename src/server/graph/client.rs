use std::net::SocketAddr;

use super::{SessionRef, Graph};
use common::wrapped::WrappedRcRefCell;
use common::id::ClientId;
use common::RcSet;
use errors::Result;

pub struct Client {
    id: ClientId,
    pub(super) sessions: RcSet<SessionRef>,
}

pub type ClientRef = WrappedRcRefCell<Client>;

impl ClientRef {
    pub fn new(graph: &mut Graph,
               address: SocketAddr) -> Result<Self> {
        if graph.clients.contains_key(&address) {
            bail!("Client {} was already in the graph", address);
        }
        let c = ClientRef::wrap(Client {
            id: address.clone(),
            sessions: Default::default(),
        });
        debug!("Creating client {}", c.get_id());
        // add to graph
        graph.clients.insert(c.get().id, c.clone());
        Ok(c)
    }

    pub fn delete(self, graph: &mut Graph) {
        debug!("Deleting client {}", self.get_id());
        // delete sessions
        let mut sessions = self.get_mut().sessions.iter().map(|x| x.clone()).collect::<Vec<_>>();
        for s in sessions { s.delete(graph); }
        // remove from graph
        graph.clients.remove(&self.get().id).unwrap();
        // assert that we hold the last reference, then drop it
        assert_eq!(self.get_num_refs(), 1);
    }

    /// Return the object ID in graph.
    pub fn get_id(&self) -> ClientId { self.get().id }
}
