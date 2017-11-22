use std::net::SocketAddr;

use super::{SessionRef, Graph};
use common::wrapped::WrappedRcRefCell;
use common::id::ClientId;
use common::{RcSet, ConsistencyCheck};
use errors::Result;

#[derive(Debug)]
pub struct Client {
    pub(in super::super) id: ClientId,
    pub(in super::super) sessions: RcSet<SessionRef>,
}

pub type ClientRef = WrappedRcRefCell<Client>;

impl ClientRef {
    /// Create new Client object
    pub fn new(address: SocketAddr) -> Self {
        ClientRef::wrap(Client {
            id: address.clone(),
            sessions: Default::default(),
        })
    }

    /// Return the object ID in graph.
    pub fn get_id(&self) -> ClientId {
        self.get().id
    }
}

impl ConsistencyCheck for ClientRef {
    /// Check for state and relationships consistency. Only explores adjacent objects but still
    /// may be slow.
    fn check_consistency(&self) -> Result<()> {
        let s = self.get();
        for sref in s.sessions.iter() {
            if sref.get().client != *self {
                bail!("session ref {:?} inconsistency in {:?}", sref, s)
            }
        }
        Ok(())
    }
}

impl ::std::fmt::Debug for ClientRef {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "ClientRef {}", self.get_id())
    }
}
