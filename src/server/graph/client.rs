use std::net::SocketAddr;
use server::graph::Session;
use common::wrapped::WrappedRcRefCell;
use common::id::ClientId;
use common::RcSet;

pub struct Inner {
    id: ClientId,
    sessions: RcSet<Session>,
}

pub type Client = WrappedRcRefCell<Inner>;

impl Client {
    pub fn new(address: &SocketAddr) -> Self {
        Self::wrap(Inner {
            id: address.clone(),
            sessions: Default::default(),
        })
    }
}