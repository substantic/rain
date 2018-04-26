use std::error::Error;
use std::net::SocketAddr;

use CLIENT_PROTOCOL_VERSION;
use common::wrapped::WrappedRcRefCell;
use super::session::Session;
use super::communicator::Communicator;

pub struct Client {
    comm: WrappedRcRefCell<Communicator>,
}

impl Client {
    pub fn new(scheduler: SocketAddr) -> Result<Self, Box<Error>> {
        let comm = WrappedRcRefCell::wrap(Communicator::new(scheduler, CLIENT_PROTOCOL_VERSION)?);

        Ok(Client { comm })
    }

    pub fn new_session(&self) -> Result<Session, Box<Error>> {
        let session_id = self.comm.get_mut().new_session()?;
        Ok(Session::new(session_id, self.comm.clone()))
    }

    pub fn terminate_server(&self) -> Result<(), Box<Error>> {
        self.comm.get_mut().terminate_server()
    }
}
