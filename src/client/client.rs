use std::error::Error;
use std::net::SocketAddr;

use CLIENT_PROTOCOL_VERSION;
use super::session::Session;
use super::communicator::Communicator;
use std::rc::Rc;

pub struct Client {
    comm: Rc<Communicator>,
}

impl Client {
    pub fn new(scheduler: SocketAddr) -> Result<Self, Box<Error>> {
        let comm = Rc::new(Communicator::new(scheduler, CLIENT_PROTOCOL_VERSION)?);

        Ok(Client { comm })
    }

    pub fn new_session(&self) -> Result<Session, Box<Error>> {
        let session_id = self.comm.new_session()?;
        Ok(Session::new(session_id, self.comm.clone()))
    }

    pub fn terminate_server(&self) -> Result<(), Box<Error>> {
        self.comm.terminate_server()
    }
}
