use common::wrapped::WrappedRcRefCell;

use super::communicator::Communicator;

pub struct Session {
    id: i32,
    comm: WrappedRcRefCell<Communicator>,
}

impl Session {
    pub fn new(id: i32, comm: WrappedRcRefCell<Communicator>) -> Self {
        debug!("Session {} created", id);

        Session { id, comm }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.comm.get_mut().close_session(self.id).unwrap();
        debug!("Session {} destroyed", self.id);
    }
}
