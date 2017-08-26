
use common::id::Sid;
use common::keeppolicy::KeepPolicy;
use common::wrapped::WrappedRcRefCell;
use super::Task;

use std::net::SocketAddr;
use std::cell::RefCell;
use std::rc::Rc;


enum DataObjState {
    Assigned,
    Remote(SocketAddr),
    Pulling(SocketAddr),
    FinishedInFile(usize),
    // FinishedMmaped(XXX),
    FinishedInMem(Vec<u8>),
}

struct Inner {
    id: Sid,
    state: DataObjState,

    producer: Option<Task>,
    consumers: Vec<Task>,

    keep: KeepPolicy,
}

pub type DataObject = WrappedRcRefCell<Inner>;