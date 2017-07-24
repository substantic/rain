
use common::id::Sid;
use common::keeppolicy::KeepPolicy;
use worker::task::Task;

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

struct DataObjectInner {
    id: Sid,
    state: DataObjState,

    producer: Option<Task>,
    consumers: Vec<Task>,

    keep: KeepPolicy,
}

pub struct DataObject {
    inner: Rc<RefCell<DataObjectInner>>,
}
