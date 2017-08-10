
use common::id::Sid;
use common::keeppolicy::KeepPolicy;
use server::task::Task;
use server::worker::Worker;

use std::net::SocketAddr;
use std::cell::RefCell;
use std::rc::Rc;


pub enum DataObjState {
    NotAssigned,
    Assigned,
    Finished(usize),
    Removed(usize),
}

struct DataObjectInner {
    id: Sid,
    state: DataObjState,

    producer: Option<Task>,
    consumers: Vec<Task>,

    keep: KeepPolicy,

    placement: Vec<Worker>,
}

pub struct DataObject {
    inner: Rc<RefCell<DataObjectInner>>,
}
