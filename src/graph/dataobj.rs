
use graph::common::Sid;
use graph::task::Task;
use graph::worker::Worker;

use std::net::SocketAddr;
use std::cell::RefCell;
use std::rc::Rc;


enum KeepPolicy {
    Temporary,
    Snapshot,
    Client,
    ClientAndSnapshot,
}

enum WorkerDataObjState {
    Assigned,
    Remote(SocketAddr),
    PullingS(SocketAddr),
    FinishedInFile,
    // FinishedMmaped(XXX),
    FinishedInMem(Vec<u8>),
}

struct WorkerDataObjDetails {
    state: WorkerDataObjState,
}

enum ServerDataObjState {
    NotAssigned,
    Assigned,
    Finished,
    Removed,
}

struct ServerDataObjDetails {
    state: ServerDataObjState,
    placement: Vec<Worker>,
}

enum DataObjDetails {
    ServerDataObj(ServerDataObjDetails),
    WorkerDataObj(WorkerDataObjDetails),
}

struct DataObjectInner {
    id: Sid,

    producer: Task,
    consumers: Vec<Task>,

    keep: KeepPolicy,

    ref_counter: u32, // When to delete Temporary object
    details: DataObjDetails,
}

pub struct DataObject {
    inner: Rc<RefCell<DataObjectInner>>,
}
