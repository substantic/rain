
use graph::common::Sid;
use graph::dataobj::DataObject;
use graph::worker::Worker;

use std::io::Bytes;
use std::cell::RefCell;
use std::rc::Rc;
use std::hash::{Hash, Hasher};

enum WorkerTaskState {
    Assigned,
    AssignedReady,
    Running,
}

struct WorkerTaskDetails {
    state: WorkerTaskState,
}

enum ServerTaskState {
    NotAssigned,
    Ready,
    Assigned,
    AssignedReady,
    Running,
    Finished,
}

struct ServerTaskDetails {
    state: ServerTaskState,
    assigned: Option<Worker>,
}

enum TaskDetails {
    ServerTask(ServerTaskDetails),
    WorkerTask(WorkerTaskDetails),
}

struct TaskInner {
    id: Sid,
    inputs: Vec<DataObject>,
    outputs: Vec<DataObject>,

    input_labels: Vec<String>,
    output_labels: Vec<String>,

    procedure_key: String,
    procedure_config: Vec<u8>,

    details: TaskDetails,
}


#[derive(Clone)]
pub struct Task {
    inner: Rc<RefCell<TaskInner>>,
}


impl Hash for Task {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ptr = &*self.inner as *const _;
        ptr.hash(state);
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for Task {}
