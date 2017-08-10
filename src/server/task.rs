use common::id::Sid;
use server::dataobj::DataObject;
use server::worker::Worker;

use std::io::Bytes;
use std::cell::RefCell;
use std::rc::Rc;
use std::hash::{Hash, Hasher};


pub enum TaskState {
    NotAssigned,
    Ready,
    Assigned(Worker),
    AssignedReady(Worker),
    Running(Worker),
    Finished(Worker),
}


struct TaskInner {
    id: Sid,
    state: TaskState,

    inputs: Vec<DataObject>,
    outputs: Vec<DataObject>,

    input_labels: Vec<String>,
    output_labels: Vec<String>,

    procedure_key: String,
    procedure_config: Vec<u8>,
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
