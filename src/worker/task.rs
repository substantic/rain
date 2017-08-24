use common::id::Sid;
use worker::dataobj::DataObject;

use std::io::Bytes;
use std::cell::RefCell;
use std::rc::Rc;
use std::hash::{Hash, Hasher};
use common::wrapped::WrappedRcRefCell;

enum TaskState {
    Assigned,
    AssignedReady,
    Running,
}


struct Inner {
    id: Sid,
    state: TaskState,

    inputs: Vec<DataObject>,
    outputs: Vec<DataObject>,

    input_labels: Vec<String>,
    output_labels: Vec<String>,

    procedure_key: String,
    procedure_config: Vec<u8>,
}

pub type Task = WrappedRcRefCell<Inner>;