use common::id::{TaskId};
use super::{DataObject, Graph};
use common::RcSet;
use std::iter::FromIterator;

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

pub struct TaskInput {
    /// Input data object.
    object: DataObject,

    /// Label may indicate the role the object plays for this task.
    label: String,

    /// Path to subdirectory/subblob; if input is not directory it has to be empty
    path: String,
}


struct Inner {
    id: TaskId,

    state: TaskState,

    /// Ordered inputs for the task. Note that one object can be present as multiple inputs!
    inputs: Vec<TaskInput>,

    /// Ordered outputs for the task. Every object in the list must be distinct.
    outputs: RcSet<DataObject>,

    /// Unfinished objects that we wait for. These must be a subset of `inputs`,
    /// but multiplicities in `inputs` are here represented only once.
    waiting_for: RcSet<DataObject>,

    procedure_key: String,
    procedure_config: Vec<u8>,
}

pub type Task = WrappedRcRefCell<Inner>;

impl Task {

    fn new(graph: Graph,
           id: TaskId,
           inputs: Vec<TaskInput>,
           outputs: RcSet<DataObject>,
           procedure_key: String,
           procedure_config: Vec<u8>
    ) -> Self {
        Self::wrap(Inner {
            id: id,
            state: TaskState::Assigned,
            waiting_for: RcSet::from_iter((&inputs).iter().map(|i| i.object.clone())),
            inputs,
            outputs,
            procedure_key,
            procedure_config
        })
    }

    fn id(&self) -> TaskId {
        self.get().id
    }

}