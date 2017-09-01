use common::id::{TaskId};
use super::{DataObjectRef, Graph};
use common::RcSet;
use std::iter::FromIterator;

use std::io::Bytes;
use std::cell::RefCell;
use std::rc::Rc;
use std::hash::{Hash, Hasher};
use common::wrapped::WrappedRcRefCell;

#[derive(PartialEq, Eq)]
enum TaskState {
    Assigned,
    AssignedReady,
    Running,
}

pub struct TaskInput {
    /// Input data object.
    pub object: DataObjectRef,

    /// Label may indicate the role the object plays for this task.
    pub label: String,

    /// Path to subdirectory/subblob; if input is not directory it has to be empty
    pub path: String,
}


pub struct Task {
    id: TaskId,

    state: TaskState,

    /// Ordered inputs for the task. Note that one object can be present as multiple inputs!
    inputs: Vec<TaskInput>,

    /// Ordered outputs for the task. Every object in the list must be distinct.
    outputs: RcSet<DataObjectRef>,

    /// Unfinished objects that we wait for. These must be a subset of `inputs`,
    /// but multiplicities in `inputs` are here represented only once.
    waiting_for: RcSet<DataObjectRef>,

    procedure_key: String,
    procedure_config: Vec<u8>,
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl TaskRef {

    pub fn new(
        graph: &mut Graph,
        id: TaskId,
        inputs: Vec<TaskInput>,
        waiting_for: RcSet<DataObjectRef>,
        procedure_key: String,
        procedure_config: Vec<u8>
    ) -> Self {
        let task = Self::wrap(Task {
            id: id,
            state: TaskState::Assigned,
            inputs,
            waiting_for,
            procedure_key,
            procedure_config,
            outputs: Default::default()
        });
        graph.tasks.insert(id, task.clone());
        task
    }

    /// Change internal state of task to AssignedReady
    pub fn set_ready(&self) {
        let mut inner = self.get_mut();
        assert!(inner.state == TaskState::Assigned);
        inner.state = TaskState::AssignedReady;
    }

    #[inline]
    fn id(&self) -> TaskId {
        self.get().id
    }

}