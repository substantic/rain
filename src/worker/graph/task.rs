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
pub enum TaskState {
    Assigned,
    Running,
    Finished
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
    pub (in super::super) id: TaskId,

    /// Ordered inputs for the task. Note that one object can be present as multiple inputs!
    pub (in super::super) inputs: Vec<TaskInput>,

    /// Ordered outputs for the task. Every object in the list must be distinct.
    pub (in super::super) outputs: Vec<DataObjectRef>,

    /// Unfinished objects that we wait for. These must be a subset of `inputs`,
    /// but multiplicities in `inputs` are here represented only once.
    pub (in super::super) waiting_for: RcSet<DataObjectRef>,

    pub (in super::super) state: TaskState,

    pub (in super::super) task_type: String,
    pub (in super::super) task_config: Vec<u8>,
}

impl Task {

    #[inline]
    pub fn is_ready(&self) -> bool {
        self.waiting_for.is_empty()
    }

    /// Remove data object from waiting_for list,
    /// Returns true when task becomes ready
    pub fn input_finished(&mut self, object: &DataObjectRef) -> bool {
        let found = self.waiting_for.remove(object);
        assert!(found);
        self.waiting_for.is_empty()
    }

}

pub type TaskRef = WrappedRcRefCell<Task>;

impl TaskRef {

    pub fn new(
        graph: &mut Graph,
        id: TaskId,
        inputs: Vec<TaskInput>,
        outputs: Vec<DataObjectRef>,
        task_type: String,
        task_config: Vec<u8>
    ) -> Self {

        let waiting_for: RcSet<_> = (&inputs)
            .iter()
            .map(|input| input.object.clone())
            .filter(|obj| !obj.get().is_finished())
            .collect();

        let task = Self::wrap(Task {
            id: id,
            inputs,
            outputs,
            waiting_for,
            task_type,
            task_config,
            state: TaskState::Assigned,
        });
        graph.tasks.insert(id, task.clone());

        task
    }

}
