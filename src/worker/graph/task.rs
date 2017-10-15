use std::sync::Arc;
use common::id::{TaskId};
use super::{DataObjectRef, Graph};
use common::{Additional, RcSet};
use std::iter::FromIterator;

use std::io::Bytes;
use std::cell::RefCell;
use std::rc::Rc;
use std::hash::{Hash, Hasher};
use worker::data::Data;
use common::wrapped::WrappedRcRefCell;
use std::fmt;

use errors::Result;

#[derive(PartialEq, Eq, Debug)]
pub enum TaskState {
    Assigned,
    Running,
    Finished,
    Failed
}

#[derive(Debug)]
pub struct TaskInput {
    /// Input data object.
    pub object: DataObjectRef,

    /// Label may indicate the role the object plays for this task.
    pub label: String,

    /// Path to subdirectory/subblob; if input is not directory it has to be empty
    pub path: String,
}


#[derive(Debug)]
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

    pub (in super::super) new_additionals: Additional
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
        let is_ready = self.waiting_for.is_empty();
        if is_ready {
            debug!("Task id={} is ready", self.id);
        }
        is_ready
    }

    /// Get input data of the task at given index
    pub fn input(&self, index: usize) -> Arc<Data> {
        let object = self.inputs.get(index).unwrap().object.get();
        object.data().clone()
    }

    /// Get all input data as vector
    pub fn inputs(&self) -> Vec<Arc<Data>> {
        self.inputs.iter().map(|input| input.object.get().data().clone()).collect()
    }

    /// Returns an error if task has different number of arguments
    pub fn check_number_of_args(&self, n_args: usize) -> Result<()> {
        if self.inputs.len() != n_args {
            bail!("Invalid number of arguments, expected: {}", n_args);
        }
        Ok(())
    }


    pub fn output(&self, index: usize) -> DataObjectRef {
        self.outputs.get(index).unwrap().clone()
    }

    pub fn set_failed(&mut self, error_message: String) {
        warn!("Task {} failed: {}", self.id, error_message);
        assert_ne!(self.state, TaskState::Failed);
        self.state == TaskState::Failed;
        self.new_additionals.set_str("error", error_message);
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
        task_config: Vec<u8>,
    ) -> Self {
        debug!("New task id={} type={}", id, task_type);

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
            new_additionals: Additional::new(),
        });

        for obj in &task.get().waiting_for {
            obj.get_mut().consumers.insert(task.clone());
        }

        graph.tasks.insert(id, task.clone());

        task
    }

}

impl fmt::Debug for TaskRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TaskRef {}", self.get().id)
    }
}