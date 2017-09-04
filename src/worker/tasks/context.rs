
use std::sync::Arc;
use futures::Future;

use worker::graph::{TaskRef};
use errors::{Result, Error};
use worker::state::{StateRef, State};
use worker::graph::Data;

pub type TaskFuture = Future<Item=TaskContext, Error=Error>;
pub type TaskResult = Result<Box<TaskFuture>>;

/// Context for running task that contains
/// resource allocations and allows to finish data objects
pub struct TaskContext {
    pub task: TaskRef,
    pub state: StateRef,

    // TODO: Allocated resources
}

impl TaskContext {

    pub fn new(task: TaskRef, state: StateRef) -> Self {
        TaskContext { task, state }
    }

    pub fn start(self, state: &State) -> TaskResult {
        match &self.task.get().task_type {
            task_type => bail!("Unknown task type {}", task_type)
        }
    }

    pub fn input(&self, index: usize) -> Arc<Data> {
        let task = self.task.get();
        let object = task.inputs.get(index).unwrap().object.get();
        object.data().clone()
    }

    pub fn inputs(&self) -> Vec<Arc<Data>> {
        let task = self.task.get();
        task.inputs.iter().map(|input| input.object.get().data().clone()).collect()
    }

    /// Returns an error if task has different number of arguments
    pub fn check_number_of_args(&self, n_args: usize) -> Result<()> {
        if self.task.get().inputs.len() != n_args {
            bail!("Invalid number of arguments, expected: {}", n_args);
        }
        Ok(())
    }

    /// Finish an output of object of task defined by index in output array
    pub fn object_finished(&self, index: usize, data: Arc<Data>) {
        let dataobject = { let task = self.task.get();
                           task.outputs.get(index).unwrap().clone() };
        self.state.get_mut().object_is_finished(&dataobject, data);
    }
}
