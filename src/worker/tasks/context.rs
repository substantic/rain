
use std::sync::Arc;
use futures::Future;

use worker::graph::{TaskRef, DataObjectRef};
use errors::{Result, Error};
use worker::state::{StateRef, State};
use worker::data::Data;
use worker::tasks;

pub type TaskFuture = Future<Item=TaskContext, Error=Error>;
pub type TaskResult = Result<Box<TaskFuture>>;

/// Context represents a running task. It contains resource allocations and
/// allows to signal finishing of data objects.

pub struct TaskContext {
    pub task: TaskRef,
    pub state: StateRef,

    // TODO: Allocated resources
}

impl TaskContext {

    pub fn new(task: TaskRef, state: StateRef) -> Self {
        TaskContext { task, state }
    }

    /// Start the task -- returns a future that is finished when task is finished
    pub fn start(self, state: &State) -> TaskResult {
        let task_function = match self.task.get().task_type.as_ref() {
            "run" => tasks::run::task_run,
            "concat" => tasks::basic::task_concat,
            "sleep" => tasks::basic::task_sleep,
            task_type => bail!("Unknown task type {}", task_type)
        };
        task_function(self, state)
    }
}
