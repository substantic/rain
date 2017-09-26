
use std::sync::Arc;
use futures::Future;

use common::id::{TaskId};
use worker::graph::{TaskRef, DataObjectRef, SubworkerRef};
use errors::{Result, Error};
use worker::state::{StateRef, State};
use worker::data::Data;
use worker::tasks;
use worker::rpc::subworker::data_from_capnp;
use common::convert::ToCapnp;

pub type TaskFuture = Future<Item=TaskContext, Error=Error>;
pub type TaskResult = Result<Box<TaskFuture>>;


/// Context represents a running task. It contains resource allocations and
/// allows to signal finishing of data objects.

pub struct TaskContext {
    pub task: TaskRef,
    pub state: StateRef,
    pub subworker: Option<SubworkerRef>
    // TODO: Allocated resources
}

impl TaskContext {

    pub fn new(task: TaskRef, state: StateRef) -> Self {
        TaskContext { task, state, subworker: None }
    }

    /// Start the task -- returns a future that is finished when task is finished
    pub fn start(self, state: &mut State) -> TaskResult {
        if self.task.get().task_type.starts_with("!") {
            // Build-in task
            let task_function = match self.task.get().task_type.as_ref() {
                "!run" => tasks::run::task_run,
                "!concat" => tasks::basic::task_concat,
                "!sleep" => tasks::basic::task_sleep,
                task_type => bail!("Unknown task type {}", task_type)
            };
            task_function(self, state)
        } else {
            // Subworker task
            self.start_task_in_subworker(state)
        }
    }

    fn start_task_in_subworker(mut self, state: &mut State) -> TaskResult {
        let future = state.get_subworker(
            &self.state, self.task.get().task_type.as_ref())?;

        Ok(Box::new(future.and_then(|subworker| {
            self.subworker = Some(subworker.clone());
            let future = {
                let mut req = subworker.get().control().run_task_request();
                let task = self.task.get();
                debug!("Starting task id={} in subworker", task.id);
                {
                    let mut param_task = req.get().get_task().unwrap();
                    task.id.to_capnp(&mut param_task.borrow().get_id().unwrap());
                    param_task.set_task_type(&task.task_type);
                    param_task.set_task_config(&task.task_config);
                }
                req.send().promise
            };

            future.and_then(|response| {
                {
                    let task = self.task.get();
                    debug!("Task id={} finished in subworker", task.id);
                    let response = response.get()?;
                    for (co, output) in response.get_objects()?.iter().zip(&task.outputs) {
                        let data = data_from_capnp(&co)?;
                        output.get_mut().set_data(Arc::new(data));
                    }
                }
                Ok(self)
            }).map_err(|e| e.into())
        })))
    }
}
