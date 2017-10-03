
use std::sync::Arc;
use futures::Future;

use common::id::{TaskId};
use worker::graph::{TaskRef, DataObjectRef, SubworkerRef};
use errors::{Result, Error};
use worker::state::{StateRef, State};
use worker::data::{Data};
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

            // Run task in subworker
            let future = {
                let mut req = subworker.get().control().run_task_request();
                let task = self.task.get();
                debug!("Starting task id={} in subworker", task.id);
                {
                    // Serialize task
                    let mut param_task = req.get().get_task().unwrap();
                    task.id.to_capnp(&mut param_task.borrow().get_id().unwrap());
                    param_task.set_task_config(&task.task_config);

                    param_task.borrow().init_inputs(task.inputs.len() as u32);
                    {
                        // Serialize inputs of task
                        let mut p_inputs = param_task.borrow().get_inputs().unwrap();
                        for (i, input) in task.inputs.iter().enumerate() {
                            let mut p_input = p_inputs.borrow().get(i as u32);
                            p_input.set_label(&input.label);
                            let obj = input.object.get();
                            obj.data().to_subworker_capnp(&mut p_input.borrow().get_data().unwrap());
                            obj.id.to_capnp(&mut p_input.get_id().unwrap());
                        }
                    }


                    param_task.borrow().init_outputs(task.outputs.len() as u32);
                    {
                        // Serialize outputs of task
                        let mut p_outputs = param_task.get_outputs().unwrap();
                        for (i, output) in task.outputs.iter().enumerate() {
                            let mut p_output = p_outputs.borrow().get(i as u32);
                            let obj = output.get();
                            p_output.set_label(&obj.label);
                            obj.id.to_capnp(&mut p_output.get_id().unwrap());
                        }
                    }
                }
                req.send().promise
            };

            // Task if finished
            future.and_then(|response| {
                {
                    let task = self.task.get();
                    debug!("Task id={} finished in subworker", task.id);
                    let response = response.get()?;
                    for (co, output) in response.get_data()?.iter().zip(&task.outputs) {
                        let data = data_from_capnp(&co)?;
                        output.get_mut().set_data(Arc::new(data));
                    }
                }
                Ok(self)
            }).map_err(|e| e.into())
        })))
    }
}
