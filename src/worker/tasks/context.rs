
use futures::Future;

use worker::graph::{TaskRef, SubworkerRef};
use errors::{Result, Error};
use worker::state::{StateRef, State};
use worker::tasks;
use worker::rpc::subworker::data_from_capnp;
use common::convert::ToCapnp;
use common::Additionals;
use common::wrapped::WrappedRcRefCell;

pub type TaskFuture = Future<Item = (), Error = Error>;
pub type TaskResult = Result<Box<TaskFuture>>;


/// Context represents a running task. It contains resource allocations and
/// allows to signal finishing of data objects.

pub struct TaskContext {
    pub task: TaskRef,
    pub state: StateRef,
    pub subworker: Option<SubworkerRef>, // TODO: Allocated resources
}

pub type TaskContextRef = WrappedRcRefCell<TaskContext>;

impl TaskContextRef {
    pub fn new(task: TaskRef, state: StateRef) -> Self {
        Self::wrap(TaskContext {
            task,
            state,
            subworker: None,
        })
    }

    /// Start the task -- returns a future that is finished when task is finished
    pub fn start(&self, state: &mut State) -> TaskResult {
        let build_in_fn = {
            let context = self.get();
            let task = context.task.get();
            let task_type : &str = task.task_type.as_ref();
            if task_type.starts_with("!") {
                // Build-in task
                Some(match task_type {
                    "!run" => tasks::run::task_run,
                    "!concat" => tasks::basic::task_concat,
                    "!sleep" => tasks::basic::task_sleep,
                    "!open" => tasks::basic::task_open,
                    task_type => bail!("Unknown task type {}", task_type),
                })
            } else {
                None
            }
        };

        if let Some(task_fn) = build_in_fn {
            task_fn(self.clone(), state)
        } else {
            // Subworker task
            self.start_task_in_subworker(state)
        }
    }

    fn start_task_in_subworker(&self, state: &mut State) -> TaskResult {
        let context = self.get();
        let future = state.get_subworker(
            &context.state,
            context.task.get().task_type.as_ref(),
        )?;

        let context_ref = self.clone();
        Ok(Box::new(future.and_then(move |subworker| {
            context_ref.get_mut().subworker = Some(subworker.clone());

            // Run task in subworker
            let future = {
                let mut req = subworker.get().control().run_task_request();
                let context = context_ref.get();
                let task = context.task.get();
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
                            obj.data().to_subworker_capnp(
                                &mut p_input.borrow().get_data().unwrap(),
                            );
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
            }.map_err::<_, Error>(|e| e.into());

            // Task is finished
            future.and_then(move |response| {
                {
                    let context = context_ref.get();
                    let mut task = context.task.get_mut();
                    let response = response.get()?;
                    task.new_additionals.update(
                        Additionals::from_capnp(&response.get_task_additionals()?));
                    let subworker = context.subworker.as_ref().unwrap().get();
                    let work_dir = subworker.work_dir();
                    if response.get_ok() {
                        debug!("Task id={} finished in subworker", task.id);
                        for (co, output) in response.get_data()?.iter().zip(&task.outputs) {
                            let data = data_from_capnp(&context.state.get(), work_dir, &co)?;
                            output.get_mut().set_data(data);
                        }
                    } else {
                        debug!("Task id={} failed in subworker", task.id);
                        bail!(response.get_error_message()?);
                    }
                }
                Ok(())
            })
        })))
    }
}
