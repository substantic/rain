use futures::Future;
use chrono::{DateTime, Utc};

use worker::graph::{SubworkerRef, TaskRef, TaskState};
use worker::state::State;
use worker::tasks;
use worker::rpc::subworker::data_from_capnp;
use common::Attributes;
use common::convert::ToCapnp;
use errors::{Error, Result};

/// Instance represents a running task. It contains resource allocations and
/// allows to signal finishing of data objects.

pub struct TaskInstance {
    task_ref: TaskRef,
    // TODO resources

    // When this sender is triggered, then task is forcefully terminated
    // When cancel_sender is None, termination is actually running
    cancel_sender: Option<::futures::unsync::oneshot::Sender<()>>,

    start_timestamp: DateTime<Utc>,
    //pub subworker: Option<SubworkerRef>
}

pub type TaskFuture = Future<Item = (), Error = Error>;
pub type TaskResult = Result<Box<TaskFuture>>;

#[derive(Serialize)]
struct AttributeInfo {
    worker: String,
    start: String,
    duration: i64,
}

fn fail_unknown_type(_state: &mut State, task_ref: TaskRef) -> TaskResult {
    bail!("Unknown task type {}", task_ref.get().task_type)
}

/// Reference to subworker. When dropped it calls "kill()" method
struct KillOnDrop {
    subworker_ref: Option<SubworkerRef>,
}

impl KillOnDrop {
    pub fn new(subworker_ref: SubworkerRef) -> Self {
        KillOnDrop {
            subworker_ref: Some(subworker_ref),
        }
    }

    pub fn deactive(&mut self) -> SubworkerRef {
        ::std::mem::replace(&mut self.subworker_ref, None).unwrap()
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if let Some(ref sw) = self.subworker_ref {
            sw.get_mut().kill();
        }
    }
}

impl TaskInstance {
    pub fn start(state: &mut State, task_ref: TaskRef) {
        {
            let mut task = task_ref.get_mut();
            state.alloc_resources(&task.resources);
            task.state = TaskState::Running;
            state.task_updated(&task_ref);
        }

        let task_fn = {
            let task = task_ref.get();
            let task_type: &str = task.task_type.as_ref();
            // Build-in task
            match task_type {
                task_type if !task_type.starts_with("!") => Self::start_task_in_subworker,
                "!run" => tasks::run::task_run,
                "!concat" => tasks::basic::task_concat,
                "!sleep" => tasks::basic::task_sleep,
                "!open" => tasks::basic::task_open,
                "!export" => tasks::basic::task_export,
                _ => fail_unknown_type,
            }
        };

        let future: Box<TaskFuture> = match task_fn(state, task_ref.clone()) {
            Ok(f) => f,
            Err(e) => {
                state.unregister_task(&task_ref);
                let mut task = task_ref.get_mut();
                state.free_resources(&task.resources);
                task.set_failed(e.description().to_string());
                state.task_updated(&task_ref);
                return;
            }
        };

        let (sender, receiver) = ::futures::unsync::oneshot::channel::<()>();

        let task_id = task_ref.get().id;
        let instance = TaskInstance {
            task_ref: task_ref,
            cancel_sender: Some(sender),
            start_timestamp: Utc::now(),
        };
        let state_ref = state.self_ref();
        state.graph.running_tasks.insert(task_id, instance);

        state.spawn_panic_on_error(
            future
                .map(|()| true)
                .select(receiver.map(|()| false).map_err(|_| unreachable!()))
                .then(move |r| {
                    let mut state = state_ref.get_mut();
                    let instance = state.graph.running_tasks.remove(&task_id).unwrap();
                    state.task_updated(&instance.task_ref);
                    state.unregister_task(&instance.task_ref);
                    let mut task = instance.task_ref.get_mut();
                    state.free_resources(&task.resources);

                    let info = AttributeInfo {
                        worker: format!("{}", state.worker_id()),
                        start: instance.start_timestamp.to_rfc3339(),
                        duration: (Utc::now().signed_duration_since(instance.start_timestamp))
                            .num_milliseconds(),
                    };
                    task.new_attributes.set("info", info).unwrap();

                    match r {
                        Ok((true, _)) => {
                            let all_finished = task.outputs.iter().all(|o| o.get().is_finished());
                            if !all_finished {
                                task.set_failed("Some of outputs were not produced".to_string());
                            } else {
                                for output in &task.outputs {
                                    state.object_is_finished(output);
                                }
                                debug!("Task was successfully finished");
                                task.state = TaskState::Finished;
                            }
                        }
                        Ok((false, _)) => {
                            debug!("Task {} was terminated", task.id);
                            task.set_failed("Task terminated by server".into());
                        }
                        Err((e, _)) => {
                            task.set_failed(e.description().to_string());
                        }
                    };
                    Ok(())
                }),
        );
    }

    pub fn stop(&mut self) {
        let cancel_sender = ::std::mem::replace(&mut self.cancel_sender, None);
        if let Some(sender) = cancel_sender {
            sender.send(()).unwrap();
        } else {
            debug!("Task stopping is already in progress");
        }
    }

    fn start_task_in_subworker(state: &mut State, task_ref: TaskRef) -> TaskResult {
        let future = state.get_subworker(task_ref.get().task_type.as_ref())?;
        let state_ref = state.self_ref();
        Ok(Box::new(future.and_then(move |subworker| {
            // Run task in subworker

            // We wrap subworker into special struct that kill subworker when dropped
            // This is can happen when task is terminated and feature dropped without finishhing
            let mut sw_wrapper = KillOnDrop::new(subworker.clone());

            let mut req = subworker.get().control().run_task_request();
            {
                let task = task_ref.get();
                debug!("Starting task id={} in subworker", task.id);
                // Serialize task
                let mut param_task = req.get().get_task().unwrap();
                task.id.to_capnp(&mut param_task.borrow().get_id().unwrap());

                task.attributes
                    .to_capnp(&mut param_task.borrow().get_attributes().unwrap());

                param_task.borrow().init_inputs(task.inputs.len() as u32);
                {
                    // Serialize inputs of task
                    let mut p_inputs = param_task.borrow().get_inputs().unwrap();
                    for (i, input) in task.inputs.iter().enumerate() {
                        let mut p_input = p_inputs.borrow().get(i as u32);
                        p_input.set_label(&input.label);
                        let mut obj = input.object.get_mut();

                        if obj.subworker_cache.contains(&subworker) {
                            let mut p_data = p_input.borrow().get_data().unwrap();
                            p_data.get_storage().set_cache(());
                        } else {
                            // This is caching hack, since we know that 1st argument is function
                            // for Python subworker, we force to cache first argument
                            if i == 0 {
                                obj.subworker_cache.insert(subworker.clone());
                                p_input.set_save_in_cache(true);
                            }

                            {
                                let mut p_data = p_input.borrow().get_data().unwrap();
                                obj.data().to_subworker_capnp(&mut p_data.borrow());
                                obj.attributes
                                    .to_capnp(&mut p_data.borrow().get_attributes().unwrap());
                            }
                        }
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
                        obj.attributes
                            .to_capnp(&mut p_output.borrow().get_attributes().unwrap());
                        obj.id.to_capnp(&mut p_output.get_id().unwrap());
                    }
                }
            }
            req.send()
                .promise
                .map_err::<_, Error>(|e| e.into())
                .then(move |r| {
                    let subworker_ref = sw_wrapper.deactive();
                    let result = match r {
                        Ok(response) => {
                            let mut task = task_ref.get_mut();
                            let response = response.get()?;
                            task.new_attributes
                                .update_from_capnp(&response.get_task_attributes()?);
                            let subworker = subworker_ref.get();
                            let work_dir = subworker.work_dir();
                            if response.get_ok() {
                                debug!("Task id={} finished in subworker", task.id);
                                for (co, output) in response.get_data()?.iter().zip(&task.outputs) {
                                    let data = data_from_capnp(&state_ref.get(), work_dir, &co)?;
                                    let attributes =
                                        Attributes::from_capnp(&co.get_attributes().unwrap());

                                    let mut o = output.get_mut();
                                    o.set_attributes(attributes);
                                    o.set_data(data);
                                }
                            } else {
                                debug!("Task id={} failed in subworker", task.id);
                                bail!(response.get_error_message()?);
                            }
                            Ok(())
                        }
                        Err(err) => Err(err.into()),
                    };
                    state_ref
                        .get_mut()
                        .graph
                        .idle_subworkers
                        .insert(subworker_ref);
                    result
                })
        })))
    }
}
