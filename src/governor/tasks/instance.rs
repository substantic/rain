use chrono::{DateTime, Utc};
use futures::Future;

use common::DataType;
use errors::{Error, Result};
use governor::graph::{ExecutorRef, TaskRef, TaskState};
use governor::rpc::executor::data_output_from_spec;
use governor::rpc::executor_serde::ResultMsg;
use governor::state::State;
use governor::tasks;

/// Instance represents a running task. It contains resource allocations and
/// allows to signal finishing of data objects.

pub struct TaskInstance {
    task_ref: TaskRef,
    // TODO resources

    // When this sender is triggered, then task is forcefully terminated
    // When cancel_sender is None, termination is actually running
    cancel_sender: Option<::futures::unsync::oneshot::Sender<()>>,

    start_timestamp: DateTime<Utc>,
    //pub executor: Option<ExecutorRef>
}

pub type TaskFuture = Future<Item = (), Error = Error>;
pub type TaskResult = Result<Box<TaskFuture>>;

#[derive(Serialize)]
struct AttributeInfo {
    governor: String,
    start: String,
    duration: i64,
}

fn fail_unknown_type(_state: &mut State, task_ref: TaskRef) -> TaskResult {
    bail!("Unknown task type {}", task_ref.get().task_type)
}

/// Reference to executor. When dropped it calls "kill()" method
struct KillOnDrop {
    executor_ref: Option<ExecutorRef>,
}

impl KillOnDrop {
    pub fn new(executor_ref: ExecutorRef) -> Self {
        KillOnDrop {
            executor_ref: Some(executor_ref),
        }
    }

    pub fn deactive(&mut self) -> ExecutorRef {
        ::std::mem::replace(&mut self.executor_ref, None).unwrap()
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        if let Some(ref sw) = self.executor_ref {
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
            // Built-in task
            match task_type {
                task_type if !task_type.starts_with("!") => Self::start_task_in_executor,
                "!run" => tasks::run::task_run,
                "!concat" => tasks::basic::task_concat,
                "!open" => tasks::basic::task_open,
                "!export" => tasks::basic::task_export,
                "!slice_directory" => tasks::basic::task_slice_directory,
                "!make_directory" => tasks::basic::task_make_directory,
                "!sleep" => tasks::basic::task_sleep,
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
                        governor: format!("{}", state.governor_id()),
                        start: instance.start_timestamp.to_rfc3339(),
                        duration: (Utc::now().signed_duration_since(instance.start_timestamp))
                            .num_milliseconds(),
                    };
                    task.new_attributes.set("info", info).unwrap();

                    match r {
                        Ok((true, _)) => {
                            let all_finished = task.outputs.iter().all(|o| o.get().is_finished());
                            if !all_finished {
                                task.set_failed("Some of outputs were not produced".into());
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

    fn start_task_in_executor(state: &mut State, task_ref: TaskRef) -> TaskResult {
        let (future, method_name) = {
            let task = task_ref.get();
            let tokens: Vec<_> = task.task_type.split('/').collect();
            if tokens.len() != 2 {
                bail!("Invalid task_type, does not contain '/'");
            }
            (
                state.get_executor(tokens.get(0).unwrap())?,
                tokens.get(1).unwrap().to_string(),
            )
        };
        let state_ref = state.self_ref();
        Ok(Box::new(future.and_then(move |executor_ref| {
            // Run task in executor

            // We wrap executor into special struct that kill executor when dropped
            // This is can happen when task is terminated and feature dropped without finishhing
            let mut sw_wrapper = KillOnDrop::new(executor_ref.clone());
            let task_ref2 = task_ref.clone();
            let task = task_ref2.get();
            let executor_ref2 = executor_ref.clone();
            let mut executor = executor_ref2.get_mut();
            executor
                .send_task(&task, method_name, &executor_ref)
                .then(move |r| {
                    sw_wrapper.deactive();
                    match r {
                        Ok(ResultMsg {
                            task: task_id,
                            attributes,
                            success,
                            outputs,
                            cached_objects,
                        }) => {
                            let result: Result<()> = {
                                let mut task = task_ref.get_mut();
                                let executor = executor_ref.get();
                                let work_dir = executor.work_dir();
                                assert!(task.id == task_id);
                                task.new_attributes.update(attributes);

                                if success {
                                    debug!("Task id={} finished in executor", task.id);
                                    for (co, output) in outputs.into_iter().zip(&task.outputs) {
                                        let attributes = co.attributes.clone();

                                        // TEMPORRARY HACK, when new attributes are introduced, this should be removed, new attributes
                                        let data_type_name : Option<String> = attributes.find("type")?;
                                        let data_type = data_type_name
                                            .map(|n| {
                                                if "directory" == n {
                                                    DataType::Directory
                                                } else {
                                                    DataType::Blob
                                                }
                                            })
                                            .unwrap_or(DataType::Blob);
                                        let data = data_output_from_spec(
                                            &state_ref.get(),
                                            work_dir,
                                            co,
                                            data_type,
                                        )?;
                                        let mut o = output.get_mut();
                                        o.set_attributes(attributes);
                                        o.set_data(data)?;
                                    }
                                    Ok(())
                                } else {
                                    debug!("Task id={} failed in executor", task.id);
                                    Err("Task failed in executor".into())
                                }
                            };

                            let mut state = state_ref.get_mut();

                            for object_id in cached_objects {
                                // TODO: Validate that object_id is input/output of the task
                                let obj_ref = state.graph.objects.get(&object_id).unwrap();
                                obj_ref
                                    .get_mut()
                                    .executor_cache
                                    .insert(executor_ref.clone());
                            }

                            state.graph.idle_executors.insert(executor_ref);

                            result
                        }
                        Err(_) => Err("Lost connection to executor".into()),
                    }
                })
        })))
    }
}
