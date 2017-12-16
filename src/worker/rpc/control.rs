
use std::sync::Arc;

use common::Attributes;
use common::Resources;
use common::RcSet;
use common::keeppolicy;
use common::convert::{ToCapnp, FromCapnp};
use common::id::{DataObjectId, WorkerId, TaskId};
use worker::graph::{DataObjectState, TaskInput};
use worker::StateRef;
use worker_capnp::worker_control;
use capnp::capability::Promise;
use std::process::exit;
use futures::future::Future;
use super::fetch::fetch_from_datastore;

pub struct WorkerControlImpl {
    state: StateRef,
}

impl WorkerControlImpl {
    pub fn new(state: &StateRef) -> Self {
        Self { state: state.clone() }
    }
}

impl Drop for WorkerControlImpl {
    fn drop(&mut self) {
        error!("Lost connection to the server");
        // exit(1);
    }
}

impl worker_control::Server for WorkerControlImpl {
    fn get_worker_resources(
        &mut self,
        _params: worker_control::GetWorkerResourcesParams,
        mut results: worker_control::GetWorkerResourcesResults,
    ) -> Promise<(), ::capnp::Error> {
        results.get().set_n_cpus(
            self.state.get().get_resources().n_cpus,
        );
        Promise::ok(())
    }

    fn unassign_objects(
        &mut self,
        params: worker_control::UnassignObjectsParams,
        mut _results: worker_control::UnassignObjectsResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let objects = pry!(params.get_objects());

        let mut state = self.state.get_mut();
        for cid in objects.iter() {
            let id = DataObjectId::from_capnp(&cid);
            debug!("Unassigning object id={}", id);

            let dataobject = pry!(state.object_by_id(id));
            let mut obj = dataobject.get_mut();
            if !obj.assigned {
                return Promise::err(::capnp::Error::failed(
                    "Object exists in worker but is not assigned".into(),
                ));
            }
            obj.assigned = false;
            if obj.consumers.is_empty() {
                let found = state.graph.objects.remove(&id);
                assert!(found.is_some());
            }
        }
        Promise::ok(())
    }

    fn stop_tasks(
        &mut self,
        params: worker_control::StopTasksParams,
        mut _results: worker_control::StopTasksResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let mut state = self.state.get_mut();
        for tid in pry!(params.get_tasks()).iter() {
            let task_id = TaskId::from_capnp(&tid);
            state.stop_task(&task_id);
        }
        Promise::ok(())
    }

    fn add_nodes(
        &mut self,
        params: worker_control::AddNodesParams,
        mut _results: worker_control::AddNodesResults,
    ) -> Promise<(), ::capnp::Error> {
        debug!("New tasks and objects");
        let params = pry!(params.get());
        let new_tasks = pry!(params.get_new_tasks());
        let new_objects = pry!(params.get_new_objects());

        let mut state = self.state.get_mut();

        let mut remote_objects = Vec::new();

        for co in new_objects.iter() {
            let id = DataObjectId::from_capnp(&co.get_id().unwrap());

            if state.graph.objects.contains_key(&id) {
                // Update remote if not downloaded yet
                continue;
            }

            let placement = WorkerId::from_capnp(&co.get_placement().unwrap());
            let object_type = co.get_type().unwrap();
            let (object_state, is_remote) = if placement == *state.worker_id() {
                (DataObjectState::Assigned, false)
            } else {
                (DataObjectState::Remote(placement), true)
            };

            let size = if co.get_size() == -1 {
                None
            } else {
                Some(co.get_size() as usize)
            };

            let label = pry!(co.get_label()).to_string();

            let assigned = co.get_assigned();
            let dataobject =
                state.add_dataobject(id, object_state, object_type, assigned, size, label);

            debug!(
                "Received DataObject {:?}, is_remote: {}",
                dataobject.get(),
                is_remote
            );

            if is_remote {
                remote_objects.push(dataobject);
            }
        }

        for ct in new_tasks.iter() {
            let id = TaskId::from_capnp(&ct.get_id().unwrap());
            let task_type = ct.get_task_type().unwrap();
            let task_config = ct.get_task_config().unwrap();
            let resources = Resources::from_capnp(&ct.get_resources().unwrap());
            let attributes = Attributes::from_capnp(&ct.get_attributes().unwrap());

            let inputs: Vec<_> = ct.get_inputs()
                .unwrap()
                .iter()
                .map(|ci| {
                    TaskInput {
                        object: state
                            .object_by_id(DataObjectId::from_capnp(&ci.get_id().unwrap()))
                            .unwrap(),
                        label: ci.get_label().unwrap().into(),
                        path: ci.get_path().unwrap().into(),
                    }
                })
                .collect();

            let outputs: Vec<_> = ct.get_outputs()
                .unwrap()
                .iter()
                .map(|co| {
                    state.object_by_id(DataObjectId::from_capnp(&co)).unwrap()
                })
                .collect();
            let task = state.add_task(
                id,
                inputs,
                outputs,
                resources,
                task_type.into(),
                task_config.into(),
                attributes,
            );

            debug!("Received Task {:?}", task.get());
        }

        // Start fetching remote objects
        // TODO: Introduce some kind of limitations of how many tasks are
        // fetched at once
        for object in remote_objects {
            let worker_id = object.get().remote().unwrap();
            let object_id = object.get().id;

            let state_ref1 = self.state.clone();
            let state_ref2 = self.state.clone();
            let object_ref = object.clone();
            let future = state.wait_for_datastore(&self.state, &worker_id).and_then(move |()| {
                    // Ask for data
                    let state = state_ref1.get();
                    let datastore = state.get_datastore(&worker_id);
                    fetch_from_datastore(&object, datastore)
                }).map(move |data| {
                    // Data obtained
                    let mut state = state_ref2.get_mut();
                    object_ref.get_mut().set_data(Arc::new(data));
                    state.object_is_finished(&object_ref);
                });
            state.handle().spawn(future.map_err(move |e| {
                // This can be an error or maybe just race condition
                // when session is closed and object was not found,
                // we are not just logging do nothing, but TODO: we should be informing server
                warn!(
                    "Fetching dataobject id={} from worker={} failed {:?}",
                    object_id,
                    worker_id,
                    e
                );
                ()
            }));
        }
        state.need_scheduling();
        Promise::ok(())
    }

    fn get_monitoring_frames(
        &mut self,
        _params: worker_control::GetMonitoringFramesParams,
        mut results: worker_control::GetMonitoringFramesResults,
    ) -> Promise<(), ::capnp::Error> {
        let mut state = self.state.get_mut();
        let monitor = state.monitor_mut();
        let frames = monitor.collect_frames();
        let mut capnp_frames = results.get().init_frames(frames.len() as u32);

        for (i, f) in frames.iter().enumerate() {
            let worker_frame = frames.get(i);
            let mut capnp_frame = capnp_frames.borrow().get(0);
            capnp_frame.set_timestamp(f.timestamp.timestamp() as u64);
            capnp_frame.set_mem_usage(f.mem_usage as u8);

            let mut capnp_usage = capnp_frame.init_cpu_usage(f.cpu_usage.len() as u32);
            for (j, u) in f.cpu_usage.iter().enumerate() {
                capnp_usage.set(j as u32, u.clone());
            }
        }
        Promise::ok(())
    }

    fn get_info(
        &mut self,
        _params: worker_control::GetInfoParams,
        mut results: worker_control::GetInfoResults,
    ) -> Promise<(), ::capnp::Error> {
        let mut result = results.get();
        let state = self.state.get();
        {
            let mut tasks = result.borrow().init_tasks(state.graph.tasks.len() as u32);
            for (i, task) in state.graph.tasks.values().enumerate() {
                task.get().id.to_capnp(&mut tasks.borrow().get(i as u32))
            }
        }
        {
            let mut objects = result.init_objects(state.graph.objects.len() as u32);
            for (i, object) in state.graph.objects.values().enumerate() {
                object.get().id.to_capnp(
                    &mut objects.borrow().get(i as u32),
                )
            }
        }
        Promise::ok(())
    }
}
