
use std::sync::Arc;

use common::RcSet;
use common::keeppolicy;
use common::convert::FromCapnp;
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

    fn get_worker_resources(&mut self,
              _params: worker_control::GetWorkerResourcesParams,
              mut results: worker_control::GetWorkerResourcesResults)
              -> Promise<(), ::capnp::Error> {
        results.get().set_n_cpus(self.state.get().get_resources().n_cpus);
        Promise::ok(())
    }

    fn add_nodes(&mut self,
                 params: worker_control::AddNodesParams,
                 mut _results: worker_control::AddNodesResults)
                 -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let new_tasks = pry!(params.get_new_tasks());
        let new_objects = pry!(params.get_new_objects());

        let mut state = self.state.get_mut();

        let mut remote_objects = Vec::new();

        for co in new_objects.iter() {
            let id = DataObjectId::from_capnp(&co.get_id().unwrap());
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

            // TODO: Correct value of keep
            let keep = Default::default();

            let dataobject = state.add_dataobject(id, object_state, object_type, keep, size, label);

            if is_remote {
                remote_objects.push(dataobject);
            }
        }

        for ct in new_tasks.iter() {
            let id = TaskId::from_capnp(&ct.get_id().unwrap());
            let task_type = ct.get_task_type().unwrap();
            let task_config = ct.get_task_config().unwrap();

            let inputs: Vec<_> = ct.get_inputs().unwrap().iter().map(|ci| {
                TaskInput {
                    object: state.object_by_id(DataObjectId::from_capnp(&ci.get_id().unwrap())).unwrap(),
                    label: ci.get_label().unwrap().into(),
                    path: ci.get_path().unwrap().into(),
                }
            }).collect();

            let outputs: Vec<_> = ct.get_outputs().unwrap().iter().map(|co| {
                state.object_by_id(DataObjectId::from_capnp(&co)).unwrap()
            }).collect();
            state.add_task(id, inputs, outputs, task_type.into(), task_config.into());
        }

        // Start fetching remote objects
        // TODO: Introduce some kind of limitations
        for object in remote_objects {
            let worker_id = object.get().remote().unwrap();

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
                    state.object_is_finished(&object_ref, Arc::new(data));
                });
            state.spawn_panic_on_error(future);
        }


        state.need_scheduling();

        Promise::ok(())
    }
}
