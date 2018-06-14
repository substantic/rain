use capnp::capability::Promise;
use futures::future::Future;
use rain_core::{errors::*, types::*, utils::*};
use std::sync::Arc;

use governor::graph::DataObjectState;
use governor::StateRef;
use rain_core::governor_capnp::governor_control;

pub struct GovernorControlImpl {
    state: StateRef,
}

impl GovernorControlImpl {
    pub fn new(state: &StateRef) -> Self {
        Self {
            state: state.clone(),
        }
    }
}

impl Drop for GovernorControlImpl {
    fn drop(&mut self) {
        error!("Lost connection to the server");
        // exit(1);
    }
}

impl governor_control::Server for GovernorControlImpl {
    fn get_governor_resources(
        &mut self,
        _params: governor_control::GetGovernorResourcesParams,
        mut results: governor_control::GetGovernorResourcesResults,
    ) -> Promise<(), ::capnp::Error> {
        results
            .get()
            .set_n_cpus(self.state.get().get_resources().cpus);
        Promise::ok(())
    }

    fn unassign_objects(
        &mut self,
        params: governor_control::UnassignObjectsParams,
        mut _results: governor_control::UnassignObjectsResults,
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
                    "Object exists in governor but is not assigned".into(),
                ));
            }
            obj.assigned = false;
            state.remove_dataobj_if_not_needed(&mut obj);
        }
        Promise::ok(())
    }

    fn stop_tasks(
        &mut self,
        params: governor_control::StopTasksParams,
        mut _results: governor_control::StopTasksResults,
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
        params: governor_control::AddNodesParams,
        mut _results: governor_control::AddNodesResults,
    ) -> Promise<(), ::capnp::Error> {
        debug!("New tasks and objects");
        let params = pry!(params.get());
        let new_tasks = pry!(params.get_new_tasks());
        let new_objects = pry!(params.get_new_objects());

        let mut state = self.state.get_mut();

        let mut remote_objects = Vec::new();

        for co in new_objects.iter() {
            let spec: ObjectSpec = ::serde_json::from_str(co.get_spec().unwrap()).unwrap();
            let obj_found = state.graph.objects.get(&spec.id).cloned();
            if let Some(obj) = obj_found {
                state.mark_as_needed(&obj);
                // TODO: Update remote if not downloaded yet
                continue;
            }

            let placement = GovernorId::from_capnp(&co.get_placement().unwrap());
            let (object_state, is_remote) = if placement == *state.governor_id() {
                (DataObjectState::Assigned, false)
            } else {
                (DataObjectState::Remote(placement), true)
            };

            let assigned = co.get_assigned();
            let dataobject = state.add_dataobject(spec, object_state, assigned);

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
            let spec: TaskSpec = ::serde_json::from_str(ct.get_spec().unwrap()).unwrap();

            let inputs: Vec<_> = spec.inputs
                .iter()
                .map(|ci| state.object_by_id(ci.id).unwrap())
                .collect();

            let outputs: Vec<_> = spec.outputs
                .iter()
                .map(|id| state.object_by_id(*id).unwrap())
                .collect();
            let task = state.add_task(spec, inputs, outputs);
            debug!("Received Task {:?}", task.get());
        }

        // Start fetching remote objects
        // TODO: Introduce some kind of limitations of how many tasks are
        // fetched at once
        for object in remote_objects {
            let object_ref = object.clone();
            let mut o = object.get_mut();
            let governor_id = o.remote().unwrap();
            let object_id = o.spec.id;
            let (sender, receiver) = ::futures::unsync::oneshot::channel();
            o.state = DataObjectState::Pulling((governor_id.clone(), sender));

            let state_ref = self.state.clone();
            let future = state
                .fetch_object(&governor_id, object_ref.clone())
                .map(move |data| {
                    object_ref.get_mut().set_data(Arc::new(data)).unwrap();
                    state_ref.get_mut().object_is_finished(&object_ref);
                });
            state.handle().spawn(
                future
                    .map_err(move |e| {
                        match e {
                            Error(ErrorKind::Ignored, _) => { /* do nothing, it is safe */ }
                            e => panic!("Fetch dataobject failed {:?}", e),
                        }
                    })
                    .select(receiver.then(move |_| {
                        debug!("Terminating fetching of data object id={}", object_id);
                        Ok(())
                    }))
                    .then(|_| Ok(())),
            );
        }
        state.need_scheduling();
        Promise::ok(())
    }

    fn get_info(
        &mut self,
        _params: governor_control::GetInfoParams,
        mut results: governor_control::GetInfoResults,
    ) -> Promise<(), ::capnp::Error> {
        let mut result = results.get();
        let state = self.state.get();
        {
            let mut tasks = result.reborrow().init_tasks(state.graph.tasks.len() as u32);
            for (i, task) in state.graph.tasks.values().enumerate() {
                task.get()
                    .spec
                    .id
                    .to_capnp(&mut tasks.reborrow().get(i as u32))
            }
        }
        {
            let mut objects = result
                .reborrow()
                .init_objects(state.graph.objects.len() as u32);
            for (i, object) in state.graph.objects.values().enumerate() {
                object
                    .get()
                    .spec
                    .id
                    .to_capnp(&mut objects.reborrow().get(i as u32))
            }
        }
        {
            let mut objects =
                result.init_objects_to_delete(state.graph.delete_wait_list.len() as u32);
            for (i, (object, _)) in state.graph.delete_wait_list.iter().enumerate() {
                object
                    .get()
                    .spec
                    .id
                    .to_capnp(&mut objects.reborrow().get(i as u32))
            }
        }
        Promise::ok(())
    }
}
