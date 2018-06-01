use capnp::capability::Promise;
use chrono::TimeZone;
use common::convert::FromCapnp;
use common::id::{DataObjectId, TaskId};
use common::{TaskSpec, ObjectSpec, TaskInfo, ObjectInfo};
use governor_capnp::governor_upstream;
use server::graph::{Governor, GovernorRef};
use server::state::StateRef;

pub struct GovernorUpstreamImpl {
    state: StateRef,
    governor: GovernorRef,
}

impl GovernorUpstreamImpl {
    pub fn new(state: &StateRef, governor: &GovernorRef) -> Self {
        Self {
            state: state.clone(),
            governor: governor.clone(),
        }
    }
}

impl Drop for GovernorUpstreamImpl {
    fn drop(&mut self) {
        error!("Connection to governor {} lost", self.governor.get_id());
        let mut s = self.state.get_mut();
        s.remove_governor(&self.governor)
            .expect("dropping governor upstream");
    }
}

impl governor_upstream::Server for GovernorUpstreamImpl {
    fn update_states(
        &mut self,
        params: governor_upstream::UpdateStatesParams,
        _: governor_upstream::UpdateStatesResults,
    ) -> Promise<(), ::capnp::Error> {
        let update = pry!(pry!(params.get()).get_update());
        let mut state = self.state.get_mut();

        // TODO: Reserve vectors
        // For some reason collect over iterator do not work here !?
        let mut obj_updates = Vec::new();
        // For some reason collect over iterator do not work here !?
        let mut task_updates = Vec::new();

        {
            for obj_update in pry!(update.get_objects()).iter() {
                let id = DataObjectId::from_capnp(&pry!(obj_update.get_id()));
                if state.is_object_ignored(&id) {
                    continue;
                }
                let object = pry!(state.object_by_id(id));
                let info: ObjectInfo = ::serde_json::from_str(obj_update.get_info().unwrap()).unwrap();
                obj_updates.push((object, pry!(obj_update.get_state()), info));
            }

            for task_update in pry!(update.get_tasks()).iter() {
                let id = TaskId::from_capnp(&pry!(task_update.get_id()));
                if state.is_task_ignored(&id) {
                    continue;
                }
                let task = pry!(state.task_by_id(id));
                let info: TaskInfo = ::serde_json::from_str(task_update.get_info().unwrap()).unwrap();
                task_updates.push((task, pry!(task_update.get_state()), info));
            }
        }

        state.updates_from_governor(&self.governor, obj_updates, task_updates);
        Promise::ok(())
    }

    fn get_client_session(
        &mut self,
        _: governor_upstream::GetClientSessionParams,
        _: governor_upstream::GetClientSessionResults,
    ) -> Promise<(), ::capnp::Error> {
        Promise::err(::capnp::Error::unimplemented(
            "get_client_session: method not implemented".to_string(), // TODO
        ))
    }

    fn push_events(
        &mut self,
        params: governor_upstream::PushEventsParams,
        _: governor_upstream::PushEventsResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let cevents = pry!(params.get_events());
        let mut state = self.state.get_mut();

        for cevent in cevents.iter() {
            let event = cevent.get_event().unwrap().to_string();
            let timestamp = pry!(cevent.get_timestamp());
            let seconds = timestamp.get_seconds() as i64;
            let subsec_nanos = timestamp.get_subsec_nanos();
            state.logger.add_event_with_timestamp(
                ::serde_json::from_str(&event).unwrap(),
                ::chrono::Utc.timestamp(seconds, subsec_nanos),
            );
        }
        Promise::ok(())
    }

    fn fetch(
        &mut self,
        params: governor_upstream::FetchParams,
        mut results: governor_upstream::FetchResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let id = DataObjectId::from_capnp(&pry!(params.get_id()));
        let mut results = results.get();

        if self.state.get().is_object_ignored(&id) {
            results.get_status().set_ignored(());
            return Promise::ok(());
        }

        let object_ref = if let Ok(o) = self.state.get().object_by_id(id) {
            o
        } else {
            results.get_status().set_removed(());
            return Promise::ok(());
        };

        let obj = object_ref.get();
        let data = obj.data.as_ref().unwrap();
        let offset = params.get_offset() as usize;
        let size = params.get_size() as usize;

        if offset < data.len() {
            let end = if offset + size < data.len() {
                offset + size
            } else {
                data.len()
            };
            results.set_data(&data[offset..end]);
        }

        if params.get_include_info() {
            results.set_info(&::serde_json::to_string(&obj.info).unwrap());
        }

        Promise::ok(())
    }
}

impl Governor {}
