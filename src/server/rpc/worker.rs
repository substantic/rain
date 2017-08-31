use server::state::StateRef;
use server::graph::WorkerRef;
use worker_capnp::worker_upstream;
use capnp::capability::Promise;
use server::rpc::DataStoreImpl;

pub struct WorkerUpstreamImpl {
    state: StateRef,
    worker: WorkerRef,
}

impl WorkerUpstreamImpl {
    pub fn new(state: &StateRef, worker: &WorkerRef) -> Self {
        Self {
            state: state.clone(),
            worker: worker.clone(),
        }
    }
}

impl Drop for WorkerUpstreamImpl {
    fn drop(&mut self) {
        error!("Connection to worker {} lost", self.worker.get_id());
        let mut s = self.state.get_mut();
        s.remove_worker(&self.worker);
    }
}

impl worker_upstream::Server for WorkerUpstreamImpl {
    fn get_data_store(
        &mut self,
        params: worker_upstream::GetDataStoreParams,
        mut results: worker_upstream::GetDataStoreResults,
    ) -> Promise<(), ::capnp::Error> {
        let datastore = ::datastore_capnp::data_store::ToClient::new(
            DataStoreImpl::new(&self.state),
        ).from_server::<::capnp_rpc::Server>();
        results.get().set_store(datastore);
        Promise::ok(())
    }

    fn update_states(
        &mut self,
        _: worker_upstream::UpdateStatesParams,
        _: worker_upstream::UpdateStatesResults,
    ) -> Promise<(), ::capnp::Error> {
        Promise::err(::capnp::Error::unimplemented(
            "method not implemented".to_string(), // TODO
        ))
    }

    fn get_client_session(
        &mut self,
        _: worker_upstream::GetClientSessionParams,
        _: worker_upstream::GetClientSessionResults,
    ) -> Promise<(), ::capnp::Error> {
        Promise::err(::capnp::Error::unimplemented(
            "method not implemented".to_string(), // TODO
        ))
    }
}
