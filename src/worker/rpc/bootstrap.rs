use std::rc::Rc;
use capnp::capability::{Promise, Results, Params};
use capnp;
use worker_capnp::{worker_bootstrap, worker_upstream, worker_control};
use worker::StateRef;
use super::WorkerControlImpl;

impl WorkerBootstrapImpl {
    pub fn new(state: &StateRef) -> Self {
        WorkerBootstrapImpl { state: state.clone() }
    }
}

pub struct WorkerBootstrapImpl {
    state: StateRef,
}

impl worker_bootstrap::Server for WorkerBootstrapImpl {
    fn get_data_store(
        &mut self,
        _arg: worker_bootstrap::GetDataStoreParams,
        res: worker_bootstrap::GetDataStoreResults,
    ) -> Promise<(), capnp::Error> {
        ::capnp::capability::Promise::err(::capnp::Error::unimplemented(
            "method not implemented".to_string(),
        ))
    }

    fn get_worker_control(

        &mut self,
        _arg: worker_bootstrap::GetWorkerControlParams,
        mut res: worker_bootstrap::GetWorkerControlResults,
    ) -> Promise<(), ::capnp::Error> {
        let control = worker_control::ToClient::new(WorkerControlImpl::new(&self.state))
            .from_server::<::capnp_rpc::Server>();
        res.get().set_control(control);
        Promise::ok(())
    }
}