use std::rc::Rc;
use capnp::capability::{Promise, Results, Params};
use capnp;
use worker_capnp::{worker_bootstrap, worker_upstream, worker_control};

pub type WorkerState = Rc<WorkerStateInner>;

#[derive(Debug, Clone)]
pub struct WorkerStateInner {
    // TODO: STUB
}


#[derive(Debug, Clone)]
pub struct WorkerBootstrapImpl {
    state: WorkerState,
}

impl WorkerBootstrapImpl {
    pub fn new(state: &WorkerState) -> Self {
        WorkerBootstrapImpl { state: state.clone() }
    }
}

impl worker_bootstrap::Server for WorkerBootstrapImpl {
    fn get_data_store(
        &mut self,
        _arg: worker_bootstrap::GetDataStoreParams,
        mut res: worker_bootstrap::GetDataStoreResults,
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




#[derive(Debug, Clone)]
pub struct WorkerControlImpl {
    state: WorkerState,
}

impl WorkerControlImpl {
    pub fn new(state: &WorkerState) -> Self {
        WorkerControlImpl { state: state.clone() }
    }
}

impl worker_control::Server for WorkerControlImpl {
    // TODO
}
