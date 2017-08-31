
use worker::StateRef;
use worker_capnp::worker_control;
use capnp::capability::Promise;
use std::process::exit;

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
        results.get().set_n_cpus(self.state.get_n_cpus());
        Promise::ok(())
    }
}
