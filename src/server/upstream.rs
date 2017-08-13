
use server::state::State;
use server::worker::Worker;
use worker_capnp::worker_upstream;
use capnp::capability::Promise;
use std::process::exit;

pub struct WorkerUpstreamImpl {
    state: State,
    worker: Worker,
}

impl WorkerUpstreamImpl {
    pub fn new(state: &State, worker: &Worker) -> Self {
        Self {
            state: state.clone(),
            worker: worker.clone()
        }
    }
}

impl Drop for WorkerUpstreamImpl {
    fn drop(&mut self)
    {
        error!("Connection to worker {} lost", self.worker.get_id());
        self.state.remove_worker(&self.worker);
    }
}

impl worker_upstream::Server for WorkerUpstreamImpl {

}
