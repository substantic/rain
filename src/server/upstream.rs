
use server::state::State;
use worker_capnp::worker_upstream;
use capnp::capability::Promise;
use std::process::exit;

pub struct WorkerUpstreamImpl {
    state: State,
}

impl WorkerUpstreamImpl {
    pub fn new(state: &State) -> Self {
        Self { state: state.clone() }
    }
}

impl Drop for WorkerUpstreamImpl {
    fn drop(&mut self) {
        error!("Connection to worker lost");
    }
}

impl worker_upstream::Server for WorkerUpstreamImpl {

}
