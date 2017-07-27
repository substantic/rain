
use capnp::capability::Promise;
use capnp;
use client_capnp::client_service;
use server::state::State;

pub struct ClientServiceImpl {
    state: State,
}

impl ClientServiceImpl {
    pub fn new(state: &State) -> Self {
        Self { state: state.clone() }
    }
}

impl client_service::Server for ClientServiceImpl {

    fn get_info(&mut self,
                _: client_service::GetInfoParams,
                mut results: client_service::GetInfoResults)
                -> Promise<(), ::capnp::Error> {
        debug!("Client asked for info");
        results.get()
            .set_n_workers(self.state.get_n_workers() as i32);
        Promise::ok(())
    }
}
