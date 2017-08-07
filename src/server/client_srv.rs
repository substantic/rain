
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
    fn get_server_info(
        &mut self,
        _: client_service::GetServerInfoParams,
        mut results: client_service::GetServerInfoResults,
    ) -> Promise<(), ::capnp::Error> {
        debug!("Client asked for info");
        results.get().set_n_workers(
            self.state.get_n_workers() as i32,
        );
        Promise::ok(())
    }

    fn new_session(
        &mut self,
        _: client_service::NewSessionParams,
        mut results: client_service::NewSessionResults,
    ) -> Promise<(), ::capnp::Error> {
        info!("Client asked for a new session");
        let session_id = self.state.new_session();
        results.get().set_session_id(session_id);
        Promise::ok(())
    }
}
