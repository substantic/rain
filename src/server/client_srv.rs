
use capnp::capability::Promise;
use capnp;
use client_capnp::client_service;
use server::state::State;
use server::datastore::DataStoreImpl;

pub struct ClientServiceImpl {
    state: State,
}

impl ClientServiceImpl {
    pub fn new(state: &State) -> Self {
        Self { state: state.clone() }
    }
}

impl Drop for ClientServiceImpl {
    fn drop(&mut self)
    {
        // TODO handle client disconnections
        panic!("Client connection lost; not implemented yet");
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

    fn submit(
        &mut self,
        params: client_service::SubmitParams,
        _: client_service::SubmitResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let tasks = pry!(params.get_tasks());
        let dataobjs = pry!(params.get_objects());
        info!("New task submission ({} tasks, {} data objects) from client",
              tasks.len(), dataobjs.len());

        //TODO: Do something useful with received tasks

        Promise::ok(())
    }

    fn get_data_store(
        &mut self,
        params: client_service::GetDataStoreParams,
        mut results: client_service::GetDataStoreResults,
    ) -> Promise<(), ::capnp::Error> {
        let datastore = ::datastore_capnp::data_store::ToClient::new(
            DataStoreImpl::new(&self.state)).from_server::<::capnp_rpc::Server>();
        results.get().set_store(datastore);
        Promise::ok(())
    }
}
