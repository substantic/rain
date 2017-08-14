
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

    fn wait(
        &mut self,
        params: client_service::WaitParams,
        _: client_service::WaitResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let task_ids = pry!(params.get_task_ids());
        let object_ids = pry!(params.get_object_ids());
        info!("New wait request ({} tasks, {} data objects) from client",
              task_ids.len(), object_ids.len());

        //TODO: Wait for tasks / dataobjs to finish

        Promise::ok(())
    }

    fn wait_some(
        &mut self,
        params: client_service::WaitSomeParams,
        mut results: client_service::WaitSomeResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let task_ids = pry!(params.get_task_ids());
        let object_ids = pry!(params.get_object_ids());
        info!("New wait_some request ({} tasks, {} data objects) from client",
              task_ids.len(), object_ids.len());

        //TODO: Wait for some tasks / dataobjs to finish.
        // Current implementation returns received task/object ids.

        results.get().set_finished_tasks(task_ids);
        results.get().set_finished_objects(object_ids);
        Promise::ok(())
    }
}
