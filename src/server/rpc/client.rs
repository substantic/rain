use capnp::capability::Promise;
use std::net::SocketAddr;

use common::id::{DataObjectId, TaskId, SessionId};
use common::convert::{FromCapnp, ToCapnp};
use client_capnp::client_service;
use server::state::State;
use super::datastore::DataStoreImpl;
use server::graph::{Session, Client, DataObject, Task};

pub struct ClientServiceImpl {
    state: State,
    client: Client,
}

impl ClientServiceImpl {
    pub fn new(state: &State, address: &SocketAddr) -> Self {
        Self { state: state.clone(), client: Client::new(&state.get().graph, address.clone()), }
    }
}

impl Drop for ClientServiceImpl {
    fn drop(&mut self)
    {
        info!("Client {} disconnected", self.client.get_id());
        self.state.remove_client(&self.client);
    }
}

impl client_service::Server for ClientServiceImpl {
    fn get_server_info(
        &mut self,
        _: client_service::GetServerInfoParams,
        mut results: client_service::GetServerInfoResults,
    ) -> Promise<(), ::capnp::Error> {
        debug!("Client asked for info");
        let g = &self.state.get().graph;
        results.get().set_n_workers(
            g.get().workers.len() as i32,
        );
        Promise::ok(())
    }

    fn new_session(
        &mut self,
        _: client_service::NewSessionParams,
        mut results: client_service::NewSessionResults,
    ) -> Promise<(), ::capnp::Error> {
        debug!("Client asked for a new session");
        let session = self.state.add_session(&self.client);
        results.get().set_session_id(session.get_id());
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

        pry!(results.get().set_finished_tasks(task_ids));
        pry!(results.get().set_finished_objects(object_ids));
        Promise::ok(())
    }

    fn unkeep(
        &mut self,
        params: client_service::UnkeepParams,
        _: client_service::UnkeepResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let object_ids = pry!(params.get_object_ids());
        info!("New unkeep request ({} data objects) from client",
              object_ids.len());

        for oid in object_ids.iter() {
            let id: DataObjectId = DataObjectId::from_capnp(&oid);
            let o: DataObject = pry!(self.state.get_object(id));
            self.state.unkeep_object(&o);
        }

        Promise::ok(())
    }

    fn get_state(
        &mut self,
        params: client_service::GetStateParams,
        mut results: client_service::GetStateResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let task_ids = pry!(params.get_task_ids());
        let object_ids = pry!(params.get_object_ids());
        info!("New get_state request ({} tasks, {} data objects) from client",
              task_ids.len(), object_ids.len());

        {
            let mut task_updates = results.get().init_tasks(task_ids.len());
            for i in 0..task_ids.len() {
                let mut update = task_updates.borrow().get(i);
                pry!(update.set_id(task_ids.get(i)));

                //TODO: set current task state
                //update.set_state(...)
            }
        }

        {
            let mut object_updates = results.get().init_objects(object_ids.len());
            for i in 0..object_ids.len() {
                let mut update = object_updates.borrow().get(i);
                pry!(update.set_id(object_ids.get(i)));

                //TODO: set current data object state
                //update.set_state(...)
            }
        }
        Promise::ok(())
    }
}
