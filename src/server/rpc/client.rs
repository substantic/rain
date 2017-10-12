use capnp::capability::Promise;
use std::net::SocketAddr;
use std::iter::FromIterator;
use futures::{Future};

use common::id::{DataObjectId, TaskId, SessionId, SId};
use common::convert::{FromCapnp, ToCapnp};
use client_capnp::client_service;
use server::state::StateRef;
use server::graph::{SessionRef, ClientRef, DataObjectRef, TaskRef, TaskInput};
use errors::{Result, ResultExt};
use common::Additional;
use server::rpc::ClientDataStoreImpl;

pub struct ClientServiceImpl {
    state: StateRef,
    client: ClientRef,
}

impl ClientServiceImpl {
    pub fn new(state: &StateRef, address: &SocketAddr)  -> Result<Self> {
        Ok(Self {
            state: state.clone(),
            client: state.get_mut().add_client(address.clone())?,
        })
    }
}

impl Drop for ClientServiceImpl {
    fn drop(&mut self)
    {
        let mut s = self.state.get_mut();
        info!("Client {} disconnected", self.client.get_id());
        s.remove_client(&self.client).expect("client connection drop");
    }
}

impl client_service::Server for ClientServiceImpl {
    fn get_server_info(
        &mut self,
        _: client_service::GetServerInfoParams,
        mut results: client_service::GetServerInfoResults,
    ) -> Promise<(), ::capnp::Error> {
        debug!("Client asked for info");
        let s = self.state.get();
        results.get().set_n_workers(
            s.graph.workers.len() as i32,
        );
        Promise::ok(())
    }

    fn new_session(
        &mut self,
        _: client_service::NewSessionParams,
        mut results: client_service::NewSessionResults,
    ) -> Promise<(), ::capnp::Error> {
        debug!("Client asked for a new session");
        let mut s = self.state.get_mut();
        let session = pry!(s.add_session(&self.client));
        results.get().set_session_id(session.get_id());
        Promise::ok(())
    }

    fn submit(
        &mut self,
        params: client_service::SubmitParams,
        _: client_service::SubmitResults,
    ) -> Promise<(), ::capnp::Error> {
        let mut s = self.state.get_mut();
        let params = pry!(params.get());
        let tasks = pry!(params.get_tasks());
        let objects = pry!(params.get_objects());
        info!("New task submission ({} tasks, {} data objects) from client {}",
              tasks.len(), objects.len(), self.client.get_id());
        let mut created_tasks = Vec::<TaskRef>::new();
        let mut created_objects = Vec::<DataObjectRef>::new();
        // catch any insertion error and clean up later
        let res: Result<()> = (|| {
            // first ceate the objects
            for co in objects.iter() {
                let id = DataObjectId::from_capnp(&co.get_id()?);
                let session = s.session_by_id(id.get_session_id())?;
                let data =
                    if co.get_has_data() {
                        Some(co.get_data()?.into())
                    } else {
                        None
                    };
                let additional = Additional::new(); // TODO: decode additional
                let o = s.add_object(&session, id,co.get_type().map_err(|_| "reading TaskType")?,
                                     co.get_keep(), co.get_label()?.to_string(),data, additional)?;
                created_objects.push(o);
            }
            // second create the tasks
            for ct in tasks.iter() {
                let id = TaskId::from_capnp(&ct.get_id()?);
                let session = s.session_by_id(id.get_session_id())?;
                let additional = Additional::new(); // TODO: decode additional
                let mut inputs = Vec::<TaskInput>::new();
                for ci in ct.get_inputs()?.iter() {
                    inputs.push(TaskInput {
                        object: s.object_by_id(DataObjectId::from_capnp(&ci.get_id()?))?,
                        label: ci.get_label()?.into(),
                        path: ci.get_path()?.into(),
                    });
                }
                let mut outputs = Vec::<DataObjectRef>::new();
                for co in ct.get_outputs()?.iter() {
                    outputs.push(s.object_by_id(DataObjectId::from_capnp(&co))?);
                }
                let t = s.add_task(&session, id, inputs, outputs,
                                   ct.get_task_type()?.to_string(), ct.get_task_config()?.into(),
                                   additional)?;
                created_tasks.push(t);
            }
            // verify submit integrity
            s.verify_submit(&created_tasks, &created_objects)
        })();
        if res.is_err() {
            for t in created_tasks {
                s.remove_task(&t);
            }
            for o in created_objects {
                s.remove_object(&o);
            }
            pry!(res);
        }
        Promise::ok(())
    }

    fn get_data_store(
        &mut self,
        params: client_service::GetDataStoreParams,
        mut results: client_service::GetDataStoreResults,
    ) -> Promise<(), ::capnp::Error> {
        let datastore = ::datastore_capnp::data_store::ToClient::new(
            ClientDataStoreImpl::new(&self.state)).from_server::<::capnp_rpc::Server>();
        results.get().set_store(datastore);
        Promise::ok(())
    }

    fn wait(
        &mut self,
        params: client_service::WaitParams,
        _: client_service::WaitResults,
    ) -> Promise<(), ::capnp::Error> {
        let mut s = self.state.get_mut();
        let params = pry!(params.get());
        let task_ids = pry!(params.get_task_ids());
        let object_ids = pry!(params.get_object_ids());
        info!("New wait request ({} tasks, {} data objects) from client",
              task_ids.len(), object_ids.len());

        // TODO: Get rid of unwrap and do proper error handling
        let futures: Vec<_> = task_ids.iter()
            .map(|id| s.task_by_id(TaskId::from_capnp(&id)).unwrap())
            .filter(|t| !t.get().is_finished())
            .map(|t| t.get_mut().wait())
            .collect();

        debug!("{} waiting futures", futures.len());

        // TODO: Wait for data objects
        Promise::from_future(::futures::future::join_all(futures)
                             .map(|_| ())
                             .map_err(|e| panic!("Wait failed")))
    }

    fn wait_some(
        &mut self,
        params: client_service::WaitSomeParams,
        mut results: client_service::WaitSomeResults,
    ) -> Promise<(), ::capnp::Error> {
        let mut s = self.state.get_mut();
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
        let mut s = self.state.get_mut();
        let params = pry!(params.get());
        let object_ids = pry!(params.get_object_ids());
        debug!("New unkeep request ({} data objects) from client",
              object_ids.len());

        for oid in object_ids.iter() {
            let id: DataObjectId = DataObjectId::from_capnp(&oid);
            let o: DataObjectRef = pry!(s.object_by_id(id));
            s.unkeep_object(&o);
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
