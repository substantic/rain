use capnp::capability::Promise;
use std::net::SocketAddr;
use std::iter::FromIterator;
use std::collections::HashSet;
use futures::{Future, future};

use common::id::{DataObjectId, TaskId, SessionId, SId};
use common::convert::{FromCapnp, ToCapnp};
use client_capnp::client_service;
use server::state::StateRef;
use server::graph::{SessionRef, ClientRef, DataObjectRef, TaskRef, TaskInput, SessionError};
use errors::{Result, ResultExt, ErrorKind, Error};
use common::Additional;
use common::RcSet;
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

        let futures : Vec<_> = s.graph.workers.iter().map(|(worker_id, worker)| {
            let w = worker.get();
            let control = w.control.as_ref().unwrap();
            let worker_id = worker_id.clone();
            control.get_info_request().send().promise.map(move |r| (worker_id, r))
        }).collect();

        Promise::from_future(future::join_all(futures).map(move |rs| {
            let results = results.get();
            let mut workers = results.init_workers(rs.len() as u32);
            for (i, &(ref worker_id, ref r)) in rs.iter().enumerate() {
                let mut w = workers.borrow().get(i as u32);
                let r = r.get().unwrap();
                w.set_n_tasks(r.get_n_tasks());
                w.set_n_objects(r.get_n_objects());
                worker_id.to_capnp(&mut w.get_worker_id().unwrap());
            }
            ()
        }))
    }

    fn new_session(
        &mut self,
        _: client_service::NewSessionParams,
        mut results: client_service::NewSessionResults,
    ) -> Promise<(), ::capnp::Error> {
        let mut s = self.state.get_mut();
        let session = pry!(s.add_session(&self.client));
        results.get().set_session_id(session.get_id());
        debug!("Client asked for a new session, got {:?}", session.get_id());
        Promise::ok(())
    }

    fn close_session(
        &mut self,
        params: client_service::CloseSessionParams,
        _: client_service::CloseSessionResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let mut s = self.state.get_mut();
        let session = pry!(s.session_by_id(params.get_session_id()));
        s.remove_session(&session).unwrap();
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
        debug!("Sessions: {:?}", s.graph.sessions);
        let mut created_tasks = Vec::<TaskRef>::new();
        let mut created_objects = Vec::<DataObjectRef>::new();
        // catch any insertion error and clean up later
        let res: Result<()> = (|| {
            // first create the objects
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
            debug!("New tasks: {:?}", created_tasks);
            debug!("New objects: {:?}", created_objects);
            // verify submit integrity
            s.verify_submit(&created_tasks, &created_objects)
        })();
        if res.is_err() {
            debug!("Error: {:?}", res);
            for t in created_tasks {
                pry!(s.remove_task(&t));
            }
            for o in created_objects {
                pry!(s.remove_object(&o));
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
        debug!("server data store requested from client");
        let datastore = ::datastore_capnp::data_store::ToClient::new(
            ClientDataStoreImpl::new(&self.state)).from_server::<::capnp_rpc::Server>();
        results.get().set_store(datastore);
        Promise::ok(())
    }

    fn wait(
        &mut self,
        params: client_service::WaitParams,
        mut result: client_service::WaitResults,
    ) -> Promise<(), ::capnp::Error> {

        // Set error from session to result
        fn set_error(mut result: &mut ::common_capnp::unit_result::Builder, error: &SessionError) {
            error.to_capnp(&mut result.borrow().init_error());
        }

        let state = self.state.clone();
        let s = self.state.get_mut();
        let params = pry!(params.get());
        let task_ids = pry!(params.get_task_ids());
        let object_ids = pry!(params.get_object_ids());
        info!("New wait request ({} tasks, {} data objects) from client",
              task_ids.len(), object_ids.len());

        let mut sessions = RcSet::new();

        // TODO: Wait for data objects
        // TODO: Implement waiting for session (for special "all" IDs)
        // TODO: Get rid of unwrap and do proper error handling

        let mut task_futures = Vec::new();

        for id in task_ids.iter() {
            match s.task_by_id_check_session(TaskId::from_capnp(&id)) {
                Ok(t) => {
                    let mut task = t.get_mut();
                    sessions.insert(task.session.clone());
                    if task.is_finished() {
                        continue;
                    }
                    task_futures.push(task.wait());
                }
                Err(Error(ErrorKind::SessionErr(ref e), _)) => {
                    set_error(&mut result.get(), e);
                    return Promise::ok(())
                },
                Err(e) => return Promise::err(::capnp::Error::failed(e.description().to_string()))
            };
        }

        debug!("{} waiting futures", task_futures.len());

        if task_futures.is_empty() {
           result.get().set_ok(());
           return Promise::ok(());
        }

        Promise::from_future(::futures::future::join_all(task_futures)
                             .then(move |r| {
                                 match r {
                                     Ok(_) => result.get().set_ok(()),
                                     Err(_) => {
                                        let session = sessions.iter().find(|s| s.get().is_failed()).unwrap();
                                        set_error(&mut result.get(), session.get().get_error().as_ref().unwrap());
                                     }
                                 };
                                 Ok(())}
                             ))
    }

    fn wait_some(
        &mut self,
        params: client_service::WaitSomeParams,
        mut results: client_service::WaitSomeResults,
    ) -> Promise<(), ::capnp::Error> {
        let s = self.state.get_mut();
        let params = pry!(params.get());
        let task_ids = pry!(params.get_task_ids());
        let object_ids = pry!(params.get_object_ids());
        info!("New wait_some request ({} tasks, {} data objects) from client",
              task_ids.len(), object_ids.len());
        Promise::err(::capnp::Error::failed("wait_sone is not implemented yet".to_string()))
    }

    fn unkeep(
        &mut self,
        params: client_service::UnkeepParams,
        mut results: client_service::UnkeepResults,
    ) -> Promise<(), ::capnp::Error> {
        let mut s = self.state.get_mut();
        let params = pry!(params.get());
        let object_ids = pry!(params.get_object_ids());
        debug!("New unkeep request ({} data objects) from client",
              object_ids.len());

        let mut objects = Vec::new();
        for oid in object_ids.iter() {
            let id: DataObjectId = DataObjectId::from_capnp(&oid);
            match s.object_by_id_check_session(id) {
                Ok(obj) => objects.push(obj),
                Err(Error(ErrorKind::SessionErr(ref e), _)) => {
                    e.to_capnp(&mut results.get().init_error());
                    return Promise::ok(())
                },
                Err(e) => return Promise::err(::capnp::Error::failed(e.description().to_string()))
            };
        }

        for o in objects.iter() {
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
