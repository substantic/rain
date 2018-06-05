use capnp::capability::Promise;
use futures::{future, Future};
use std::net::SocketAddr;

use client_capnp::client_service;
use common::{RcSet, TaskSpec, ObjectSpec};
use common::convert::{FromCapnp, ToCapnp};
use common::id::{DataObjectId, SId, TaskId};
use errors::{Error, ErrorKind, Result};
use server::graph::{ClientRef, SessionError, TaskRef};
use server::graph::{DataObjectRef, DataObjectState};
use server::state::StateRef;

pub struct ClientServiceImpl {
    state: StateRef,
    client: ClientRef,
}

impl ClientServiceImpl {
    pub fn new(state: &StateRef, address: &SocketAddr) -> Result<Self> {
        Ok(Self {
            state: state.clone(),
            client: state.get_mut().add_client(address.clone())?,
        })
    }
}

impl Drop for ClientServiceImpl {
    fn drop(&mut self) {
        let mut s = self.state.get_mut();
        info!("Client {} disconnected", self.client.get_id());
        s.remove_client(&self.client)
            .expect("client connection drop");
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

        let futures: Vec<_> = s.graph
            .governors
            .iter()
            .map(|(governor_id, governor)| {
                let w = governor.get();
                let control = w.control.as_ref().unwrap();
                let governor_id = governor_id.clone();
                let resources = w.resources.clone();
                control
                    .get_info_request()
                    .send()
                    .promise
                    .map(move |r| (governor_id, r, resources))
            })
            .collect();

        Promise::from_future(future::join_all(futures).map(move |rs| {
            let results = results.get();
            let mut governors = results.init_governors(rs.len() as u32);
            for (i, &(ref governor_id, ref r, ref resources)) in rs.iter().enumerate() {
                let mut w = governors.reborrow().get(i as u32);
                let r = r.get().unwrap();
                w.set_tasks(r.get_tasks().unwrap()).unwrap();
                w.set_objects(r.get_objects().unwrap()).unwrap();
                w.set_objects_to_delete(r.get_objects_to_delete().unwrap())
                    .unwrap();
                resources.to_capnp(&mut w.reborrow().get_resources().unwrap());
                governor_id.to_capnp(&mut w.get_governor_id().unwrap());
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
        info!(
            "New task submission ({} tasks, {} data objects) from client {}",
            tasks.len(),
            objects.len(),
            self.client.get_id()
        );
        debug!("Sessions: {:?}", s.graph.sessions);
        let mut created_tasks = Vec::<TaskRef>::new();
        let mut created_objects = Vec::<DataObjectRef>::new();
        // catch any insertion error and clean up later
        let res: Result<()> = (|| {
            // first create the objects
            for co in objects.iter() {
                let spec: ObjectSpec = ::serde_json::from_str(co.get_spec().unwrap()).unwrap();
                let session = s.session_by_id(spec.id.get_session_id())?;
                let data = if co.get_has_data() {
                    Some(co.get_data()?.into())
                } else {
                    None
                };
                let o = s.add_object(
                    &session,
                    spec,
                    co.get_keep(),
                    data,
                )?;
                created_objects.push(o);
            }
            // second create the tasks
            for ct in tasks.iter() {
                let spec: TaskSpec = ::serde_json::from_str(ct.get_spec().unwrap()).unwrap();
                let session = s.session_by_id(spec.id.get_session_id())?;
                let mut inputs = Vec::<DataObjectRef>::with_capacity(spec.inputs.len());
                for ci in spec.inputs.iter() {
                    inputs.push(s.object_by_id(ci.id)?);
                }
                let mut outputs = Vec::<DataObjectRef>::with_capacity(spec.outputs.len());
                for co in spec.outputs.iter() {
                    outputs.push(s.object_by_id(*co)?);
                }
                let t = s.add_task(
                    &session,
                    spec,
                    inputs,
                    outputs,
                )?;
                created_tasks.push(t);
            }
            debug!("New tasks: {:?}", created_tasks);
            debug!("New objects: {:?}", created_objects);
            s.logger.add_client_submit_event(
                created_tasks
                    .iter()
                    .map(|t| t.get().spec.clone())
                    .collect(),
                created_objects
                    .iter()
                    .map(|o| o.get().spec.clone())
                    .collect(),
            );
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

    fn wait(
        &mut self,
        params: client_service::WaitParams,
        mut result: client_service::WaitResults,
    ) -> Promise<(), ::capnp::Error> {
        // Set error from session to result
        fn set_error(result: &mut ::common_capnp::unit_result::Builder, error: &SessionError) {
            error.to_capnp(&mut result.reborrow().init_error());
        }

        let s = self.state.get_mut();
        let params = pry!(params.get());
        let task_ids = pry!(params.get_task_ids());
        let object_ids = pry!(params.get_object_ids());
        info!(
            "New wait request ({} tasks, {} data objects) from client",
            task_ids.len(),
            object_ids.len()
        );

        if task_ids.len() == 1 && object_ids.len() == 0
            && task_ids.get(0).get_id() == ::common_capnp::ALL_TASKS_ID
        {
            let session_id = task_ids.get(0).get_session_id();
            debug!("Waiting for all session session_id={}", session_id);
            let session = match s.session_by_id(session_id) {
                Ok(s) => s,
                Err(e) => return Promise::err(::capnp::Error::failed(e.description().to_string())),
            };
            if let &Some(ref e) = session.get().get_error() {
                set_error(&mut result.get(), e);
                return Promise::ok(());
            }

            let session2 = session.clone();
            return Promise::from_future(session.get_mut().wait().then(move |r| {
                match r {
                    Ok(_) => result.get().set_ok(()),
                    Err(_) => {
                        set_error(
                            &mut result.get(),
                            session2.get().get_error().as_ref().unwrap(),
                        );
                    }
                };
                Ok(())
            }));
        }

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
                    return Promise::ok(());
                }
                Err(e) => return Promise::err(::capnp::Error::failed(e.description().to_string())),
            };
        }

        debug!("{} waiting futures", task_futures.len());

        if task_futures.is_empty() {
            result.get().set_ok(());
            return Promise::ok(());
        }

        Promise::from_future(::futures::future::join_all(task_futures).then(move |r| {
            match r {
                Ok(_) => result.get().set_ok(()),
                Err(_) => {
                    let session = sessions.iter().find(|s| s.get().is_failed()).unwrap();
                    set_error(
                        &mut result.get(),
                        session.get().get_error().as_ref().unwrap(),
                    );
                }
            };
            Ok(())
        }))
    }

    fn wait_some(
        &mut self,
        params: client_service::WaitSomeParams,
        _results: client_service::WaitSomeResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let task_ids = pry!(params.get_task_ids());
        let object_ids = pry!(params.get_object_ids());
        info!(
            "New wait_some request ({} tasks, {} data objects) from client",
            task_ids.len(),
            object_ids.len()
        );
        Promise::err(::capnp::Error::failed(
            "wait_sone is not implemented yet".to_string(),
        ))
    }

    fn unkeep(
        &mut self,
        params: client_service::UnkeepParams,
        mut results: client_service::UnkeepResults,
    ) -> Promise<(), ::capnp::Error> {
        let mut s = self.state.get_mut();
        let params = pry!(params.get());
        let object_ids = pry!(params.get_object_ids());
        debug!(
            "New unkeep request ({} data objects) from client",
            object_ids.len()
        );

        let mut objects = Vec::new();
        for oid in object_ids.iter() {
            let id: DataObjectId = DataObjectId::from_capnp(&oid);
            match s.object_by_id_check_session(id) {
                Ok(obj) => objects.push(obj),
                Err(Error(ErrorKind::SessionErr(ref e), _)) => {
                    e.to_capnp(&mut results.get().init_error());
                    return Promise::ok(());
                }
                Err(e) => return Promise::err(::capnp::Error::failed(e.description().to_string())),
            };
        }

        for o in objects.iter() {
            s.unkeep_object(&o);
        }
        s.logger
            .add_client_unkeep_event(objects.iter().map(|o| o.get().spec.id).collect());
        Promise::ok(())
    }

    fn fetch(
        &mut self,
        params: client_service::FetchParams,
        mut results: client_service::FetchResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let id = DataObjectId::from_capnp(&pry!(params.get_id()));

        debug!("Client fetch for object id={}", id);

        let object = match self.state.get().object_by_id_check_session(id) {
            Ok(t) => t,
            Err(Error(ErrorKind::SessionErr(ref e), _)) => {
                e.to_capnp(&mut results.get().get_status().init_error());
                return Promise::ok(());
            }
            Err(e) => return Promise::err(::capnp::Error::failed(e.description().to_string())),
        };
        let object2 = object.clone();
        let mut obj = object2.get_mut();
        if obj.state == DataObjectState::Removed {
            return Promise::err(::capnp::Error::failed(format!(
                "create_reader on removed object {:?}",
                object.get()
            )));
        }

        let size = params.get_size();

        if size > 32 << 20
        /* 32 MB */
        {
            let mut err = results.get().get_status().init_error();
            err.set_message("Fetch size is too big.");
            return Promise::ok(());
        }

        let offset = params.get_offset();
        let include_info = params.get_include_info();
        let session = obj.session.clone();
        let state_ref = self.state.clone();

        Promise::from_future(
            obj.wait()
                .then(move |r| -> future::Either<_, _> {
                    if r.is_err() {
                        let session = session.get();
                        session
                            .get_error()
                            .as_ref()
                            .unwrap()
                            .to_capnp(&mut results.get().get_status().init_error());
                        return future::Either::A(future::result(Ok(())));
                    }
                    let obj = object.get();
                    if obj.state == DataObjectState::Removed {
                        let session = session.get();
                        session
                            .get_error()
                            .as_ref()
                            .unwrap()
                            .to_capnp(&mut results.get().get_status().init_error());
                        return future::Either::A(future::result(Ok(())));
                    }
                    assert_eq!(
                        obj.state,
                        DataObjectState::Finished,
                        "triggered finish hook on unfinished object"
                    );

                    if obj.data.is_some() {
                        // Fetching uploaded objects is not implemented yet
                        unimplemented!();
                    }
                    let governor_ref = obj.located.iter().next().unwrap().clone();
                    let mut governor = governor_ref.get_mut();
                    debug!("Redirecting client fetch id={} to {}", governor.id(), id);
                    future::Either::B(
                        governor
                            .wait_for_data_connection(&governor_ref, &state_ref)
                            .and_then(move |data_conn| {
                                let mut req = data_conn.fetch_request();
                                {
                                    let mut request = req.get();
                                    request.set_offset(offset);
                                    request.set_size(size);
                                    request.set_include_info(include_info);
                                    id.to_capnp(&mut request.get_id().unwrap());
                                }
                                req.send()
                                    .promise
                                    .map(move |r| {
                                        results.set(r.get().unwrap()).unwrap();
                                    })
                                    .map_err(|e| e.into())
                            }),
                    )
                })
                .map_err(|e| panic!("Fetch failed: {:?}", e)),
        )
    }

    fn get_state(
        &mut self,
        params: client_service::GetStateParams,
        mut results: client_service::GetStateResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let task_ids = pry!(params.get_task_ids());
        let object_ids = pry!(params.get_object_ids());
        info!(
            "New get_state request ({} tasks, {} data objects) from client",
            task_ids.len(),
            object_ids.len()
        );

        let s = self.state.get();
        let tasks: Vec<_> = match task_ids
            .iter()
            .map(|id| s.task_by_id_check_session(TaskId::from_capnp(&id)))
            .collect()
        {
            Ok(tasks) => tasks,
            Err(Error(ErrorKind::SessionErr(ref e), _)) => {
                e.to_capnp(&mut results.get().get_state().unwrap().init_error());
                return Promise::ok(());
            }
            Err(e) => return Promise::err(::capnp::Error::failed(e.description().to_string())),
        };

        let objects: Vec<_> = match object_ids
            .iter()
            .map(|id| s.object_by_id_check_session(DataObjectId::from_capnp(&id)))
            .collect()
        {
            Ok(tasks) => tasks,
            Err(Error(ErrorKind::SessionErr(ref e), _)) => {
                e.to_capnp(&mut results.get().get_state().unwrap().init_error());
                return Promise::ok(());
            }
            Err(e) => return Promise::err(::capnp::Error::failed(e.description().to_string())),
        };

        let mut results = results.get();

        {
            let mut task_updates = results.reborrow().init_tasks(tasks.len() as u32);
            for (i, task) in tasks.iter().enumerate() {
                let mut update = task_updates.reborrow().get(i as u32);
                let t = task.get();
                update.set_info(&::serde_json::to_string(&t.info).unwrap());
                t.spec.id.to_capnp(&mut update.get_id().unwrap());
            }
        }

        {
            let mut obj_updates = results.reborrow().init_objects(objects.len() as u32);
            for (i, obj) in objects.iter().enumerate() {
                let mut update = obj_updates.reborrow().get(i as u32);
                let o = obj.get();
                update.set_info(&::serde_json::to_string(&o.info).unwrap());
                o.spec.id.to_capnp(&mut update.get_id().unwrap());
            }
        }

        results.get_state().unwrap().set_ok(());
        Promise::ok(())
    }
}
