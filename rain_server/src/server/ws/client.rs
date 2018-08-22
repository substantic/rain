use futures::future::{self, err, join_all, ok, Future};
use rain_core::{
    comm::client_message::{
        CloseSessionRequest, CloseSessionResponse, DataObjectState, DataObjectUpdate, FetchRequest,
        FetchResponse, FetchStatus, GetServerInfoRequest, GetServerInfoResponse, GetStateRequest,
        GetStateResponse, GovernorInfo, NewSessionRequest, NewSessionResponse,
        RegisterClientRequest, RegisterClientResponse, RpcError, RpcResult, SubmitRequest,
        SubmitResponse, TaskState, TaskUpdate, TerminateServerRequest, TerminateServerResponse,
        UnkeepRequest, UnkeepResponse, Update, WaitRequest, WaitResponse, WaitSomeRequest,
        WaitSomeResponse,
    },
    common_capnp::DataObjectState as CapnpDataObjectState,
    errors::SessionError,
    types::{DataObjectId, ObjectSpec, SId, TaskId, TaskSpec},
    utils::{FromCapnp, RcSet, ToCapnp},
    Error, ErrorKind, CLIENT_PROTOCOL_VERSION,
};
use server::{
    graph::{ClientRef, DataObjectRef, TaskRef},
    state::StateRef,
};
use std::net::SocketAddr;

type Response<T> = Box<Future<Item = T, Error = Error>>;

pub trait ClientService {
    fn register_client(&mut self, msg: RegisterClientRequest) -> Response<RegisterClientResponse>;
    fn new_session(&mut self, msg: NewSessionRequest) -> Response<NewSessionResponse>;
    fn close_session(&mut self, msg: CloseSessionRequest) -> Response<CloseSessionResponse>;
    fn get_server_info(&mut self, msg: GetServerInfoRequest) -> Response<GetServerInfoResponse>;
    fn submit(&mut self, msg: SubmitRequest) -> Response<SubmitResponse>;
    fn fetch(&mut self, msg: FetchRequest) -> Response<FetchResponse>;
    fn unkeep(&mut self, msg: UnkeepRequest) -> Response<UnkeepResponse>;
    fn wait(&mut self, msg: WaitRequest) -> Response<WaitResponse>;
    fn wait_some(&mut self, msg: WaitSomeRequest) -> Response<WaitSomeResponse>;
    fn get_state(&mut self, msg: GetStateRequest) -> Response<GetStateResponse>;
    fn terminate_server(
        &mut self,
        msg: TerminateServerRequest,
    ) -> Response<TerminateServerResponse>;
}

pub struct ClientServiceImpl {
    address: SocketAddr,
    state: StateRef,
    client: ClientRef,
    registered: bool,
}

impl ClientServiceImpl {
    pub fn new(address: SocketAddr, state: StateRef) -> Result<Self, Error> {
        let client = state.get_mut().add_client(address.clone())?;
        Ok(ClientServiceImpl {
            address,
            client,
            state,
            registered: false,
        })
    }

    fn check_registration(&self) -> Result<(), Error> {
        if !self.registered {
            bail!("Client not registered")
        } else {
            Ok(())
        }
    }
}

macro_rules! fry {
    ($result:expr) => {
        match $result {
            Ok(res) => res,
            Err(e) => return Box::new(err(e.into())),
        }
    };
}

macro_rules! from_capnp_list {
    ($builder:expr, $items:ident, $obj:ident) => {{
        $builder
            .$items()?
            .iter()
            .map(|item| $obj::from_capnp(&item))
            .collect()
    }};
}

fn convert_task_state(state: &::rain_core::common_capnp::TaskState) -> TaskState {
    match state {
        ::rain_core::common_capnp::TaskState::NotAssigned => TaskState::NotAssigned,
        ::rain_core::common_capnp::TaskState::Ready => TaskState::Ready,
        ::rain_core::common_capnp::TaskState::Assigned => TaskState::Assigned,
        ::rain_core::common_capnp::TaskState::Running => TaskState::Running,
        ::rain_core::common_capnp::TaskState::Finished => TaskState::Finished,
        ::rain_core::common_capnp::TaskState::Failed => TaskState::Failed,
    }
}
fn convert_object_state(state: &::rain_core::common_capnp::DataObjectState) -> DataObjectState {
    match state {
        ::rain_core::common_capnp::DataObjectState::Unfinished => DataObjectState::Unfinished,
        ::rain_core::common_capnp::DataObjectState::Finished => DataObjectState::Finished,
        ::rain_core::common_capnp::DataObjectState::Removed => DataObjectState::Removed,
    }
}

fn response<T: 'static>(response: T) -> Response<T> {
    Box::new(ok(response))
}
fn error<T: 'static>(error: Error) -> Response<T> {
    Box::new(err(error))
}

impl Drop for ClientServiceImpl {
    fn drop(&mut self) {
        let mut s = self.state.get_mut();
        info!("Client {} disconnected", self.client.get_id());
        s.remove_client(&self.client)
            .expect("client connection drop");
    }
}

impl ClientService for ClientServiceImpl {
    fn register_client(&mut self, msg: RegisterClientRequest) -> Response<RegisterClientResponse> {
        if self.registered {
            error!("Multiple registration from connection {}", self.address);
            return error("Connection already registered".into());
        }

        let version = msg.version as i32;
        if version != CLIENT_PROTOCOL_VERSION {
            error!(
                "Client protocol mismatch, expected {}, got {}",
                CLIENT_PROTOCOL_VERSION, version
            );
            return error(
                format!(
                    "Client protocol mismatch, expected {}, got {}",
                    CLIENT_PROTOCOL_VERSION, version
                ).into(),
            );
        }

        info!("Connection {} registered as client", self.address);

        self.registered = true;
        response(RegisterClientResponse {})
    }
    fn new_session(&mut self, msg: NewSessionRequest) -> Response<NewSessionResponse> {
        fry!(self.check_registration());

        let mut s = self.state.get_mut();
        let spec = ::serde_json::from_str(&msg.spec).unwrap();
        let session = fry!(s.add_session(&self.client, spec));

        debug!("Client asked for a new session, got {:?}", session.get_id());

        response(NewSessionResponse {
            session_id: session.get_id(),
        })
    }
    fn close_session(&mut self, msg: CloseSessionRequest) -> Response<CloseSessionResponse> {
        fry!(self.check_registration());

        let mut s = self.state.get_mut();
        let session = fry!(s.session_by_id(msg.session_id));
        s.remove_session(&session).unwrap();
        response(CloseSessionResponse {})
    }
    fn get_server_info(&mut self, _: GetServerInfoRequest) -> Response<GetServerInfoResponse> {
        fry!(self.check_registration());

        debug!("Client asked for info");
        let s = self.state.get();

        let futures: Vec<_> = s
            .graph
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
            }).collect();

        Box::new(
            join_all(futures)
                .and_then(move |rs| {
                    let mut governors = vec![];
                    for &(ref governor_id, ref r, ref resources) in rs.iter() {
                        let r = r.get()?;

                        governors.push(GovernorInfo {
                            governor_id: *governor_id,
                            tasks: from_capnp_list!(r, get_tasks, TaskId),
                            objects: from_capnp_list!(r, get_objects, DataObjectId),
                            objects_to_delete: from_capnp_list!(
                                r,
                                get_objects_to_delete,
                                DataObjectId
                            ),
                            resources: resources.clone(),
                        });
                    }
                    Ok(GetServerInfoResponse { governors })
                }).map_err(|e| e.into()),
        )
    }
    fn submit(&mut self, msg: SubmitRequest) -> Response<SubmitResponse> {
        fry!(self.check_registration());

        let mut s = self.state.get_mut();
        let tasks = msg.tasks;
        let mut objects = msg.objects;
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
        let res: Result<(), Error> = (|| {
            // first create the objects
            for co in objects.iter_mut() {
                let spec: ObjectSpec = ::serde_json::from_str(&co.spec).unwrap();
                let session = s.session_by_id(spec.id.session_id)?;

                let data = if co.has_data {
                    Some(::std::mem::replace(&mut co.data, vec![]))
                } else {
                    None
                };
                let o = s.add_object(&session, spec, co.keep, data)?;
                created_objects.push(o);
            }
            // second create the tasks
            for ct in tasks.iter() {
                let spec: TaskSpec = ::serde_json::from_str(&ct.spec).unwrap();
                let session = s.session_by_id(spec.id.get_session_id())?;
                let mut inputs = Vec::<DataObjectRef>::with_capacity(spec.inputs.len());
                for ci in spec.inputs.iter() {
                    inputs.push(s.object_by_id(ci.id)?);
                }
                let mut outputs = Vec::<DataObjectRef>::with_capacity(spec.outputs.len());
                for co in spec.outputs.iter() {
                    outputs.push(s.object_by_id(*co)?);
                }
                let t = s.add_task(&session, spec, inputs, outputs)?;
                created_tasks.push(t);
            }
            debug!("New tasks: {:?}", created_tasks);
            debug!("New objects: {:?}", created_objects);
            s.logger.add_client_submit_event(
                created_tasks.iter().map(|t| t.get().spec.clone()).collect(),
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
                fry!(s.remove_task(&t));
            }
            for o in created_objects {
                fry!(s.remove_object(&o));
            }
            fry!(res);
        }
        response(SubmitResponse {})
    }
    fn fetch(&mut self, msg: FetchRequest) -> Response<FetchResponse> {
        fry!(self.check_registration());

        let id = msg.id;
        debug!("Client fetch for object id={}", id);

        let object = match self.state.get().object_by_id_check_session(id) {
            Ok(t) => t,
            Err(Error(ErrorKind::SessionErr(ref e), _)) => {
                return response(FetchResponse::error(RpcError {
                    message: e.message.clone(),
                    debug: e.debug.clone(),
                    task: e.task_id.clone(),
                }));
            }
            Err(e) => return error(e.description().into()),
        };
        let object2 = object.clone();
        let mut obj = object2.get_mut();
        if obj.state == CapnpDataObjectState::Removed {
            return error(format!("create_reader on removed object {:?}", obj).into());
        }

        let size = msg.size;
        if size > 32 << 20
        /* 32 MB */
        {
            return response(FetchResponse::error(RpcError {
                message: "Fetch size is too big.".to_owned(),
                debug: "".to_owned(),
                task: TaskId {
                    id: 0,
                    session_id: 0,
                },
            }));
        }

        let offset = msg.offset;
        let include_info = msg.include_info;
        let session = obj.session.clone();
        let state_ref = self.state.clone();

        Box::new(
            obj.wait()
                .then(move |r| -> future::Either<_, _> {
                    if r.is_err() {
                        let session = session.get();
                        let error = session.get_error().as_ref().unwrap();
                        return future::Either::A(future::result(Ok(FetchResponse::error(
                            RpcError {
                                message: error.message.clone(),
                                debug: error.debug.clone(),
                                task: error.task_id.clone(),
                            },
                        ))));
                    }
                    let obj = object.get();
                    if obj.state == CapnpDataObjectState::Removed {
                        let session = session.get();
                        let error = session.get_error().as_ref().unwrap();
                        return future::Either::A(future::result(Ok(FetchResponse::error(
                            RpcError {
                                message: error.message.clone(),
                                debug: error.debug.clone(),
                                task: error.task_id.clone(),
                            },
                        ))));
                    }
                    assert_eq!(
                        obj.state,
                        CapnpDataObjectState::Finished,
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
                                        let result = r.get().unwrap();

                                        FetchResponse {
                                            status: FetchStatus::Ok,
                                            data: result.get_data().unwrap().to_vec(),
                                            info: result.get_info().unwrap().to_string(),
                                            transport_size: result.get_transport_size(),
                                        }
                                    }).map_err(|e| e.into())
                            }),
                    )
                }).map_err(|e| panic!("Fetch failed: {:?}", e)),
        )
    }
    fn unkeep(&mut self, msg: UnkeepRequest) -> Response<UnkeepResponse> {
        fry!(self.check_registration());

        let mut s = self.state.get_mut();
        let object_ids = msg.object_ids;
        debug!(
            "New unkeep request ({} data objects) from client",
            object_ids.len()
        );

        let mut objects = Vec::new();
        for oid in object_ids.iter() {
            match s.object_by_id_check_session(*oid) {
                Ok(obj) => objects.push(obj),
                Err(Error(ErrorKind::SessionErr(ref e), _)) => {
                    return response(UnkeepResponse {
                        status: RpcResult::Error(RpcError {
                            message: e.message.clone(),
                            debug: e.debug.clone(),
                            task: e.task_id.clone(),
                        }),
                    });
                }
                Err(e) => return error(e.description().into()),
            };
        }

        for o in objects.iter() {
            s.unkeep_object(&o);
        }
        s.logger
            .add_client_unkeep_event(objects.iter().map(|o| o.get().spec.id).collect());
        response(UnkeepResponse {
            status: RpcResult::Ok,
        })
    }
    fn wait(&mut self, msg: WaitRequest) -> Response<WaitResponse> {
        fry!(self.check_registration());

        fn session_error(error: &SessionError) -> WaitResponse {
            WaitResponse {
                status: RpcResult::Error(RpcError {
                    message: error.message.clone(),
                    debug: error.debug.clone(),
                    task: error.task_id.clone(),
                }),
            }
        };
        fn response_ok() -> WaitResponse {
            WaitResponse {
                status: RpcResult::Ok,
            }
        }

        let s = self.state.get_mut();
        let task_ids = msg.task_ids;
        let object_ids = msg.object_ids;
        info!(
            "New wait request ({} tasks, {} data objects) from client",
            task_ids.len(),
            object_ids.len()
        );

        if task_ids.len() == 1
            && object_ids.len() == 0
            && task_ids[0].id == ::rain_core::common_capnp::ALL_TASKS_ID
        {
            let session_id = task_ids[0].session_id;
            debug!("Waiting for all session session_id={}", session_id);
            let session = match s.session_by_id(session_id) {
                Ok(s) => s,
                Err(e) => return error(e.description().into()),
            };
            if let &Some(ref e) = session.get().get_error() {
                return response(session_error(e));
            }

            let session2 = session.clone();
            return Box::new(session.get_mut().wait().then(move |r| {
                ok(match r {
                    Ok(_) => response_ok(),
                    Err(_) => session_error(&session2.get().get_error().clone().unwrap()),
                })
            }));
        }

        let mut sessions = RcSet::new();

        // TODO: Wait for data objects
        // TODO: Implement waiting for session (for special "all" IDs)
        // TODO: Get rid of unwrap and do proper error handling

        let mut task_futures = Vec::new();

        for id in task_ids.iter() {
            match s.task_by_id_check_session(*id) {
                Ok(t) => {
                    let mut task = t.get_mut();
                    sessions.insert(task.session.clone());
                    if task.is_finished() {
                        continue;
                    }
                    task_futures.push(task.wait());
                }
                Err(Error(ErrorKind::SessionErr(ref e), _)) => {
                    return response(session_error(e));
                }
                Err(e) => return error(e.description().into()),
            };
        }

        debug!("{} waiting futures", task_futures.len());

        if task_futures.is_empty() {
            return Box::new(ok(response_ok()));
        }

        Box::new(join_all(task_futures).then(move |r| {
            ok(match r {
                Ok(_) => response_ok(),
                Err(_) => {
                    let session = sessions.iter().find(|s| s.get().is_failed()).unwrap();
                    session_error(&session.get().get_error().clone().unwrap())
                }
            })
        }))
    }
    fn wait_some(&mut self, msg: WaitSomeRequest) -> Response<WaitSomeResponse> {
        fry!(self.check_registration());

        let task_ids = msg.task_ids;
        let object_ids = msg.object_ids;
        info!(
            "New wait_some request ({} tasks, {} data objects) from client",
            task_ids.len(),
            object_ids.len()
        );
        error("wait_some is not implemented yet".into())
    }
    fn get_state(&mut self, msg: GetStateRequest) -> Response<GetStateResponse> {
        fry!(self.check_registration());

        fn session_error(error: &SessionError) -> GetStateResponse {
            GetStateResponse {
                update: Update {
                    status: RpcResult::Error(RpcError {
                        message: error.message.clone(),
                        debug: error.debug.clone(),
                        task: error.task_id.clone(),
                    }),
                    tasks: vec![],
                    objects: vec![],
                },
            }
        };

        let task_ids = msg.task_ids;
        let object_ids = msg.object_ids;
        info!(
            "New get_state request ({} tasks, {} data objects) from client",
            task_ids.len(),
            object_ids.len()
        );

        let s = self.state.get();
        let tasks: Vec<_> = match task_ids
            .iter()
            .map(|id| s.task_by_id_check_session(*id))
            .collect()
        {
            Ok(tasks) => tasks,
            Err(Error(ErrorKind::SessionErr(ref e), _)) => {
                return response(session_error(e));
            }
            Err(e) => return error(e.description().into()),
        };

        let objects: Vec<_> = match object_ids
            .iter()
            .map(|id| s.object_by_id_check_session(*id))
            .collect()
        {
            Ok(tasks) => tasks,
            Err(Error(ErrorKind::SessionErr(ref e), _)) => {
                return response(session_error(e));
            }
            Err(e) => return error(e.description().into()),
        };

        let mut update = Update {
            tasks: vec![],
            objects: vec![],
            status: RpcResult::Ok,
        };

        for task in tasks.iter() {
            let t = task.get();
            update.tasks.push(TaskUpdate {
                id: t.id(),
                state: convert_task_state(&t.state),
                info: ::serde_json::to_string(&t.info).unwrap(),
            });
        }
        for obj in objects.iter() {
            let o = obj.get();
            update.objects.push(DataObjectUpdate {
                id: o.id(),
                state: convert_object_state(&o.state),
                info: ::serde_json::to_string(&o.info).unwrap(),
            });
        }

        response(GetStateResponse { update })
    }
    fn terminate_server(&mut self, _: TerminateServerRequest) -> Response<TerminateServerResponse> {
        fry!(self.check_registration());

        response(TerminateServerResponse {})
    }
}
