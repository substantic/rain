use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::rc::Rc;
use std::time::{Duration, Instant};

use common::Attributes;
use common::DataType;
use common::RcSet;
use common::asyncinit::AsyncInitWrapper;
use common::comm::Connection;
use common::convert::{FromCapnp, ToCapnp};
use common::events;
use common::fs::logdir::LogDir;
use common::id::{empty_governor_id, DataObjectId, GovernorId, TaskId};
use common::monitor::Monitor;
use common::resources::Resources;
use common::wrapped::WrappedRcRefCell;

use governor::data::Data;
use governor::data::transport::TransportView;
use governor::fs::workdir::WorkDir;
use governor::graph::{executor_command, DataObject, DataObjectRef, DataObjectState, ExecutorRef,
                      Graph, TaskInput, TaskRef, TaskState};
use governor::rpc::GovernorControlImpl;
use governor::rpc::executor::check_registration;
use governor::graph::executor::get_log_tails;
use governor::rpc::executor_serde::ExecutorToGovernorMessage;
use governor::tasks::TaskInstance;

use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp;
use errors::{Error, Result};
use futures::Future;
use futures::IntoFuture;
use futures::Stream;
use tokio_core::net::TcpListener;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_uds::UnixListener;

use GOVERNOR_PROTOCOL_VERSION;

const MONITORING_INTERVAL: u64 = 5; // Monitoring interval in seconds
const DELETE_WAIT_LIST_INTERVAL: u64 = 2; // How often is delete_wait_list checked in seconds
const DEFAULT_DELETE_LIST_MAX_TIMEOUT: u32 = 5;
const DEFAULT_TRANSPORT_VIEW_TIMEOUT: u32 = 10;

pub struct State {
    pub(super) graph: Graph,

    /// If true, next "turn" the scheduler is executed
    need_scheduling: bool,

    /// Tokio core handle
    handle: Handle,

    /// Handle to GovernorUpstream (that resides in server)
    upstream: Option<::governor_capnp::governor_upstream::Client>,

    remote_governors:
        HashMap<GovernorId, AsyncInitWrapper<::governor_capnp::governor_bootstrap::Client>>,

    updated_objects: RcSet<DataObjectRef>,
    updated_tasks: RcSet<TaskRef>,

    /// Transport views (2nd element of tuple is timeout)
    transport_views: HashMap<DataObjectId, (Rc<TransportView>, ::std::time::Instant)>,

    /// A governor assigned to this governor
    governor_id: GovernorId,

    /// This is hard limit for number of simultaneously executed tasks
    /// The purpose is to limit task with empty resources
    /// The initial value is 4 * n_cpus
    free_slots: u32,

    resources: Resources,

    free_resources: Resources,

    /// Path to working directory
    work_dir: WorkDir,

    log_dir: LogDir,

    delete_list_max_timeout: u32,

    monitor: Monitor,

    // Map from name of executors to its arguments
    // e.g. "py" => ["python", "-m", "rain.executor"]
    executor_args: HashMap<String, Vec<String>>,

    self_ref: Option<StateRef>,
}

pub type StateRef = WrappedRcRefCell<State>;

impl State {
    #[inline]
    pub fn work_dir(&self) -> &WorkDir {
        &self.work_dir
    }

    #[inline]
    pub fn log_dir(&self) -> &LogDir {
        &self.log_dir
    }

    #[inline]
    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    #[inline]
    pub fn governor_id(&self) -> &GovernorId {
        &self.governor_id
    }

    #[inline]
    pub fn upstream(&self) -> &Option<::governor_capnp::governor_upstream::Client> {
        &self.upstream
    }

    pub fn plan_scheduling(&mut self) {
        unimplemented!();
    }

    pub fn get_resources(&self) -> &Resources {
        &self.resources
    }

    /// Start scheduler in next loop
    pub fn need_scheduling(&mut self) {
        self.need_scheduling = true;
    }

    pub fn get_transport_view(&mut self, id: DataObjectId) -> Option<Rc<TransportView>> {
        let now = ::std::time::Instant::now();
        let new_timeout =
            now + ::std::time::Duration::from_secs(DEFAULT_TRANSPORT_VIEW_TIMEOUT as u64);

        if let ::std::collections::hash_map::Entry::Occupied(mut e) = self.transport_views.entry(id)
        {
            debug!("Getting transport view from cache id={}", id);
            let &mut (ref tw, ref mut timeout) = e.get_mut();
            *timeout = new_timeout;
            return Some(tw.clone());
        }
        self.graph.objects.get(&id).cloned().map(|obj_ref| {
            debug!("Creating new transport view for object id={}", id);
            let transport_view = Rc::new(TransportView::from(self, obj_ref.get().data()).unwrap());
            self.transport_views
                .insert(id, (transport_view.clone(), new_timeout));
            transport_view
        })
    }

    pub fn add_task(
        &mut self,
        id: TaskId,
        inputs: Vec<TaskInput>,
        outputs: Vec<DataObjectRef>,
        resources: Resources,
        task_type: String,
        attributes: Attributes,
    ) -> TaskRef {
        let task = TaskRef::new(
            &mut self.graph,
            id,
            inputs,
            outputs,
            resources,
            task_type,
            attributes,
        );
        if task.get().is_ready() {
            self.graph.ready_tasks.push(task.clone());
        }
        task
    }

    pub fn object_by_id(&self, id: DataObjectId) -> Result<DataObjectRef> {
        match self.graph.objects.get(&id) {
            Some(o) => Ok(o.clone()),
            None => Err(format!("Object {:?} not found", id))?,
        }
    }

    pub fn task_by_id(&self, id: TaskId) -> Result<TaskRef> {
        match self.graph.tasks.get(&id) {
            Some(t) => Ok(t.clone()),
            None => Err(format!("Task {:?} not found", id))?,
        }
    }

    pub fn object_is_finished(&mut self, dataobj: &DataObjectRef) {
        let mut dataobject = dataobj.get_mut();
        if dataobject.is_removed() {
            debug!("Removed object finished id={}", dataobject.id);
            return;
        }
        debug!("Object id={} is finished", dataobject.id);
        self.updated_objects.insert(dataobj.clone());

        let mut new_ready = false;
        for task in &dataobject.consumers {
            if task.get_mut().input_finished(dataobj) {
                self.graph.ready_tasks.push(task.clone());
                new_ready = true;
            }
        }

        if new_ready {
            self.need_scheduling();
        }

        self.remove_dataobj_if_not_needed(&mut dataobject);
    }

    /// Send status of updated elements (updated_tasks/updated_objects) and then clear this sets
    pub fn send_update(&mut self) {
        debug!(
            "Sending update objs={}, tasks={}",
            self.updated_objects.len(),
            self.updated_tasks.len()
        );

        let mut req = self.upstream.as_ref().unwrap().update_states_request();

        {
            // Data Objects
            let req_update = req.get().get_update().unwrap();
            let mut req_objs = req_update.init_objects(self.updated_objects.len() as u32);

            for (i, object) in self.updated_objects.iter().enumerate() {
                let mut co = req_objs.reborrow().get(i as u32);
                let mut object = object.get_mut();

                if object.is_finished() {
                    co.set_state(::common_capnp::DataObjectState::Finished);
                    co.set_size(object.data().size() as u64);
                } else {
                    // TODO: Handle failure state
                    panic!("Updating non finished object");
                }

                if !object.new_attributes.is_empty() {
                    object
                        .new_attributes
                        .to_capnp(&mut co.reborrow().get_attributes().unwrap());
                    object.new_attributes.clear();
                }
                object.id.to_capnp(&mut co.get_id().unwrap());
            }

            self.updated_objects.clear();
        }

        {
            // Tasks
            let req_update = req.get().get_update().unwrap();
            let mut req_tasks = req_update.init_tasks(self.updated_tasks.len() as u32);

            for (i, task) in self.updated_tasks.iter().enumerate() {
                let mut ct = req_tasks.reborrow().get(i as u32);
                let mut task = task.get_mut();

                ct.set_state(match task.state {
                    TaskState::Running => ::common_capnp::TaskState::Running,
                    TaskState::Finished => ::common_capnp::TaskState::Finished,
                    TaskState::Failed => ::common_capnp::TaskState::Failed,
                    _ => panic!("Invalid state"),
                });

                if !task.new_attributes.is_empty() {
                    task.new_attributes
                        .to_capnp(&mut ct.reborrow().get_attributes().unwrap());
                    task.new_attributes.clear();
                }
                task.id.to_capnp(&mut ct.get_id().unwrap());
            }

            self.updated_tasks.clear();
        }

        self.spawn_panic_on_error(req.send().promise.map(|_| ()).map_err(|e| e.into()));
    }

    fn executor_cleanup(&mut self, executor_ref: &ExecutorRef) {
        self.graph.idle_executors.remove(&executor_ref);
        for (_, obj_ref) in &self.graph.objects {
            obj_ref.get_mut().executor_cache.remove(&executor_ref);
        }
    }

    pub fn get_executor(
        &mut self,
        executor_type: &str,
    ) -> Result<Box<Future<Item = ExecutorRef, Error = Error>>> {
        use tokio_process::CommandExt;

        let sw_result = self.graph
            .idle_executors
            .iter()
            .find(|sw| sw.get().executor_type() == executor_type)
            .cloned();
        match sw_result {
            None => {
                if let Some(args) = self.executor_args.get(executor_type) {
                    let executor_id = self.graph.make_id();
                    let executor_type = executor_type.to_string();
                    info!(
                        "Staring new executor type={} id={}",
                        executor_type, executor_id
                    );
                    let executor_dir = self.work_dir.make_executor_work_dir(executor_id)?;
                    let listen_path = executor_dir.path().join("socket");

                    // --- Start listening Unix socket for executors ----
                    let listener =
                        {
                            let backup = ::std::env::current_dir().unwrap();
                            ::std::env::set_current_dir(executor_dir.path()).unwrap();
                            let result = UnixListener::bind("socket", &self.handle);
                            ::std::env::set_current_dir(backup).unwrap();
                            result
                        }.map_err(|e| info!("Cannot create listening unix socket: {:?}", e))
                            .unwrap();

                    let program_name = &args[0];
                    let mut command = executor_command(
                        &executor_dir,
                        &listen_path,
                        &self.log_dir,
                        executor_id,
                        program_name,
                        &args[1..],
                    )?;

                    let state_ref = self.self_ref();
                    let command_future = command
                        .status_async2(&self.handle)
                        .map_err(|e| {
                            format!(
                                "Executor command '{}' failed: {:?}",
                                program_name,
                                ::std::error::Error::description(&e)
                            )
                        })?
                        .map_err(|e| {
                            format!(
                                "Executor command failed: {:?}",
                                ::std::error::Error::description(&e)
                            ).into()
                        })
                        .and_then(move |status| {
                            error!("Executor {} terminated with {}", executor_id, status);
                            let (out_log_name, err_log_name) = state_ref.get().log_dir().executor_log_paths(executor_id);
                            let logs = get_log_tails(&out_log_name, &err_log_name, 600);
                            bail!("Executor unexpectedly terminated with {}\n{}", status, logs);
                        });

                    let executor_type2 = executor_type.clone();
                    let listen_future = listener
                        .incoming()
                        .into_future()
                        .map_err(|_| "Executor connection failed".into())
                        .and_then(move |(r, _)| {
                            info!("Connection for executor id={}", executor_id);
                            let (raw_stream, _) = r.unwrap();
                            let stream = ::common::comm::create_protocol_stream(raw_stream);
                            stream
                                .into_future()
                                .map_err(|(e, _)| {
                                    format!("Executor error: Error on unregistered executor connection: {:?}", e).into()
                                })
                                .and_then(move |(r, stream)| {
                                    check_registration(r, executor_id, &executor_type2)
                                        .map(|()| stream)
                                })
                        });

                    let state_ref = self.self_ref();
                    let ready_future = listen_future
                        .select2(command_future)
                        .and_then(move |r| {
                            // TODO: replace in futures 0.2.0 by left()
                            let (stream, command_future) = match r {
                                ::futures::future::Either::A(x) => x,
                                ::futures::future::Either::B(((), _)) => unreachable!(),
                            };
                            let connection = Connection::from(stream);
                            let sender = connection.sender();
                            let executor =
                                ExecutorRef::new(executor_id, executor_type, sender, executor_dir);
                            let executor2 = executor.clone();
                            let result = executor.clone();

                            let comm_future = connection.start_future(move |data| {
                                let message: ExecutorToGovernorMessage =
                                    ::serde_cbor::from_slice(&data).unwrap();
                                match message {
                                    ExecutorToGovernorMessage::Result(msg) => {
                                        let mut sw = executor.get_mut();
                                        match sw.pick_finish_sender() {
                                        Some(sender) => { sender.send(msg).unwrap() },
                                        None => {
                                            bail!("No task is currentl running in executor, but 'result' received")
                                        }
                                    };
                                    }
                                    ExecutorToGovernorMessage::Register(_) => {
                                        bail!("Executor send 'Register' message but it is already registered");
                                    }
                                }
                                Ok(())
                            });
                            let state_ref2 = state_ref.clone();
                            let future = comm_future.select(command_future).then(move |r| {
                                match r {
                                    Ok(_) => {
                                        debug!("Executor terminating");
                                    }
                                    Err((e, _)) => error!("Executor failed: {}", e),
                                };
                                executor2.get_mut().pick_finish_sender(); // just picke sender and them it away
                                let mut state = state_ref2.get_mut();
                                state.executor_cleanup(&executor2);
                                Ok(())
                            });
                            let state = state_ref.get();
                            state.handle().spawn(future);
                            Ok(result)
                        })
                        .map_err(|e| {
                            // TODO: replace in futures 0.2.0 by into_inner()
                            e.split().0
                        });
                    Ok(Box::new(ready_future))
                } else {
                    bail!("Executor '{}' is not registered", executor_type);
                }
            }
            Some(sw) => {
                self.graph.idle_executors.remove(&sw);
                Ok(Box::new(Ok(sw).into_future()))
            }
        }
    }

    pub fn spawn_panic_on_error<F>(&self, f: F)
    where
        F: Future<Item = (), Error = Error> + 'static,
    {
        self.handle
            .spawn(f.map_err(|e| panic!("Future failed {:?}", e.description())));
    }

    pub fn add_dataobject(
        &mut self,
        id: DataObjectId,
        state: DataObjectState,
        assigned: bool,
        size: Option<usize>,
        label: String,
        data_type: DataType,
        attributes: Attributes,
    ) -> DataObjectRef {
        DataObjectRef::new(
            &mut self.graph,
            id,
            state,
            assigned,
            size,
            label,
            data_type,
            attributes,
        )
    }

    /// n_redirects is a protection against ifinite loop of redirections
    pub fn fetch_object(
        &mut self,
        governor_id: &GovernorId,
        dataobj_id: DataObjectId,
    ) -> Box<Future<Item = Data, Error = Error>> {
        let is_server = governor_id.ip().is_unspecified();
        let mut context = ::governor::rpc::fetch::FetchContext {
            state_ref: self.self_ref(),
            dataobj_id: dataobj_id,
            remote: None,
            builder: None,
            size: 0,
            offset: 0,
            n_redirects: 0,
        };
        if is_server {
            ::governor::rpc::fetch::fetch(context)
        } else {
            Box::new(
                self.wait_for_remote_governor(&governor_id)
                    .and_then(move |remote_governor| {
                        context.remote = Some(remote_governor);
                        ::governor::rpc::fetch::fetch(context)
                    }),
            )
        }
    }

    pub fn remove_object(&mut self, object: &mut DataObject) {
        debug!("Removing object {}", object.id);
        let id_list = [object.id];
        for sw in ::std::mem::replace(&mut object.executor_cache, Default::default()) {
            sw.get().send_remove_cached_objects(&id_list);
        }
        object.set_as_removed();
        self.graph.objects.remove(&object.id);
    }

    // Call when object may be waiting for delete, but now is needed again
    pub fn mark_as_needed(&mut self, object_ref: &DataObjectRef) {
        if self.graph.delete_wait_list.remove(&object_ref).is_some() {
            debug!("Object id={} is retaken from cache", object_ref.get().id);
        }
    }

    pub fn remove_dataobj_if_not_needed(&mut self, object: &mut DataObject) {
        if !object.assigned && object.consumers.is_empty() {
            debug!("Object {:?} is not needed", object);
            assert!(!object.is_removed());
            if !object.is_finished() || self.graph.delete_wait_list.len() > 100
                || self.delete_list_max_timeout == 0
            {
                // Instant deletion
                self.remove_object(object);
            } else {
                // Delayed deletion
                let now = ::std::time::Instant::now();
                let timeout =
                    now + ::std::time::Duration::from_secs(self.delete_list_max_timeout as u64);
                let object_ref = self.graph.objects.get(&object.id).unwrap().clone();
                let r = self.graph.delete_wait_list.insert(object_ref, timeout);
                assert!(r.is_none()); // it should not be in delete list
            }
        }
    }

    pub fn remove_consumer(&mut self, object: &mut DataObject, task: &TaskRef) {
        let found = object.consumers.remove(task);
        // We test "found" because of possible multiple occurence of object in inputs
        if found {
            self.remove_dataobj_if_not_needed(object);
        }
    }

    /// Remove task from graph
    pub fn unregister_task(&mut self, task_ref: &TaskRef) {
        let task = task_ref.get_mut();
        debug!("Unregistering task id = {}", task.id);

        let removed = self.graph.tasks.remove(&task.id);
        assert!(removed.is_some());

        for input in &task.inputs {
            let mut obj = input.object.get_mut();
            self.remove_consumer(&mut obj, &task_ref);
        }

        /*for output in &task.outputs {
            self.remove_dataobj_if_not_needed(&mut output.get_mut());
        }*/
    }

    /// Remove task from governor, if running it is forced to stop
    /// If task does not exists, call is silently ignored
    pub fn stop_task(&mut self, task_id: &TaskId) {
        debug!("Stopping task {}", task_id);
        if let Some(instance) = self.graph.running_tasks.get_mut(task_id) {
            instance.stop();
            return;
        }

        let task_ref = match self.graph.tasks.get(task_id) {
            Some(task_ref) => task_ref.clone(),
            None => return,
        };

        if let Some(p) = self.graph.ready_tasks.iter().position(|t| t == &task_ref) {
            self.graph.ready_tasks.remove(p);
        }
        self.unregister_task(&task_ref);
    }

    #[inline]
    pub fn task_updated(&mut self, task: &TaskRef) {
        self.updated_tasks.insert(task.clone());
    }

    pub fn alloc_resources(&mut self, resources: &Resources) {
        self.free_resources.remove(resources);
        assert!(self.free_slots > 0);
        self.free_slots -= 1;
        debug!(
            "{} cpus allocated, free now: {}",
            resources.cpus(),
            self.free_resources.cpus()
        );
    }

    pub fn free_resources(&mut self, resources: &Resources) {
        self.free_resources.add(resources);
        self.free_slots += 1;
        self.need_scheduling();
        debug!(
            "{} cpus disposed, free now: {}",
            resources.cpus(),
            self.free_resources.cpus()
        );
    }

    pub fn start_task(&mut self, task_ref: TaskRef) {
        TaskInstance::start(self, task_ref);
    }

    pub fn schedule(&mut self) {
        let mut i = 0;
        while i < self.graph.ready_tasks.len() {
            if self.free_slots == 0 {
                break;
            }
            let n_cpus = self.free_resources.cpus;
            let j = self.graph.ready_tasks[i..]
                .iter()
                .position(|task| n_cpus >= task.get().resources.cpus);
            if j.is_none() {
                break;
            }
            let j = j.unwrap();
            let task_ref = self.graph.ready_tasks.remove(i + j);
            self.start_task(task_ref.clone());
            i += j;
        }
    }

    pub fn wait_for_remote_governor(
        &mut self,
        governor_id: &GovernorId,
    ) -> Box<Future<Item = Rc<::governor_capnp::governor_bootstrap::Client>, Error = Error>> {
        if let Some(ref mut wrapper) = self.remote_governors.get_mut(governor_id) {
            return wrapper.wait();
        }

        let wrapper = AsyncInitWrapper::new();
        self.remote_governors.insert(governor_id.clone(), wrapper);

        let state = self.self_ref();
        let governor_id = governor_id.clone();

        Box::new(
            TcpStream::connect(&governor_id, &self.handle)
                .map(move |stream| {
                    debug!("Connection to governor {} established", governor_id);
                    let mut rpc_system = ::common::rpc::new_rpc_system(stream, None);
                    let bootstrap: Rc<
                        ::governor_capnp::governor_bootstrap::Client,
                    > = Rc::new(rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server));
                    let mut s = state.get_mut();
                    {
                        let wrapper = s.remote_governors.get_mut(&governor_id).unwrap();
                        wrapper.set_value(bootstrap.clone());
                    }
                    s.spawn_panic_on_error(rpc_system.map_err(|e| e.into()));
                    bootstrap
                })
                .map_err(|e| e.into()),
        )
    }

    pub fn monitor_mut(&mut self) -> &mut Monitor {
        &mut self.monitor
    }

    /// Send event to server
    pub fn send_event(&mut self, event: events::Event) {
        debug!("Sending event to server");
        let now = ::chrono::Utc::now();
        let mut req = self.upstream.as_ref().unwrap().push_events_request();
        {
            let mut req_events = req.get().init_events(1);
            let mut capnp_event = req_events.reborrow().get(0);
            capnp_event.set_event(&::serde_json::to_string(&event).unwrap());
            let mut capnp_ts = capnp_event.init_timestamp();
            capnp_ts.set_seconds(now.timestamp() as u64);
            capnp_ts.set_subsec_nanos(now.timestamp_subsec_nanos() as u32);
        }
        self.spawn_panic_on_error(req.send().promise.map(|_| ()).map_err(|e| e.into()));
    }

    #[inline]
    pub fn self_ref(&self) -> StateRef {
        self.self_ref.as_ref().unwrap().clone()
    }
}

impl StateRef {
    pub fn new(
        handle: Handle,
        work_dir: PathBuf,
        log_dir: PathBuf,
        n_cpus: u32,
        executors: HashMap<String, Vec<String>>,
    ) -> Self {
        let resources = Resources { cpus: n_cpus };

        let state = Self::wrap(State {
            handle,
            free_slots: 4 * n_cpus,
            resources: resources.clone(),
            free_resources: resources,
            upstream: None,
            remote_governors: HashMap::new(),
            updated_objects: Default::default(),
            updated_tasks: Default::default(),
            work_dir: WorkDir::new(work_dir),
            log_dir: LogDir::new(log_dir),
            governor_id: empty_governor_id(),
            graph: Graph::new(),
            need_scheduling: false,
            monitor: Monitor::new(),
            executor_args: executors,
            self_ref: None,
            delete_list_max_timeout: ::std::env::var("RAIN_DELETE_LIST_TIMEOUT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_DELETE_LIST_MAX_TIMEOUT),
            transport_views: Default::default(),
        });
        state.get_mut().self_ref = Some(state.clone());
        state
    }

    // This is called when an incoming connection arrives
    fn on_connection(&self, stream: TcpStream, address: SocketAddr) {
        // Handle an incoming connection; spawn gate object for it

        info!("New connection from {}", address);
        stream.set_nodelay(true).unwrap();

        let bootstrap = ::governor_capnp::governor_bootstrap::ToClient::new(
            ::governor::rpc::bootstrap::GovernorBootstrapImpl::new(self),
        ).from_server::<::capnp_rpc::Server>();
        let rpc_system = ::common::rpc::new_rpc_system(stream, Some(bootstrap.client));
        self.get()
            .spawn_panic_on_error(rpc_system.map_err(|e| e.into()));
    }

    // This is called when governor connection to server is established
    pub fn on_connected_to_server(
        &self,
        stream: TcpStream,
        listen_address: SocketAddr,
        ready_file: Option<String>,
    ) {
        info!("Connected to server; registering as governor");
        stream.set_nodelay(true).unwrap();
        let mut rpc_system = ::common::rpc::new_rpc_system(stream, None);
        let bootstrap: ::server_capnp::server_bootstrap::Client =
            rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

        let governor_control = ::governor_capnp::governor_control::ToClient::new(
            GovernorControlImpl::new(self),
        ).from_server::<::capnp_rpc::Server>();

        let mut req = bootstrap.register_as_governor_request();

        req.get().set_version(GOVERNOR_PROTOCOL_VERSION);
        req.get().set_control(governor_control);
        listen_address.to_capnp(&mut req.get().get_address().unwrap());
        self.get()
            .resources
            .to_capnp(&mut req.get().get_resources().unwrap());

        let state = self.clone();
        let future = req.send()
            .promise
            .and_then(move |response| {
                let response = pry!(response.get());
                let upstream = pry!(response.get_upstream());
                let governor_id = pry!(response.get_governor_id());
                let mut inner = state.get_mut();
                inner.upstream = Some(upstream);
                inner.governor_id = GovernorId::from_capnp(&governor_id);
                debug!("Registration completed");

                // Create ready file - a file that is created when governor is connected & registered
                if let Some(name) = ready_file {
                    ::common::fs::create_ready_file(Path::new(&name));
                }

                Promise::ok(())
            })
            .map_err(|e| {
                panic!("Error {}", e);
            });

        let inner = self.get();
        inner.handle.spawn(future);
        inner
            .handle
            .spawn(rpc_system.map_err(|e| error!("RPC error: {:?}", e)));
    }

    pub fn start(
        &self,
        server_address: SocketAddr,
        mut listen_address: SocketAddr,
        ready_file: Option<&str>,
    ) {
        let handle = self.get().handle.clone();

        // --- Start listening TCP/IP for governor2governor communications ----
        let listener = TcpListener::bind(&listen_address, &handle).unwrap();
        let port = listener.local_addr().unwrap().port();
        // Since listen port may be 0, we need to update the real port
        listen_address.set_port(port);
        info!("Start listening on port={}", port);

        let state = self.clone();
        let future = listener
            .incoming()
            .for_each(move |(stream, addr)| {
                state.on_connection(stream, addr);
                Ok(())
            })
            .map_err(|e| {
                panic!("Listening failed {:?}", e);
            });
        handle.spawn(future);

        // --- Start monitoring ---
        let state = self.clone();
        let now = Instant::now();

        let interval = ::tokio_timer::Interval::new(now, Duration::from_secs(MONITORING_INTERVAL));
        let monitoring = interval
            .for_each(move |_| {
                debug!("Monitoring wakeup");
                let mut s = state.get_mut();
                let governor_id = s.governor_id;

                // Check that we already know our address
                if governor_id.ip().is_unspecified() {
                    debug!("Monitoring skipped, registration is not completed yet");
                    return Ok(());
                }

                let event = s.monitor.build_event(&governor_id);
                s.send_event(event);
                Ok(())
            })
            .map_err(|e| error!("Monitoring error {}", e));
        handle.spawn(monitoring);

        // --- Start checking wait list ----
        let state = self.clone();
        let interval =
            ::tokio_timer::Interval::new(now, Duration::from_secs(DELETE_WAIT_LIST_INTERVAL));
        let check_list = interval
            .for_each(move |_| {
                debug!("Checking wait list wakeup");
                let mut s = state.get_mut();
                if s.graph.delete_wait_list.is_empty() {
                    return Ok(());
                }
                let now = ::std::time::Instant::now();
                let to_delete: Vec<_> = s.graph
                    .delete_wait_list
                    .iter()
                    .filter(|pair| pair.1 < &now)
                    .map(|pair| pair.0.clone())
                    .collect();
                for obj in to_delete {
                    {
                        let mut o = obj.get_mut();
                        s.remove_object(&mut o);
                        s.transport_views.remove(&o.id);
                    }
                    s.graph.delete_wait_list.remove(&obj);
                }

                let to_delete: Vec<DataObjectId> = s.transport_views
                    .iter()
                    .filter(|pair| (pair.1).1 < now)
                    .map(|pair| *pair.0)
                    .collect();

                for id in to_delete {
                    s.transport_views.remove(&id);
                }
                Ok(())
            })
            .map_err(|e| panic!("Error during checking wait list {}", e));
        handle.spawn(check_list);

        // --- Start connection to server ----
        let core1 = self.clone();
        let ready_file = ready_file.map(|f| f.to_string());
        info!("Connecting to server addr={}", server_address);
        let connect = TcpStream::connect(&server_address, &handle)
            .and_then(move |stream| {
                core1.on_connected_to_server(stream, listen_address, ready_file);
                Ok(())
            })
            .map_err(|e| {
                error!("Connecting to server failed: {}", e);
                exit(1);
            });
        handle.spawn(connect);
    }

    pub fn turn(&self) {
        let mut state = self.get_mut();
        if state.need_scheduling {
            state.need_scheduling = false;
            state.schedule();
        }

        // Important: Scheduler should be before update, since scheduler may produce another updates
        if !state.updated_objects.is_empty() || !state.updated_tasks.is_empty() {
            state.send_update()
        }
    }
}
