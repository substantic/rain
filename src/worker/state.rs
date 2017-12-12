use std::rc::Rc;
use std::sync::Arc;
use std::cell::RefCell;
use std::net::SocketAddr;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::process::exit;
use std::path::{Path, PathBuf};
use std::io::Write;
use std::time::Duration;
use std::iter::FromIterator;
use std::collections::HashMap;

use common::asycinit::AsyncInitWrapper;
use common::RcSet;
use common::id::{SId, SubworkerId, WorkerId, empty_worker_id, TaskId, DataObjectId};
use common::convert::{ToCapnp, FromCapnp};
use common::keeppolicy::KeepPolicy;
use common::wrapped::WrappedRcRefCell;
use common::resources::Resources;
use common::monitor::{Monitor, Frame};

use worker::graph::{DataObjectRef, DataObjectType, DataObjectState, DataObject, Graph, TaskRef,
                    TaskInput, TaskState, SubworkerRef, subworker_command};
use worker::data::{DataBuilder};
use worker::tasks::TaskInstance;
use worker::rpc::{SubworkerUpstreamImpl, WorkerControlImpl};
use worker::fs::workdir::WorkDir;

use futures::{Future, future};
use futures::Stream;
use futures::IntoFuture;
use tokio_core::reactor::Handle;
use tokio_core::net::TcpListener;
use tokio_core::net::TcpStream;
use tokio_io::AsyncRead;
use tokio_timer;
use tokio_uds::{UnixListener, UnixStream};
use capnp_rpc::{RpcSystem, twoparty, rpc_twoparty_capnp};
use capnp::capability::Promise;
use errors::{Error, Result};

use WORKER_PROTOCOL_VERSION;

const MONITORING_INTERVAL:u64 = 5; // Monitoring interval in seconds

pub struct State {
    pub(super) graph: Graph,

    /// If true, next "turn" the scheduler is executed
    need_scheduling: bool,

    /// Tokio core handle
    handle: Handle,

    /// Handle to WorkerUpstream (that resides in server)
    upstream: Option<::worker_capnp::worker_upstream::Client>,

    /// Handle to DataStore (that resides in server)
    datastores: HashMap<WorkerId, AsyncInitWrapper<::datastore_capnp::data_store::Client>>,

    updated_objects: RcSet<DataObjectRef>,
    updated_tasks: RcSet<TaskRef>,

    /// A worker assigned to this worker
    worker_id: WorkerId,

    timer: tokio_timer::Timer,

    /// This is hard limit for number of simultaneously executed tasks
    /// The purpose is to limit task with empty resources
    /// The initial value is 4 * n_cpus
    free_slots: u32,

    resources: Resources,

    free_resources: Resources,

    /// Path to working directory
    work_dir: WorkDir,

    monitor: Monitor,

    /// Listing of subworkers that were started as process, but not registered
    /// The second member of triplet is subworker_type
    /// Third member (oneshot) is fired when registration is completed
    initializing_subworkers: Vec<
        (SubworkerId,
         String, // type (e.g. "py")
         ::tempdir::TempDir, // working dir
         ::futures::unsync::oneshot::Sender<SubworkerRef>, // when finished
         ::futures::unsync::oneshot::Sender<()>), // kill switch of worker
    >,

    // Map from name of subworkers to its arguments
    // e.g. "py" => ["python", "-m", "rain.subworker"]
    subworker_args: HashMap<String, Vec<String>>,

    self_ref: Option<StateRef>,
}

pub type StateRef = WrappedRcRefCell<State>;

impl State {
    #[inline]
    pub fn work_dir(&self) -> &WorkDir {
        &self.work_dir
    }

    #[inline]
    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    #[inline]
    pub fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    #[inline]
    pub fn timer(&self) -> &tokio_timer::Timer {
        &self.timer
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

    pub fn add_task(
        &mut self,
        id: TaskId,
        inputs: Vec<TaskInput>,
        outputs: Vec<DataObjectRef>,
        resources: Resources,
        procedure_key: String,
        procedure_config: Vec<u8>,
    ) -> TaskRef {
        let task = TaskRef::new(
            &mut self.graph,
            id,
            inputs,
            outputs,
            resources,
            procedure_key,
            procedure_config,
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
        let dataobject = dataobj.get_mut();
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

        self.remove_dataobj_if_not_needed(&dataobject);
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
                let mut co = req_objs.borrow().get(i as u32);
                let object = object.get();

                if object.is_finished() {
                    co.set_state(::common_capnp::DataObjectState::Finished);
                    co.set_size(object.data().size() as u64);
                } else {
                    // TODO: Handle failure state
                    panic!("Updating non finished object");
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
                let mut ct = req_tasks.borrow().get(i as u32);
                let mut task = task.get_mut();

                ct.set_state(match task.state {
                    TaskState::Running => ::common_capnp::TaskState::Running,
                    TaskState::Finished => ::common_capnp::TaskState::Finished,
                    TaskState::Failed => ::common_capnp::TaskState::Failed,
                    _ => panic!("Invalid state"),
                });

                if !task.new_additionals.is_empty() {
                    task.new_additionals.to_capnp(&mut ct.borrow()
                        .get_additionals()
                        .unwrap());
                    task.new_additionals.clear();
                }
                task.id.to_capnp(&mut ct.get_id().unwrap());
            }

            self.updated_tasks.clear();
        }

        self.spawn_panic_on_error(req.send().promise
                                  .map(|_| ())
                                  .map_err(|e| e.into()));
    }

    fn register_subworker(&mut self, name: &str, args: &[&str]) {
        self.subworker_args.insert(
            name.into(),
            args.iter()
                .map(|i| i.to_string())
                .collect(),
        );
    }

    pub fn get_subworker(
        &mut self,
        subworker_type: &str,
    ) -> Result<Box<Future<Item = SubworkerRef, Error = Error>>> {
        use tokio_process::CommandExt;
        let index = self.graph.idle_subworkers.iter().position(|sw| {
            sw.get().subworker_type() == subworker_type
        });
        match index {
            None => {
                let subworker_id = self.graph.make_id();
                if let Some(args) = self.subworker_args.get(subworker_type) {
                    let (ready_sender, ready_receiver) = ::futures::unsync::oneshot::channel();
                    let (kill_sender, kill_receiver) = ::futures::unsync::oneshot::channel();
                    let program_name = &args[0];
                    let (mut command, subworker_dir) = subworker_command(
                        &self.work_dir,
                        subworker_id,
                        subworker_type,
                        program_name,
                        &args[1..],
                    )?;

                    self.initializing_subworkers.push((
                        subworker_id,
                        subworker_type.to_string(),
                        subworker_dir,
                        ready_sender,
                        kill_sender,
                    ));

                    let command_future = command.status_async2(&self.handle)?.and_then(move |status| {
                        error!(
                            "Subworker {} terminated with exit code: {}",
                            subworker_id,
                            status
                        );
                        panic!("Subworker terminated; TODO handle this situation");
                        Ok(())
                    }).map_err(|e| e.into());

                    // We do not care how kill switch of activated, so receiving () or CancelError is ok
                    let kill_switch = kill_receiver.then(|_| Ok(()));

                    self.spawn_panic_on_error(command_future.select(kill_switch)
                                              .map_err(|(e, _)| e)
                                              .map(|_| ()));

                    Ok(Box::new(
                        ready_receiver.map_err(|_| "Subwork start cancelled".into()),
                    ))
                } else {
                    bail!("Unknown subworker")
                }
            }
            Some(i) => {
                let sw = self.graph.idle_subworkers.remove(i);
                Ok(Box::new(Ok(sw).into_future()))
            }
        }
    }

    /// This method is called when subworker is connected & registered
    pub fn add_subworker(
        &mut self,
        subworker_id: SubworkerId,
        subworker_type: String,
        control: ::subworker_capnp::subworker_control::Client,
    ) -> Result<()> {
        let index = self.initializing_subworkers
            .iter()
            .position(|&(id, _, _, _, _)| id == subworker_id)
            .ok_or("Subworker registered under unexpected id")?;

        info!("Subworker registered (subworker_id={})", subworker_id);

        let (sw, sw_type, work_dir, ready_sender, kill_sender) = self.initializing_subworkers.remove(index);

        if sw_type != subworker_type {
            bail!("Unexpected type of worker registered");
        }

        let subworker = SubworkerRef::new(subworker_id, subworker_type, control, work_dir, kill_sender);

        let r = self.graph.subworkers.insert(
            subworker_id,
            subworker.clone(),
        );
        assert!(r.is_none());

        if let Err(subworker) = ready_sender.send(subworker) {
            debug!("Failed to inform about new subworker");
            self.graph.idle_subworkers.push(subworker);
        }
        Ok(())
    }

    /// You can call this ONLY when wait_for_datastore was successfully finished
    pub fn get_datastore(&self, worker_id: &WorkerId) -> &::datastore_capnp::data_store::Client {
        self.datastores.get(worker_id).unwrap().get()
    }

    pub fn spawn_panic_on_error<F>(&self, f: F)
    where
        F: Future<Item = (), Error = Error> + 'static
    {
        self.handle.spawn(
            f.map_err(|e| panic!("Future failed {:?}", e.description())),
        );
    }

    pub fn add_dataobject(
        &mut self,
        id: DataObjectId,
        state: DataObjectState,
        obj_type: DataObjectType,
        assigned: bool,
        size: Option<usize>,
        label: String,
    ) -> DataObjectRef {
        let dataobj =
            DataObjectRef::new(&mut self.graph, id, state, obj_type, assigned, size, label);
        /*if dataobj.remote().is_some() {
            self.fetch_dataobject(&dataobj)
        }*/
        dataobj
    }

    pub fn remove_dataobj_if_not_needed(&mut self, object: &DataObject) {
        if !object.assigned && object.consumers.is_empty() {
            self.graph.objects.remove(&object.id);
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

        for output in &task.outputs {
            self.remove_dataobj_if_not_needed(&output.get());
        }
    }

    /// Remove task from worker, if running it is forced to stop
    /// If task does not exists, call is silently ignored
    pub fn stop_task(&mut self, task_id: &TaskId) {
        debug!("Stopping task {}", task_id);
        if let Some(instance) = self.graph.running_tasks.get_mut(task_id) {
            instance.stop();
            return
        }

        let task_ref = match self.graph.tasks.get(task_id) {
            Some(task_ref) => task_ref.clone(),
            None => return
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
    }

    pub fn free_resources(&mut self, resources: &Resources) {
        self.free_resources.add(resources);
        self.free_slots += 1;
        self.need_scheduling();
    }

    pub fn start_task(&mut self, task_ref: TaskRef) {
        TaskInstance::start(self, task_ref);
    }

    pub fn schedule(&mut self, state_ref: &StateRef) {
        let mut i = 0;
        while i < self.graph.ready_tasks.len() {
            if self.free_slots == 0 {
                    break;
            }
            let n_cpus = self.free_resources.n_cpus;
            let j = self.graph.ready_tasks[i..].iter().position(|task| {
                n_cpus >= task.get().resources.n_cpus
            });

            if j.is_none() {
                break;
            }
            let j = j.unwrap();
            let task_ref = self.graph.ready_tasks.remove(i + j);
            self.start_task(task_ref.clone());
            i += j + 1;
        }
    }

    pub fn wait_for_datastore(
        &mut self,
        state: &StateRef,
        worker_id: &WorkerId,
    ) -> Box<Future<Item = (), Error = Error>> {
        if let Some(ref mut wrapper) = self.datastores.get_mut(worker_id) {
            return wrapper.wait();
        }

        let wrapper = AsyncInitWrapper::new();
        self.datastores.insert(worker_id.clone(), wrapper);

        let state = state.clone();
        let worker_id = worker_id.clone();

        if worker_id.ip().is_unspecified() {
            // Data are on server
            let req = self.upstream.as_ref().unwrap().get_data_store_request();
            Box::new(
                req.send()
                    .promise
                    .map(move |r| {
                        debug!("Obtained datastore from server");
                        let datastore = r.get().unwrap().get_store().unwrap();
                        let mut inner = state.get_mut();
                        let wrapper = inner.datastores.get_mut(&worker_id).unwrap();
                        wrapper.set_value(datastore);
                    })
                    .map_err(|e| e.into()),
            )
        } else {
            Box::new(TcpStream::connect(&worker_id, &self.handle)
                         .map(move |stream| {
                            debug!("Connection to worker {} established", worker_id);
                            let mut rpc_system = ::common::rpc::new_rpc_system(stream, None);
                            let datastore: ::datastore_capnp::data_store::Client = rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
                            let mut s = state.get_mut();
                            {
                                 let wrapper = s.datastores.get_mut(&worker_id).unwrap();
                                 wrapper.set_value(datastore);
                            }
                            s.spawn_panic_on_error(rpc_system.map_err(|e| e.into()));
                        }).map_err(|e| e.into()))
        }
    }

    pub fn monitor_mut(&mut self) -> &mut Monitor {
        &mut self.monitor
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
        n_cpus: u32,
        subworkers: HashMap<String, Vec<String>>,
    ) -> Self {
        let resources = Resources { n_cpus };

        let state = Self::wrap(State {
            handle,
            free_slots: 4 * n_cpus,
            resources: resources.clone(),
            free_resources: resources,
            upstream: None,
            datastores: HashMap::new(),
            updated_objects: Default::default(),
            updated_tasks: Default::default(),
            timer: tokio_timer::wheel()
                .tick_duration(Duration::from_millis(100))
                .num_slots(256)
                .build(),
            work_dir: WorkDir::new(work_dir),
            worker_id: empty_worker_id(),
            graph: Graph::new(),
            need_scheduling: false,
            monitor: Monitor::new(),
            initializing_subworkers: Vec::new(),
            subworker_args: subworkers,
            self_ref: None,
        });
        state.get_mut().self_ref = Some(state.clone());
        state
    }

    // This is called when an incoming connection arrives
    fn on_connection(&self, stream: TcpStream, address: SocketAddr) {
        // Handle an incoming connection; spawn gate object for it

        info!("New connection from {}", address);
        stream.set_nodelay(true).unwrap();

        let bootstrap = ::datastore_capnp::data_store::ToClient::new(
            ::worker::rpc::datastore::DataStoreImpl::new(self),
        ).from_server::<::capnp_rpc::Server>();
        let rpc_system = ::common::rpc::new_rpc_system(stream, Some(bootstrap.client));
        self.get().spawn_panic_on_error(rpc_system.map_err(|e| e.into()));
    }

    // This is called when worker connection to server is established
    pub fn on_connected_to_server(
        &self,
        stream: TcpStream,
        listen_address: SocketAddr,
        ready_file: Option<String>,
    ) {
        info!("Connected to server; registering as worker");
        stream.set_nodelay(true).unwrap();
        let mut rpc_system = ::common::rpc::new_rpc_system(stream, None);
        let bootstrap: ::server_capnp::server_bootstrap::Client =
            rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

        let worker_control = ::worker_capnp::worker_control::ToClient::new(
            WorkerControlImpl::new(self),
        ).from_server::<::capnp_rpc::Server>();

        let mut req = bootstrap.register_as_worker_request();

        req.get().set_version(WORKER_PROTOCOL_VERSION);
        req.get().set_control(worker_control);
        listen_address.to_capnp(&mut req.get().get_address().unwrap());
        self.get().resources.to_capnp(
            &mut req.get().get_resources().unwrap(),
        );

        let state = self.clone();
        let future = req.send()
            .promise
            .and_then(move |response| {
                let response = pry!(response.get());
                let upstream = pry!(response.get_upstream());
                let worker_id = pry!(response.get_worker_id());
                let mut inner = state.get_mut();
                inner.upstream = Some(upstream);
                inner.worker_id = WorkerId::from_capnp(&worker_id);
                debug!("Registration completed");

                // Create ready file - a file that is created when worker is connected & registered
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
        inner.handle.spawn(rpc_system.map_err(
            |e| error!("RPC error: {:?}", e),
        ));
    }

    pub fn on_subworker_connection(&self, stream: UnixStream) {
        info!("New subworker connected");
        let upstream = ::subworker_capnp::subworker_upstream::ToClient::new(
            SubworkerUpstreamImpl::new(self),
        ).from_server::<::capnp_rpc::Server>();
        let rpc_system = ::common::rpc::new_rpc_system(stream, Some(upstream.client));
        let inner = self.get();
        inner.handle.spawn(rpc_system.map_err(
            |e| error!("RPC error: {:?}", e),
        ));
    }


    pub fn start(
        &self,
        server_address: SocketAddr,
        mut listen_address: SocketAddr,
        ready_file: Option<&str>,
    ) {
        let handle = self.get().handle.clone();

        // --- Create workdir layout ---
        {
            let state = self.get();
        }

        // --- Start listening Unix socket for subworkers ----
        let listener = UnixListener::bind(self.get().work_dir().subworker_listen_path(), &handle)
            .expect("Cannot initialize unix socket for subworkers");
        let state = self.clone();
        let future = listener
            .incoming()
            .for_each(move |(stream, addr)| {
                state.on_subworker_connection(stream);
                Ok(())
            })
            .map_err(|e| {
                panic!("Subworker listening failed {:?}", e);
            });
        handle.spawn(future);

        // -- Start python subworker (FOR TESTING PURPOSE)
        //start_python_subworker(self);

        // --- Start listening TCP/IP for worker2worker communications ----
        let listener = TcpListener::bind(&listen_address, &handle).unwrap();
        let port = listener.local_addr().unwrap().port();
        listen_address.set_port(port); // Since listen port may be 0, we need to update the real port
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
        let timer = state.get().timer.clone();
        let interval = timer.interval(Duration::from_secs(MONITORING_INTERVAL));

        let monitoring = interval
            .for_each(move |()| {
                state.get_mut().monitor.collect_samples();
                Ok(())
            })
            .map_err(|e| {
                error!("Monitoring error {}", e)
            });
        handle.spawn(monitoring);

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
            state.schedule(self);
        }

        // Important: Scheduler should be before update, since scheduler may produce another updates
        if !state.updated_objects.is_empty() || !state.updated_tasks.is_empty() {
            state.send_update()
        }
    }
}
