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
use common::id::{SId, SubworkerId, SessionId, WorkerId, empty_worker_id, Id, TaskId, DataObjectId};
use common::convert::{ToCapnp, FromCapnp};
use common::keeppolicy::KeepPolicy;
use common::wrapped::WrappedRcRefCell;
use common::resources::Resources;
use worker::graph::{DataObjectRef, DataObjectType, DataObjectState,
                    Graph, TaskRef, TaskInput, TaskState, SubworkerRef, start_python_subworker,
                    DataBuilder, BlobBuilder, Data};
use worker::tasks::TaskContext;
use worker::rpc::{SubworkerUpstreamImpl, WorkerControlImpl};

use futures::{Future, future};
use futures::Stream;
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

pub struct State {
    graph: Graph,

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

    /// Path to working directory
    work_dir: PathBuf,

    resources: Resources,
}

pub type StateRef = WrappedRcRefCell<State>;

impl State {

    pub fn make_subworker_id(&mut self) -> SubworkerId {
        self.graph.make_id()
    }

    #[inline]
    pub fn work_dir(&self) -> &PathBuf {
        &self.work_dir
    }

    pub fn subworker_listen_path(&self) -> PathBuf {
        self.work_dir.join(Path::new("subworkers/listen"))
    }

    pub fn subworker_log_paths(&self, id: Id) -> (PathBuf, PathBuf) {
        let out = self.work_dir.join(Path::new(&format!("subworkers/logs/subworker-{}.out",
                                                          id)));
        let err = self.work_dir.join(Path::new(&format!("subworkers/logs/subworker-{}.err",
                                                          id)));
        (out, err)
    }

    pub fn temp_dir_for_task(&self, task_id: TaskId) -> Result<::tempdir::TempDir> {
        ::tempdir::TempDir::new_in(self.work_dir.join("tasks"),
                                   &format!("{}-{}", task_id.get_id(), task_id.get_session_id()))
            .map_err(|e| e.into())
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

    pub fn add_task(&mut self,
                    id: TaskId,
                    inputs: Vec<TaskInput>,
                    outputs: Vec<DataObjectRef>,
                    procedure_key: String,
                    procedure_config: Vec<u8>) -> TaskRef {
        let task = TaskRef::new(&mut self.graph,
                                id,
                                inputs,
                                outputs,
                                procedure_key,
                                procedure_config);
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

    pub fn object_is_finished(&mut self, dataobj: &DataObjectRef, data: Arc<Data>) {
        let mut dataobject = dataobj.get_mut();
        debug!("Object id={} is finished", dataobject.id);

        dataobject.set_data(data);
        self.updated_objects.insert(dataobj.clone());

        let mut new_ready = false;
        for task in ::std::mem::replace(&mut dataobject.consumers, Default::default()) {
            if task.get_mut().input_finished(dataobj) {
                self.graph.ready_tasks.push(task);
                new_ready = true;
            }
        }

        if new_ready {
            self.need_scheduling();
        }

        // TODO inform server
    }

    /// Send status of updated elements (updated_tasks/updated_objects) and then clear this sets
    pub fn send_update(&mut self) {

        debug!("Sending update objs={}, tasks={}",
               self.updated_objects.len(), self.updated_tasks.len());

        let mut req = self.upstream.as_ref().unwrap().update_states_request();

        {   // Data Objects
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

        {   // Tasks
            let req_update = req.get().get_update().unwrap();
            let mut req_tasks = req_update.init_tasks(self.updated_tasks.len() as u32);

            for (i, task) in self.updated_tasks.iter().enumerate() {
                let mut ct = req_tasks.borrow().get(i as u32);
                let task = task.get();
                ct.set_state(match task.state {
                    TaskState::Running => ::common_capnp::TaskState::Running,
                    TaskState::Finished => ::common_capnp::TaskState::Finished,
                    _ => panic!("Invalid state")
                });
                task.id.to_capnp(&mut ct.get_id().unwrap());
            }

            self.updated_tasks.clear();
        }

        self.spawn_panic_on_error(req.send().promise.map(|_| ()));
    }

    pub fn add_subworker(&mut self, subworker: SubworkerRef) {
        info!("Subworker registered subworker_id={}", subworker.id());
        let subworker_id = subworker.id();
        self.graph.subworkers.insert(subworker_id, subworker);
        // TODO: Someone probably started subworker and he wants to be notified
    }

    /// You can call this ONLY when wait_for_datastore was success full
    pub fn get_datastore(&self, worker_id: &WorkerId) ->  &::datastore_capnp::data_store::Client {
        self.datastores.get(worker_id).unwrap().get()
    }

    pub fn fetch_dataobject(&self, dataobj: &DataObjectRef) -> Box<Future<Item=(), Error=Error>> {
        /*let worker_id = dataobj.remote().unwrap();

        let builder = Box::new(BlobBuilder::new());

        if worker_id.ip().is_unspecified() {
            // Fetch from server
            self.fetch_from_datastore(dataobj,
                                      &self.datastore.as_ref().unwrap(),
                                      builder).and_then(|data| {

            })
        } else {
            // TODO FETCH FROM WORKER
            unimplemented!();
        }*/
        unimplemented!()
    }

    pub fn spawn_panic_on_error<F, E>(&self, f: F)
        where F: Future<Item = (), Error = E> + 'static, E : ::std::fmt::Debug
    {
        self.handle.spawn(f.map_err(|e| panic!("Future failed {:?}", e)));
    }

    pub fn add_dataobject(&mut self,
                          id: DataObjectId,
                          state: DataObjectState,
                          obj_type: DataObjectType,
                          keep: KeepPolicy,
                          size: Option<usize>,
                          label: String) -> DataObjectRef {
        let dataobj = DataObjectRef::new(&mut self.graph, id, state, obj_type, keep, size, label);
        /*if dataobj.remote().is_some() {
            self.fetch_dataobject(&dataobj)
        }*/
        dataobj
    }

    pub fn start_task(&mut self, task: TaskRef, state_ref: &StateRef) {
        {
            let mut t = task.get_mut();
            t.state = TaskState::Running;
            debug!("Task id={} started", t.id);
        }

        self.updated_tasks.insert(task.clone());

        let future = TaskContext::new(task, state_ref.clone()).start(self).unwrap();

        let state_ref = state_ref.clone();
        self.handle.spawn(future.and_then(move |context| {
            let mut task = context.task.get_mut();
            task.state = TaskState::Finished;
            debug!("Task id={} finished", task.id);

            state_ref.get_mut().updated_tasks.insert(context.task.clone());

            for input in &task.inputs {
                if (!input.object.get().is_finished()) {
                    bail!("Not all inputs produced");
                }
            }
            Ok(())
        }).map_err(|e| {
            // TODO: Handle error properly
            panic!("Task failed: {:?}", e);
        }));
    }

    pub fn schedule(&mut self, state_ref: &StateRef) {
        // TODO: Some serious scheduling
        if let Some(task) = self.graph.ready_tasks.pop() {
            self.start_task(task, state_ref);
        }
    }

    pub fn wait_for_datastore(&mut self, state: &StateRef, worker_id: &WorkerId) -> Box<Future<Item=(), Error=Error>> {
        if let Some(ref mut wrapper) = self.datastores.get_mut(worker_id) {
            return wrapper.wait();
        }

        let mut wrapper = AsyncInitWrapper::new();
        self.datastores.insert(worker_id.clone(), wrapper);

        let state = state.clone();
        let worker_id = worker_id.clone();

        if worker_id.ip().is_unspecified() {
            // Data are on server
            let req = self.upstream.as_ref().unwrap().get_data_store_request();
            Box::new(req.send().promise.map(move |r| {
                let datastore = r.get().unwrap().get_store().unwrap();
                let mut inner = state.get_mut();
                let mut wrapper = inner.datastores.get_mut(&worker_id).unwrap();
                wrapper.set_value(datastore);
            }).map_err(|e| e.into()))
        } else {
            // Data are on workers
            unimplemented!();
        }
    }
}

impl StateRef {
    pub fn new(handle: Handle, work_dir: PathBuf, n_cpus: u32) -> Self {
        Self::wrap(State {
                       handle,
                       resources: Resources {n_cpus},
                       upstream: None,
                       datastores: HashMap::new(),
                       updated_objects: Default::default(),
                       updated_tasks: Default::default(),
                       timer: tokio_timer::wheel()
                           .tick_duration(Duration::from_millis(100))
                           .num_slots(256)
                           .build(),
                       work_dir,
                       worker_id: empty_worker_id(),
                       graph: Graph::new(),
                       need_scheduling: false,
                   })
    }

    // This is called when an incoming connection arrives
    fn on_connection(&self, stream: TcpStream, address: SocketAddr) {
        // Handle an incoming connection; spawn gate object for it

        info!("New connection from {}", address);
        stream.set_nodelay(true).unwrap();

        let bootstrap = ::datastore_capnp::data_store::ToClient::new(
            ::worker::rpc::datastore::DataStoreImpl::new(self)
        ).from_server::<::capnp_rpc::Server>();
        let rpc_system = ::common::rpc::new_rpc_system(stream, Some(bootstrap.client));
        self.get().spawn_panic_on_error(rpc_system);
    }

    // This is called when worker connection to server is established
    pub fn on_connected_to_server(&self,
                                  stream: TcpStream,
                                  listen_address: SocketAddr,
                                  ready_file: Option<String>) {
        info!("Connected to server; registering as worker");
        stream.set_nodelay(true).unwrap();
        let mut rpc_system = ::common::rpc::new_rpc_system(stream, None);
        let bootstrap: ::server_capnp::server_bootstrap::Client =
            rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

        let worker_control =
            ::worker_capnp::worker_control::ToClient::new(WorkerControlImpl::new(self))
                .from_server::<::capnp_rpc::Server>();

        let mut req = bootstrap.register_as_worker_request();

        req.get().set_version(WORKER_PROTOCOL_VERSION);
        req.get().set_control(worker_control);
        listen_address.to_capnp(&mut req.get().get_address().unwrap());

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
        inner
            .handle
            .spawn(rpc_system.map_err(|e| error!("RPC error: {:?}", e)));
    }

    pub fn on_subworker_connection(&self, stream: UnixStream) {
        info!("New subworker connected");
        let upstream =
            ::subworker_capnp::subworker_upstream::ToClient::new(SubworkerUpstreamImpl::new(self))
                .from_server::<::capnp_rpc::Server>();
        let rpc_system = ::common::rpc::new_rpc_system(stream, Some(upstream.client));
        let inner = self.get();
        inner
            .handle
            .spawn(rpc_system.map_err(|e| error!("RPC error: {:?}", e)));
    }


    pub fn start(&self,
                 server_address: SocketAddr,
                 mut listen_address: SocketAddr,
                 ready_file: Option<&str>) {
        let handle = self.get().handle.clone();

        // --- Create workdir layout ---
        {
            let state = self.get();
            ::std::fs::create_dir(state.work_dir.join("data")).unwrap();
            ::std::fs::create_dir(state.work_dir.join("tasks")).unwrap();
            ::std::fs::create_dir(state.work_dir.join("subworkers")).unwrap();
            ::std::fs::create_dir(state.work_dir.join("subworkers/logs")).unwrap();
        }

        // --- Start listening Unix socket for subworkers ----
        let listener = UnixListener::bind(self.get().subworker_listen_path(), &handle)
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
