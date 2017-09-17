use std::net::{SocketAddr};
use std::collections::HashMap;


use futures::{Future, Stream};
use tokio_core::reactor::Handle;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_io::AsyncRead;
use capnp_rpc::{RpcSystem, twoparty, rpc_twoparty_capnp};

use errors::Result;
use common::id::{SessionId, WorkerId, DataObjectId, TaskId, ClientId};
use common::rpc::new_rpc_system;
use server::graph::{Graph, WorkerRef, DataObjectRef, TaskRef, SessionRef,
                    ClientRef, DataObjectState, DataObjectType, TaskState};
use server::rpc::ServerBootstrapImpl;
use common::convert::ToCapnp;
use common::wrapped::WrappedRcRefCell;
use common::keeppolicy::KeepPolicy;
use common::resources::Resources;
use common::Additional;

pub struct State {
    // Contained objects
    pub(super) graph: Graph,

    /// If true, next "turn" the scheduler is executed
    need_scheduling: bool,

    /// Listening port and address.
    listen_address: SocketAddr,

    /// Tokio core handle.
    handle: Handle,

}

impl State {
    pub fn add_worker(&mut self,
                      address: SocketAddr,
                      control: Option<::worker_capnp::worker_control::Client>,
                      resources: Resources) -> Result<WorkerRef> {
        WorkerRef::new(&mut self.graph, address, control, resources)
    }

    pub fn remove_worker(&mut self, worker: &WorkerRef) -> Result<()> {
        unimplemented!()
    }

    pub fn add_client(&mut self, address: SocketAddr) -> Result<ClientRef> {
        ClientRef::new(&mut self.graph, address)
    }

    pub fn remove_client(&mut self, client: &ClientRef)  -> Result<()> {
        unimplemented!()
    }

    pub fn add_session(&mut self, client: &ClientRef) -> Result<SessionRef> {
        SessionRef::new(&mut self.graph, client)
    }

    pub fn remove_session(&mut self, session: &SessionRef)  -> Result<()> {
        unimplemented!()
    }

    pub fn add_object(&mut self,
               session: &SessionRef,
               id: DataObjectId,
               object_type: DataObjectType,
               keep: KeepPolicy,
               label: String,
               data: Option<Vec<u8>>,
               additional: Additional) -> Result<DataObjectRef> {
        DataObjectRef::new(&mut self.graph, session, id, object_type, keep,
                           label, data, additional)
    }

    pub fn remove_object(&mut self, object: &DataObjectRef) -> Result<()> {
        unimplemented!()
    }

    pub fn unkeep_object(&mut self, object: &DataObjectRef) -> Result<()> { unimplemented!()
    }

    pub fn add_task(&mut self, session: &SessionRef, id: TaskId /* TODO: more */) -> TaskRef {
        unimplemented!()
    }

    pub fn remove_task(&mut self, task: &TaskRef) -> Result<()> {
        unimplemented!()
    }

    pub fn worker_by_id(&self, id: WorkerId) -> Result<WorkerRef> {
        match self.graph.workers.get(&id) {
            Some(w) => Ok(w.clone()),
            None => Err(format!("Worker {:?} not found", id))?,
        }
    }

    pub fn client_by_id(&self, id: ClientId) -> Result<ClientRef> {
        match self.graph.clients.get(&id) {
            Some(c) => Ok(c.clone()),
            None => Err(format!("Client {:?} not found", id))?,
        }
    }

    pub fn session_by_id(&self, id: SessionId) -> Result<SessionRef> {
        match self.graph.sessions.get(&id) {
            Some(s) => Ok(s.clone()),
            None => Err(format!("Session {:?} not found", id))?,
        }
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

    pub fn verify_submit(&mut self, tasks: &[TaskRef], objects: &[DataObjectRef]) -> Result<()> {
        for oref in objects.iter() {
            let o = oref.get();
            if o.producer.is_some() && o.data.is_some() {
                bail!("Object {} submitted with both producer task {} and data of size {}",
                    o.id, o.producer.as_ref().unwrap().get_id(),
                    o.data.as_ref().unwrap().len());
            }
            if o.producer.is_none() && o.data.is_none() {
                bail!("Object {} submitted with neither producer nor data.", o.id);
            }
        }
        self.check_consistency_opt()?;
        Ok(())
    }

    /// Optionally call `check_consistency` depending on global `DEBUG_CHECK_CONSISTENCY`.
    pub fn check_consistency_opt(&mut self) -> Result<()> {
        if ::DEBUG_CHECK_CONSISTENCY.load(::std::sync::atomic::Ordering::Relaxed) {
            self.check_consistency()?;
        }
        Ok(())
    }

    /// Check consistency of all tasks, objects, workers, clients and sessions.
    pub fn check_consistency(&mut self)  -> Result<()> {
        for tr in self.graph.tasks.values() {
            tr.check_consistency()?;
        }
        for or in self.graph.objects.values() {
            or.check_consistency()?;
        }
        for wr in self.graph.workers.values() {
            wr.check_consistency()?;
        }
        for sr in self.graph.sessions.values() {
            sr.check_consistency()?;
        }
        for cr in self.graph.clients.values() {
            cr.check_consistency()?;
        }
        Ok(())

    }

    pub fn add_task_to_worker(&self, task: &TaskRef) {
        let mut t = task.get_mut();
        assert!(t.scheduled.is_some());
        assert!(t.assigned.is_none());

        // Collect objects

        // objects is vector of pairs (object, worker_id) where worker_id is placement
        // of object data object
        let mut objects: Vec<(DataObjectRef, WorkerId)> = Vec::new();
        let worker_ref = t.scheduled.as_ref().unwrap().clone();
        t.assigned = Some(worker_ref.clone());
        let worker = worker_ref.get();
        let worker_id = worker.id().clone();
        let empty_worker_id = ::common::id::empty_worker_id();

        for input in &t.inputs {
            let mut o = input.object.get_mut();
            if !o.assigned.contains(&worker_ref) {

                // Just take first placement
                let placement = o.located.iter().next()
                    .map(|w| w.get().id().clone())
                    .unwrap_or_else(|| {
                        // If there is no placement, then server is the source of datobject
                        assert!(o.data.is_some());
                        empty_worker_id.clone()
                    });
                objects.push((input.object.clone(), placement));
                o.assigned.insert(worker_ref.clone());
            }
        }
        for output in &t.outputs {
            objects.push((output.clone(), worker_id.clone()));
            output.get_mut().assigned.insert(worker_ref.clone());
        }

        debug!("Assiging task id={} to worker={}", t.id, worker.id());

        // Create request

        let mut req = worker.control.as_ref().unwrap().add_nodes_request();

        // Serialize objects
        {
            let mut new_objects = req.get().init_new_objects(objects.len() as u32);
            for (i, &(ref object, placement)) in objects.iter().enumerate() {
                let mut co = &mut new_objects.borrow().get(i as u32);
                placement.to_capnp(&mut co.borrow().get_placement().unwrap());
                let obj = object.get();
                obj.to_worker_capnp(&mut co);
                // TODO: Additionals
                // TODO: Object state (or remove it)
            }
        }

        // Serialize tasks
        {
            let mut new_tasks = req.get().init_new_tasks(1);
            t.to_worker_capnp(&mut new_tasks.get(0));
        }

        self.handle.spawn(req
            .send().promise
            .map(|_| ())
            .map_err(|e| panic!("Send failed {:?}", e)));
    }

    pub fn update_states(&mut self,
                         worker: &WorkerRef,
                         obj_updates: &[(DataObjectRef, DataObjectState, usize)],
                         task_updates: &[(TaskRef, TaskState)]) {
        debug!("Update states objs: {}, tasks: {}", obj_updates.len(), task_updates.len());
        for &(ref task, state) in task_updates {
            task.get_mut().set_state(state);
        }


        for &(ref obj, state, size) in obj_updates {
            obj.get_mut().set_state(worker, state, Some(size));
        }

        self.need_scheduling();
    }

    #[inline]
    pub fn need_scheduling(&mut self) {
        self.need_scheduling = true;
    }

    pub fn run_scheduler(&mut self) {
        debug!("Running scheduler");

        // Scheduler
        let workers: Vec<_> = self.graph.workers.values().collect();
        for (task, w) in self.graph.new_tasks.iter().zip(workers) {
            let mut t = task.get_mut();
            t.scheduled = Some(w.clone());
            if t.inputs.iter().all(|i| i.object.get().data.is_some()) {
                self.graph.ready_tasks.push(task.clone());
            }
        }
        self.graph.new_tasks.clear();

        // Reactor
        for task in &self.graph.ready_tasks {
            self.add_task_to_worker(&task);
        }
        self.graph.ready_tasks.clear();
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

/// Note: No `Drop` impl as a `State` is assumed to live forever.
pub type StateRef = WrappedRcRefCell<State>;

impl StateRef {
    pub fn new(handle: Handle, listen_address: SocketAddr) -> Self {
        Self::wrap(State {
            graph: Default::default(),
            need_scheduling: false,
            listen_address: listen_address,
            handle: handle,
        })
    }


    // TODO: Functional cleanup of code below after structures specification


    pub fn start(&self) {
        let listen_address = self.get().listen_address;
        let handle = self.get().handle.clone();
        let listener = TcpListener::bind(&listen_address, &handle).unwrap();

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
        info!("Start listening on address={}", listen_address);
    }

    pub fn turn(&self) {
        let mut state = self.get_mut();
        if state.need_scheduling {
            state.need_scheduling = false;
            state.run_scheduler();
        }
    }

    fn on_connection(&self, stream: TcpStream, address: SocketAddr) {
        // Handle an incoming connection; spawn gate object for it

        info!("New connection from {}", address);
        stream.set_nodelay(true).unwrap();
        let bootstrap = ::server_capnp::server_bootstrap::ToClient::new(
            ServerBootstrapImpl::new(self, address),
        ).from_server::<::capnp_rpc::Server>();

        let rpc_system = new_rpc_system(stream, Some(bootstrap.client));
        self.get().handle.spawn(rpc_system.map_err(|e| {
            panic!("RPC error: {:?}", e)
        }));
    }

    #[inline]
    pub fn handle(&self) -> Handle {
        self.get().handle.clone()
    }
}
