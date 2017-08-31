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
use server::graph::{Graph, Worker, DataObject, Task, Session, Client};
use server::rpc::ServerBootstrapImpl;
use common::wrapped::WrappedRcRefCell;

use common::resources::Resources;

pub struct Inner {
    // Contained objects
    pub(super) graph: Graph,

    /// Listening port and address.
    listen_address: SocketAddr,

    /// Tokio core handle.
    handle: Handle,
}

/// Note: No `Drop` impl as a `State` is assumed to live forever.
pub type State = WrappedRcRefCell<Inner>;

impl State {
    pub fn new(handle: Handle, listen_address: SocketAddr) -> Self {
        Self::wrap(Inner {
            graph: Default::default(),
            listen_address: listen_address,
            handle: handle,
        })
    }

    pub fn add_worker(&self,
                      address: SocketAddr,
                      control: Option<::worker_capnp::worker_control::Client>,
                      resources: Resources) -> Worker {
        unimplemented!()
    }

    pub fn remove_worker(&self, worker: &Worker) {
        unimplemented!()
    }

    pub fn add_client(&self, address: &SocketAddr) -> Client {
        unimplemented!()
    }

    pub fn remove_client(&self, client: &Client) { unimplemented!() }

    pub fn add_session(&self, client: &Client) -> Session {
        unimplemented!()
    }

    pub fn remove_session(&self, session: &Session) { unimplemented!() }

    pub fn add_object(&self, session: &Session, id: DataObjectId /* TODO: more */) -> DataObject {
        unimplemented!()
    }

    pub fn remove_object(&self, object: &DataObject) { unimplemented!() }

    pub fn unkeep_object(&self, object: &DataObject) { unimplemented!() }

    pub fn add_task(&self, session: &Session, id: TaskId /* TODO: more */) -> Task {
        unimplemented!()
    }

    pub fn remove_task(&self, task: &Task) { unimplemented!() }

    pub fn get_worker(&self, id: WorkerId) -> Result<Worker> {
        let s = self.get();
        let g = s.graph.get();
        match g.workers.get(&id) {
            Some(w) => Ok(w.clone()),
            None => Err(format!("Worker {:?} not found", id))?,
        }
    }

    pub fn get_client(&self, id: ClientId) -> Result<Client> {
        let s = self.get();
        let g = s.graph.get();
        match g.clients.get(&id) {
            Some(c) => Ok(c.clone()),
            None => Err(format!("Client {:?} not found", id))?,
        }
    }

    pub fn get_session(&self, id: SessionId) -> Result<Session> {
        let s = self.get();
        let g = s.graph.get();
        match g.sessions.get(&id) {
            Some(s) => Ok(s.clone()),
            None => Err(format!("Session {:?} not found", id))?,
        }
    }

    pub fn get_object(&self, id: DataObjectId) -> Result<DataObject> {
        let s = self.get();
        let g = s.graph.get();
        match g.objects.get(&id) {
            Some(o) => Ok(o.clone()),
            None => Err(format!("Object {:?} not found", id))?,
        }
    }

    pub fn get_task(&self, id: TaskId) -> Result<Task> {
        let s = self.get();
        let g = s.graph.get();
        match g.tasks.get(&id) {
            Some(t) => Ok(t.clone()),
            None => Err(format!("Task {:?} not found", id))?,
        }
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
        // Now do nothing
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
