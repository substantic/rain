
use std::rc::Rc;
use std::cell::RefCell;
use std::net::SocketAddr;
use std::net::IpAddr;
use std::net::Ipv4Addr;

use common::id::{SessionId, WorkerId};
use common::rpc::new_rpc_system;
use server::graph::Graph;
use server::worker::Worker;
use server::interface::ServerBootstrapImpl;

use futures::Future;
use futures::Stream;
use tokio_core::reactor::Handle;
use tokio_core::net::TcpListener;
use tokio_core::net::TcpStream;
use tokio_io::AsyncRead;
use capnp_rpc::{RpcSystem, twoparty, rpc_twoparty_capnp};


struct StateInner {
    //graph: Graph,
    handle: Handle, // Tokio core handle

    workers: Vec<Worker>,

    session_id_counter: SessionId,
    listen_address: SocketAddr, // Listening port
}

#[derive(Clone)]
pub struct State {
    inner: Rc<RefCell<StateInner>>,
}

impl State {
    pub fn new(handle: Handle, listen_address: SocketAddr) -> Self {
        Self {
            inner: Rc::new(RefCell::new(StateInner {
                handle: handle,
                listen_address: listen_address,
                workers: Vec::new(),
                session_id_counter: 1,
            })),
        }
    }

    pub fn add_worker(&self, worker: Worker) {
        self.inner.borrow_mut().workers.push(worker);
    }

    pub fn remove_worker(&self, worker: &Worker) {
        // TODO removing workers
        panic!("Worker removed; not implemented yet");
    }

    pub fn get_n_workers(&self) -> usize {
        self.inner.borrow().workers.len()
    }

    pub fn start(&self) {
        let listen_address = self.inner.borrow().listen_address;
        let handle = self.inner.borrow().handle.clone();
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

    // Creates a new session and returns its id
    pub fn new_session(&self) -> SessionId {
        let mut inner = self.inner.borrow_mut();
        let session_id = inner.session_id_counter;
        inner.session_id_counter += 1;
        debug!("Creating a new session (session_id={})", session_id);
        session_id
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
        self.inner.borrow().handle.spawn(rpc_system.map_err(|e| {
            panic!("RPC error: {:?}", e)
        }));
    }
}
