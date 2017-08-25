use std::net::{SocketAddr};
use std::collections::HashMap;

use futures::{Future, Stream};
use tokio_core::reactor::Handle;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_io::AsyncRead;
use capnp_rpc::{RpcSystem, twoparty, rpc_twoparty_capnp};

use common::id::{SessionId, WorkerId, DataObjectId, TaskId, ClientId};
use common::rpc::new_rpc_system;
use server::graph::{Graph, Worker, DataObject, Task, Session, Client};
use server::rpc::ServerBootstrapImpl;
use common::wrapped::WrappedRcRefCell;

pub struct Inner {
    // Contained objects
    pub(super) graph: Graph,

    /// Listening port and address.
    listen_address: SocketAddr,

    /// Tokio core handle.
    handle: Handle,
}

pub type State = WrappedRcRefCell<Inner>;


// TODO: Functional cleanup of code below after structures specification

impl State {
    pub fn new(handle: Handle, listen_address: SocketAddr) -> Self {
        Self::wrap(Inner {
            graph: Default::default(),
            listen_address: listen_address,
            handle: handle,
        })
    }

    pub fn add_worker(&self, worker: Worker) {
        unimplemented!();
    }

    pub fn remove_worker(&self, worker: &Worker) {
        unimplemented!();
    }

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
