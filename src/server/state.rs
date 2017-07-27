
use std::rc::Rc;
use std::cell::RefCell;
use std::net::SocketAddr;
use std::net::IpAddr;
use std::net::Ipv4Addr;

use server::graph::Graph;
use server::worker::Worker;
use server::gate::GateImpl;

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
    port: u16       // Listening port
}

#[derive(Clone)]
pub struct State {
    inner: Rc<RefCell<StateInner>>
}

impl State {

    pub fn new(handle: Handle, port: u16) -> Self {
        Self {
            inner: Rc::new(RefCell::new(StateInner {
                handle, port
            }))
        }
    }

    pub fn get_n_workers(&self) -> usize {
        // Return number of workers
        0 // TODO
    }

    pub fn start(&self) {
        let port = self.inner.borrow().port;
        let handle = self.inner.borrow().handle.clone();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
        let listener = TcpListener::bind(&addr, &handle).unwrap();

        let state = self.clone();
        let future = listener.incoming()
            .for_each(move |(stream, addr)| {
                state.on_connection(stream, addr);
                Ok(())
            })
            .map_err(|e| {
                panic!("Listening failed {:?}", e);
            });
        handle.spawn(future);
        info!("Start listening on port={}", port);
    }

    pub fn turn(&self) {
        // Now do nothing
    }

    fn on_connection(&self, stream: TcpStream, address: SocketAddr) {
        // Handle an incoming connection; spawn gate object for it

        info!("New connection from {}", address);
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.split();
        let gate_obj = ::gate_capnp::gate::ToClient::new(
            GateImpl::new(self, address))
            .from_server::<::capnp_rpc::Server>();

        let network = twoparty::VatNetwork::new(reader,
                                                writer,
                                                rpc_twoparty_capnp::Side::Server,
                                                Default::default());

        let rpc_system = RpcSystem::new(Box::new(network), Some(gate_obj.client));
        self.inner
            .borrow()
            .handle
            .spawn(rpc_system.map_err(|e| panic!("RPC error: {:?}", e)));
    }
}
