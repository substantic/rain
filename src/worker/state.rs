use std::rc::Rc;
use std::cell::RefCell;
use std::net::SocketAddr;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::process::exit;

use common::id::{SessionId, WorkerId, empty_worker_id};
use common::convert::{ToCapnp, FromCapnp};
use worker::graph::Graph;

use futures::Future;
use futures::Stream;
use tokio_core::reactor::Handle;
use tokio_core::net::TcpListener;
use tokio_core::net::TcpStream;
use tokio_io::AsyncRead;
use capnp_rpc::{RpcSystem, twoparty, rpc_twoparty_capnp};
use capnp::capability::Promise;

use WORKER_PROTOCOL_VERSION;


pub struct InnerState {

    handle: Handle, // Tokio core handle

    worker_id: WorkerId,
    upstream: Option<::worker_capnp::worker_upstream::Client>,

    n_cpus: u32,  // Resources

    graph: Graph,
}

#[derive(Clone)]
pub struct State {
    inner: Rc<RefCell<InnerState>>,
}


impl State {
    pub fn new(handle: Handle, n_cpus: u32) -> Self {
        Self {
            inner: Rc::new(RefCell::new(InnerState {
                handle,
                n_cpus,
                upstream: None,
                worker_id: empty_worker_id(),
                graph: Graph::new(),
            })),
        }
    }

    // Get number of cpus for assigned to this worker
    pub fn get_n_cpus(&self) -> u32 {
        self.inner.borrow().n_cpus
    }

    // This is called when an incomming connection arrives
    fn on_connection(&self, stream: TcpStream, address: SocketAddr) {
        // Handle an incoming connection; spawn gate object for it

        info!("New connection from {}", address);
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.split();

        panic!("Not implemented yet");
        /*
        let bootstrap_obj = ::server_capnp::server_bootstrap::ToClient::new(
            ServerBootstrapImpl::new(self, address),
        ).from_server::<::capnp_rpc::Server>();

        let network = twoparty::VatNetwork::new(
            reader,
            writer,
            rpc_twoparty_capnp::Side::Server,
            Default::default(),
        );

        let rpc_system = RpcSystem::new(Box::new(network), Some(bootstrap_obj.client));
        self.inner.borrow().handle.spawn(rpc_system.map_err(|e| {
            panic!("RPC error: {:?}", e)
        }));*/
    }

    // This is called when worker connection to server is established
    pub fn on_connected_to_server(&self, stream: TcpStream, listen_address: SocketAddr) {
        info!("Connected to server; registering as worker");
        stream.set_nodelay(true).unwrap();
        let mut rpc_system = ::common::rpc::new_rpc_system(stream, None);
        let bootstrap: ::server_capnp::server_bootstrap::Client =
            rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);

        let worker_control = ::worker_capnp::worker_control::ToClient::new(
            ::worker::control::WorkerControlImpl::new(self))
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
                let mut inner = state.inner.borrow_mut();
                inner.upstream = Some(upstream);
                inner.worker_id = WorkerId::from_capnp(&worker_id);
                debug!("Registration completed");
                Promise::ok(())
            })
            .map_err(|e| {
                panic!("Error {}", e);
            });

        let inner = self.inner.borrow();
        inner.handle.spawn(future);
        inner.handle
            .spawn(rpc_system.map_err(|e| println!("RPC error: {:?}", e)));
    }

    pub fn start(&self, server_address: SocketAddr, listen_address: SocketAddr) {
        // --- Start listening ----
        let handle = self.inner.borrow().handle.clone();
        let listener = TcpListener::bind(&listen_address, &handle).unwrap();
        let port = listener.local_addr().unwrap().port();
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
        info!("Connecting to server addr={}", server_address);
        let connect = TcpStream::connect(&server_address, &handle)
            .and_then(move |stream| {
                core1.on_connected_to_server(stream, listen_address);
                Ok(())
            })
            .map_err(|e| {
                error!("Connecting to server failed: {}", e);
                exit(1);
            });
        handle.spawn(connect);
    }

    pub fn turn(&self) {
        // Now do nothing
    }
}
