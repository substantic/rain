use std::rc::Rc;
use std::cell::RefCell;
use std::net::SocketAddr;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::process::exit;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std;

use common::id::{SessionId, WorkerId, empty_worker_id, Id};
use common::convert::{ToCapnp, FromCapnp};
use common::rpc::new_rpc_system;
use worker::graph::Graph;
use worker::subworker::{start_python_subworker,
                        SubworkerUpstreamImpl,
                        Subworker};

use futures::Future;
use futures::Stream;
use tokio_core::reactor::Handle;
use tokio_core::net::TcpListener;
use tokio_core::net::TcpStream;
use tokio_io::AsyncRead;
use tokio_timer;
use tokio_uds::{UnixListener, UnixStream};
use capnp_rpc::{RpcSystem, twoparty, rpc_twoparty_capnp};
use capnp::capability::Promise;

use WORKER_PROTOCOL_VERSION;


pub struct InnerState {

    handle: Handle, // Tokio core handle

    worker_id: WorkerId,
    upstream: Option<::worker_capnp::worker_upstream::Client>,
    timer: tokio_timer::Timer,
    subworkers: Vec<Subworker>,
    work_dir: PathBuf,
    n_cpus: u32,  // Resources
    graph: Graph,
    id_counter: Id, // Internal Id counter
}

#[derive(Clone)]
pub struct State {
    inner: Rc<RefCell<InnerState>>,
}


impl State {

    pub fn new(handle: Handle, work_dir: PathBuf, n_cpus: u32) -> Self {
        Self {
            inner: Rc::new(RefCell::new(InnerState {
                handle,
                n_cpus,
                upstream: None,
                timer: tokio_timer::wheel()
                    .tick_duration(Duration::from_millis(100))
                    .num_slots(256).build(),
                work_dir,
                worker_id: empty_worker_id(),
                graph: Graph::new(),
                subworkers: Vec::new(),
                id_counter: 0
            })),
        }
    }

    pub fn get_new_id(&self) -> Id {
        let mut inner = self.inner.borrow_mut();
        inner.id_counter += 1;
        inner.id_counter
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
            .spawn(rpc_system.map_err(|e| error!("RPC error: {:?}", e)));
    }

    pub fn on_subworker_connection(&self, stream: UnixStream) {
        info!("New subworker connected");
        let upstream = ::subworker_capnp::subworker_upstream::ToClient::new(
            SubworkerUpstreamImpl::new(self),
        ).from_server::<::capnp_rpc::Server>();
        let rpc_system = new_rpc_system(stream, Some(upstream.client));
        let inner = self.inner.borrow();
        inner.handle.spawn(rpc_system.map_err(|e| error!("RPC error: {:?}", e)));
    }

    pub fn start(&self, server_address: SocketAddr, listen_address: SocketAddr) {
        let handle = self.inner.borrow().handle.clone();

        // --- Create workdir layout ---
        self.create_dir_in_work_dir(Path::new("data")).unwrap();
        self.create_dir_in_work_dir(Path::new("tasks")).unwrap();
        self.create_dir_in_work_dir(Path::new("subworkers")).unwrap();
        self.create_dir_in_work_dir(Path::new("subworkers/logs")).unwrap();

        // --- Start listening Unix socket for subworkers ----
        let listener = UnixListener::bind(self.subworker_listen_path(), &handle)
            .expect("Cannot initialize unix socket for subworkers");
        let state = self.clone();
        let future = listener.incoming()
            .for_each(move |(stream, addr)| {
                state.on_subworker_connection(stream);
                Ok(())
            })
            .map_err(|e| {
                panic!("Subworker listening failed {:?}", e);
            });
        handle.spawn(future);

        // -- Start python subworker (FOR TESTING PURPOSE)
        start_python_subworker(self);

        // --- Start listening TCP/IP for worker2worker communications ----
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

    pub fn add_subworker(&self, subworker: Subworker) {
        info!("Subworker registered subworker_id={}", subworker.id());
        self.inner.borrow_mut().subworkers.push(subworker);
        // TODO: Someone probably started subworker and he wants to be notified
    }

    pub fn turn(&self) {
        // Now do nothing
    }

    #[inline]
    pub fn path_in_work_dir(&self, path: &Path) -> PathBuf {
        self.inner.borrow().work_dir.join(path)
    }

    pub fn create_dir_in_work_dir(&self, path: &Path) -> std::io::Result<()> {
        std::fs::create_dir(self.path_in_work_dir(path))
    }

    pub fn subworker_listen_path(&self) -> PathBuf {
        self.path_in_work_dir(Path::new("subworkers/listen"))
    }

    pub fn subworker_log_paths(&self, id: Id) -> (PathBuf, PathBuf) {
        let out = self.path_in_work_dir(Path::new(&format!("subworkers/logs/subworker-{}.out", id)));
        let err = self.path_in_work_dir(Path::new(&format!("subworkers/logs/subworker-{}.err", id)));
        (out, err)
    }

    #[inline]
    pub fn handle(&self) -> Handle {
        self.inner.borrow().handle.clone()
    }

    #[inline]
    pub fn spawn<F>(&self, f: F) where F: Future<Item = (), Error = ()> + 'static {
        self.inner.borrow().handle.spawn(f);
    }
}
