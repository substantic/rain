use server::state::State;
use server_capnp::server_bootstrap;
use capnp::capability::Promise;
use std::net::SocketAddr;
use capnp;

use server::client_srv::ClientServiceImpl;

const CLIENT_PROTOCOL_VERSION: i32 = 0;

// Gate is the entry point of RPC service. It is created on server and provided
// to connection that can registered as worker or client.

pub struct ServerBootstrapImpl {
    state: State,
    registered: bool,
    address: SocketAddr,
}

impl ServerBootstrapImpl {
    pub fn new(state: &State, address: SocketAddr) -> Self {
        Self {
            state: state.clone(),
            registered: false,
            address: address,
        }
    }
}

impl Drop for ServerBootstrapImpl {
    fn drop(&mut self) {
        debug!("ServerBootstrap dropped {}", self.address);
    }
}

impl server_bootstrap::Server for ServerBootstrapImpl {
    fn register_as_client(&mut self,
                          params: server_bootstrap::RegisterAsClientParams,
                          mut results: server_bootstrap::RegisterAsClientResults)
                          -> Promise<(), ::capnp::Error> {

        if self.registered {
            error!("Multiple registration from connection {}", self.address);
            return Promise::err(capnp::Error::failed(format!("Connection already registered")));
        }

        let params = pry!(params.get());

        if params.get_version() != CLIENT_PROTOCOL_VERSION {
            error!("Client protocol mismatch");
            return Promise::err(capnp::Error::failed(format!("Protocol mismatch")));
        }

        self.registered = true;
        info!("Connection {} registered as client", self.address);

        let service =
            ::client_capnp::client_service::ToClient::new(ClientServiceImpl::new(&self.state))
                .from_server::<::capnp_rpc::Server>();

        results.get().set_service(service);
        Promise::ok(())
    }
}
