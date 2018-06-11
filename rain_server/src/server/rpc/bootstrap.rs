use capnp;
use capnp::capability::Promise;
use futures::Future;
use std::net::SocketAddr;
use rain_core::{errors::*, types::*, utils::*, comm::*};
use rain_core::server_capnp::server_bootstrap;

use super::{ClientServiceImpl, GovernorUpstreamImpl};
use server::state::StateRef;

use rain_core::{CLIENT_PROTOCOL_VERSION, GOVERNOR_PROTOCOL_VERSION};

// ServerBootstrap is the entry point of RPC service.
// It is created on the server and provided
// to every incomming connections

pub struct ServerBootstrapImpl {
    state: StateRef,
    registered: bool,    // true if the connection is already registered
    address: SocketAddr, // Remote address of the connection
}

impl ServerBootstrapImpl {
    pub fn new(state: &StateRef, address: SocketAddr) -> Self {
        ServerBootstrapImpl {
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
    fn register_as_client(
        &mut self,
        params: server_bootstrap::RegisterAsClientParams,
        mut results: server_bootstrap::RegisterAsClientResults,
    ) -> Promise<(), ::capnp::Error> {
        if self.registered {
            error!("Multiple registration from connection {}", self.address);
            return Promise::err(capnp::Error::failed(format!(
                "Connection already registered"
            )));
        }

        let params = pry!(params.get());

        if params.get_version() != CLIENT_PROTOCOL_VERSION {
            error!("Client protocol mismatch");
            return Promise::err(capnp::Error::failed(format!("Protocol mismatch")));
        }

        self.registered = true;

        let service = ::rain_core::client_capnp::client_service::ToClient::new(pry!(ClientServiceImpl::new(
            &self.state,
            &self.address
        ))).from_server::<::capnp_rpc::Server>();

        info!("Connection {} registered as client", self.address);
        results.get().set_service(service);
        Promise::ok(())
    }

    fn register_as_governor(
        &mut self,
        params: server_bootstrap::RegisterAsGovernorParams,
        mut results: server_bootstrap::RegisterAsGovernorResults,
    ) -> Promise<(), ::capnp::Error> {
        if self.registered {
            error!("Multiple registration from connection {}", self.address);
            return Promise::err(capnp::Error::failed(format!(
                "Connection already registered"
            )));
        }

        let params = pry!(params.get());

        if params.get_version() != GOVERNOR_PROTOCOL_VERSION {
            error!("Governor protocol mismatch");
            return Promise::err(capnp::Error::failed(format!("Protocol mismatch")));
        }

        self.registered = true;

        // If governor fully specifies its address, then we use it as governor_id
        // otherwise we use announced port number and assign IP address of connection
        let address = GovernorId::from_capnp(&pry!(params.get_address()));
        let governor_id = if address.ip().is_unspecified() {
            SocketAddr::new(self.address.ip(), address.port())
        } else {
            address
        };

        let resources = Resources::from_capnp(&pry!(params.get_resources()));

        info!(
            "Connection {} registered as governor {} with {:?}",
            self.address, governor_id, resources
        );

        let control = pry!(params.get_control());
        let state = self.state.clone();

        // Ask for resources and then create a new governor in server
        let req = control.get_governor_resources_request();
        Promise::from_future(req.send().promise.and_then(move |_| {
            // The order is important here:
            // 1) add governor
            // 2) create upstream
            // reason: upstream drop tries to remove governor
            let governor = pry!(state.get_mut().add_governor(
                governor_id,
                Some(control),
                resources,
            ));
            let upstream = ::rain_core::governor_capnp::governor_upstream::ToClient::new(
                GovernorUpstreamImpl::new(&state, &governor),
            ).from_server::<::capnp_rpc::Server>();
            results.get().set_upstream(upstream);
            governor_id.to_capnp(&mut results.get().get_governor_id().unwrap());
            Promise::ok(())
        }))
    }
}
