use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp;
use common::rpc::new_rpc_system;
use futures::Future;
use std::error::Error;
use std::net::SocketAddr;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Core;

use CLIENT_PROTOCOL_VERSION;
use super::session::Session;

pub struct Client {
    core: Core,
    service: ::client_capnp::client_service::Client,
}

impl Client {
    pub fn new(scheduler: &SocketAddr) -> Result<Self, Box<Error>> {
        let mut core = Core::new()?;
        let handle = core.handle();
        let stream = core.run(TcpStream::connect(&scheduler, &handle))?;
        stream.set_nodelay(true)?;

        debug!("Connection to server {} established", scheduler);

        let mut rpc = Box::new(new_rpc_system(stream, None));
        let bootstrap: ::server_capnp::server_bootstrap::Client =
            rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
        handle.spawn(rpc.map_err(|err| panic!("RPC error: {}", err)));

        let mut request = bootstrap.register_as_client_request();
        request.get().set_version(CLIENT_PROTOCOL_VERSION);

        let service = core.run(
            request
                .send()
                .promise
                .and_then(|response| Promise::ok(pry!(response.get()).get_service())),
        )??;

        Ok(Client { core, service })
    }

    pub fn new_session(&mut self) -> Result<Session, Box<Error>> {
        let id: i32 = self.core.run(
            self.service
                .new_session_request()
                .send()
                .promise
                .and_then(|response| Promise::ok(pry!(response.get()).get_session_id())),
        )?;

        Ok(Session { id })
    }

    pub fn terminate_server(&mut self) -> Result<(), Box<Error>> {
        self.core
            .run(self.service.terminate_server_request().send().promise)?;
        Ok(())
    }
}
