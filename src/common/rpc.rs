
use tokio_core::net::TcpStream;
use capnp_rpc::{RpcSystem, twoparty, rpc_twoparty_capnp};
use tokio_io::AsyncRead;


pub fn new_rpc_system
    (stream: TcpStream, bootstrap: Option<::capnp::capability::Client>) -> RpcSystem<twoparty::VatId>
    {
        let (reader, writer) = stream.split();
        let network = Box::new(twoparty::VatNetwork::new(reader,
                                                         writer,
                                                         rpc_twoparty_capnp::Side::Client,
                                                         Default::default()));
        RpcSystem::new(network, bootstrap)
}