use super::client::ClientServiceImpl;
use futures::{future::err, Future, Stream};
use rain_core::{
    comm::client_message::{
        ClientToServerMessage, RequestType, ResponseType, ServerToClientMessage,
    },
    Error,
};
use serde_cbor::{de::from_slice, ser::to_vec};
use server::{state::StateRef, ws::client::ClientService};
use std::{fmt::Debug, net::SocketAddr};
use tokio_core::reactor::Handle;
use websocket::{async::Server, server::InvalidConnection, OwnedMessage};

macro_rules! rpc_methods {
    ( $message:expr, $client:expr, $( ($method:ident, $implementation:ident) ),* ) => {
        match $message.data {
            $(
                RequestType::$method(data) => {
                    let id = $message.id;
                    debug!("Message from client: {:?}", stringify!($method));
                    return Some(Box::new($client.$implementation(data).map(move |r| {
                        ServerToClientMessage {
                            id,
                            data: ResponseType::$method(r)
                        }
                    })));
                }
            )*
        }
    }
}

fn handle_message(
    m: OwnedMessage,
    client: &mut dyn ClientService,
) -> Option<Box<Future<Item = ServerToClientMessage, Error = Error>>> {
    if let OwnedMessage::Binary(data) = m {
        let message = match from_slice::<ClientToServerMessage>(&data) {
            Ok(msg) => msg,
            Err(e) => {
                debug!("Received invalid message from client: {:?}", e);
                return Some(Box::new(err(e.into())));
            }
        };

        rpc_methods!(
            message,
            client,
            (RegisterClient, register_client),
            (NewSession, new_session),
            (CloseSession, close_session),
            (GetServerInfo, get_server_info),
            (Submit, submit),
            (Fetch, fetch),
            (Unkeep, unkeep),
            (Wait, wait),
            (WaitSome, wait_some),
            (GetState, get_state),
            (TerminateServer, terminate_server)
        )
    }

    None
}

pub struct ClientCommunicator {
    address: SocketAddr,
}

impl ClientCommunicator {
    pub fn new(address: SocketAddr) -> Self {
        ClientCommunicator { address }
    }

    pub fn start(&self, handle: Handle, state: StateRef) {
        let server = Server::bind(&self.address, &handle).unwrap();
        let protocol = "rain-ws";

        let start_handle = handle.clone();
        let handler = server
            .incoming()
            .map_err(|InvalidConnection { error, .. }| error)
            .for_each(move |(upgrade, addr)| {
                if !upgrade.protocols().iter().any(|s| s == protocol) {
                    spawn_future(upgrade.reject(), &handle);
                    return Ok(());
                }

                let mut client_impl = match ClientServiceImpl::new(addr, state.clone()) {
                    Ok(client) => client,
                    Err(_) => {
                        spawn_future(upgrade.reject(), &handle);
                        return Ok(());
                    }
                };

                let future = upgrade
                    .use_protocol(protocol)
                    .accept()
                    .map_err(|e| e.into())
                    .and_then(move |(s, _)| {
                        let (sink, stream) = s.split();
                        stream
                            .take_while(|m| Ok(!m.is_close()))
                            .map_err(|e| e.into())
                            .and_then(move |m| {
                                if let Some(fut) = handle_message(m, &mut client_impl) {
                                    Some(fut.map(|res| OwnedMessage::Binary(to_vec(&res).unwrap())))
                                } else {
                                    None
                                }
                            }).filter_map(|x| x)
                            .forward(sink)
                    });

                spawn_future(future, &handle);
                Ok(())
            });
        start_handle.spawn(handler.map_err(|e| panic!("RPC error: {:?}", e)));
    }
}

fn spawn_future<F, I, E>(f: F, handle: &Handle)
where
    F: Future<Item = I, Error = E> + 'static,
    E: Debug,
{
    // errors from individual clients are logged elsewhere and silently discarded here
    handle.spawn(
        f.map_err(|e| {
            println!("RPC error: {:?}", e);
        }).map(|_| {}),
    );
}
