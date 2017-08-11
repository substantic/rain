use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6, Ipv4Addr, Ipv6Addr};

use common_capnp::{task_id, data_object_id, socket_address};
use super::{FromCapnp, ToCapnp};

/// Type identifying a worker, in this case its address and port as seen by server.
/// When the worker has multiple addresses, one is selected and fixed on registrtion.
pub type WorkerId = SocketAddr;

impl<'a> FromCapnp<'a> for WorkerId {
    type Reader = socket_address::Reader<'a>;

    fn from_capnp(read: &Self::Reader) -> Self {
        match read.get_address().which().unwrap() {
            socket_address::address::Ipv4(ref octet) =>
                SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::from(*array_ref![octet.as_ref().unwrap(), 0, 4]),
                    read.get_port())),
            socket_address::address::Ipv6(ref octet) =>
                SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::from(*array_ref![octet.as_ref().unwrap(), 0, 16]),
                    read.get_port(), 0, 0)),
        }
    }
}

impl<'a> ToCapnp<'a> for WorkerId {
    type Builder = socket_address::Builder<'a>;

    fn to_capnp(self: &WorkerId, build: &mut Self::Builder) {
        build.set_port(self.port());
        let mut build_addr = &mut build.borrow().get_address();
        match self {
            &SocketAddr::V4(ref ipv4) => build_addr.set_ipv4(&ipv4.ip().octets()),
            &SocketAddr::V6(ref ipv6) => build_addr.set_ipv6(&ipv6.ip().octets()),
        }
    }
}

/// Generic ID type. Negative values have special meaning.
pub type Id = i32;

/// Session ID type. Negative values have special meaning.
pub type SessionId = i32;

trait SId: for<'a> FromCapnp<'a> + for <'a> ToCapnp<'a> {
    fn new(session_id: SessionId, id: Id) -> Self;
    fn get_id(&self) -> Id;
    fn get_session_id(&self) -> SessionId;
}

/// ID type for task objects.
#[derive(Copy, Clone, Debug, Ord, Eq, PartialEq, PartialOrd, Hash)]
pub struct TaskId {
    session_id: SessionId,
    id: Id,
}

impl SId for TaskId {
    #[inline]
    fn new(session_id: SessionId, id: Id) -> Self {
        TaskId { session_id: session_id, id: id }
    }

    #[inline]
    fn get_id(&self) -> Id {
        self.id
    }

    #[inline]
    fn get_session_id(&self) -> SessionId {
        self.session_id
    }
}

impl<'a> ToCapnp<'a> for TaskId {
    type Builder = task_id::Builder<'a>;

    #[inline]
    fn to_capnp(self: &Self, build: &mut Self::Builder) {
        build.set_id(self.id);
        build.set_session_id(self.session_id);
    }
}

impl<'a> FromCapnp<'a> for TaskId {
    type Reader = task_id::Reader<'a>;

    #[inline]
    fn from_capnp(read: &Self::Reader) -> Self {
        TaskId::new(read.get_session_id(), read.get_id())
    }
}

// TODO(gavento): Replace Sid by Task/DO ID
pub type Sid = TaskId;
// TODO(gavento): Create DataObjectId as an independent object
pub type DataObjectId = TaskId;

#[cfg(test)]
mod tests {
    use super::*;
    use common::{FromCapnp, ToCapnp};
    use common_capnp::{task_id, data_object_id, socket_address};
    use capnp::{message, serialize};

    #[test]
    fn worker_id_capnp() {
        let addr6: SocketAddr = "[fd75::c5a:7c4e]:1024".parse().unwrap();
        let mut msg = message::Builder::new_default();
        addr6.to_capnp(&mut msg.get_root::<socket_address::Builder>().unwrap());
        let mut buf: Vec<u8> = Vec::new();
        serialize::write_message(&mut buf, &msg).unwrap();
    }
}