use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use common_capnp::{data_object_id, socket_address, task_id};
use super::convert::{FromCapnp, ReadCapnp, ToCapnp, WriteCapnp};
use std::io::Read;
use capnp::serialize;
use std::fmt;
use serde::{Serialize, Serializer, Deserialize, Deserializer};

/// Generic ID type. Negative values have special meaning.
pub type Id = i32;

/// Session ID type. Negative values have special meaning.
pub type SessionId = i32;

/// Type identifying a worker, in this case its address and port as seen by server.
/// When the worker has multiple addresses, one is selected and fixed on registrtion.
pub type WorkerId = SocketAddr;

/// Type identifying a client, in this case its address and port as seen by server.
pub type ClientId = SocketAddr;

/// Type identifying a subworker
pub type SubworkerId = Id;

impl<'a> FromCapnp<'a> for SocketAddr {
    type Reader = socket_address::Reader<'a>;

    fn from_capnp(read: &Self::Reader) -> Self {
        match read.get_address().which().unwrap() {
            socket_address::address::Ipv4(ref octet) => SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(*array_ref![octet.as_ref().unwrap(), 0, 4]),
                read.get_port(),
            )),
            socket_address::address::Ipv6(ref octet) => SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(*array_ref![octet.as_ref().unwrap(), 0, 16]),
                read.get_port(),
                0,
                0,
            )),
        }
    }
}

impl<'a> ToCapnp<'a> for SocketAddr {
    type Builder = socket_address::Builder<'a>;

    fn to_capnp(self: &WorkerId, build: &mut Self::Builder) {
        build.set_port(self.port());
        let build_addr = &mut build.reborrow().get_address();
        match self {
            &SocketAddr::V4(ref ipv4) => build_addr.set_ipv4(&ipv4.ip().octets()),
            &SocketAddr::V6(ref ipv6) => build_addr.set_ipv6(&ipv6.ip().octets()),
        }
    }
}

impl ReadCapnp for SocketAddr {
    fn read_capnp<R: Read>(r: &mut R) -> Self {
        let msg = serialize::read_message(r, Default::default()).unwrap();
        let read = msg.get_root::<socket_address::Reader>().unwrap();
        WorkerId::from_capnp(&read)
    }
}

/// Common trait for `TaskId` and `DataObjectID`.
pub trait SId
    : for<'a> ToCapnp<'a> + for<'a> FromCapnp<'a> + WriteCapnp + ReadCapnp {
    fn new(session_id: SessionId, id: Id) -> Self;
    fn get_id(&self) -> Id;
    fn get_session_id(&self) -> SessionId;

    fn invalid() -> Self {
        Self::new(-1, 0)
    }

    fn is_invalid(&self) -> bool {
        self.get_session_id() == -1
    }
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
        TaskId {
            session_id: session_id,
            id: id,
        }
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

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({},{})", self.get_session_id(), self.get_id())
    }
}

impl fmt::Display for DataObjectId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({},{})", self.get_session_id(), self.get_id())
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

impl ReadCapnp for TaskId {
    fn read_capnp<R: Read>(r: &mut R) -> Self {
        let msg = serialize::read_message(r, Default::default()).unwrap();
        let read = msg.get_root::<task_id::Reader>().unwrap();
        TaskId::from_capnp(&read)
    }
}

impl<'a> FromCapnp<'a> for TaskId {
    type Reader = task_id::Reader<'a>;

    fn from_capnp(read: &'a Self::Reader) -> Self {
        TaskId::new(read.get_session_id(), read.get_id())
    }
}

/// ID type for task objects.
#[derive(Copy, Clone, Debug, Ord, Eq, PartialEq, PartialOrd, Hash)]
pub struct DataObjectId {
    session_id: SessionId,
    id: Id,
}

impl SId for DataObjectId {
    #[inline]
    fn new(session_id: SessionId, id: Id) -> Self {
        DataObjectId {
            session_id: session_id,
            id: id,
        }
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

macro_rules! serde_for_id {
    ($T:ty) => {
impl Serialize for $T {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        (self.get_session_id(), self.get_id()).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for $T {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let tup: (SessionId, Id) = Deserialize::deserialize(deserializer)?;
        Ok(Self::new(tup.0, tup.1))
    }
}}}

serde_for_id!(TaskId);
serde_for_id!(DataObjectId);

impl<'a> ToCapnp<'a> for DataObjectId {
    type Builder = data_object_id::Builder<'a>;

    #[inline]
    fn to_capnp(self: &Self, build: &mut Self::Builder) {
        build.set_id(self.id);
        build.set_session_id(self.session_id);
    }
}

impl ReadCapnp for DataObjectId {
    fn read_capnp<R: Read>(r: &mut R) -> Self {
        let msg = serialize::read_message(r, Default::default()).unwrap();
        let read = msg.get_root::<data_object_id::Reader>().unwrap();
        DataObjectId::from_capnp(&read)
    }
}

impl<'a> FromCapnp<'a> for DataObjectId {
    type Reader = data_object_id::Reader<'a>;

    fn from_capnp(read: &'a Self::Reader) -> Self {
        DataObjectId::new(read.get_session_id(), read.get_id())
    }
}

// TODO(gavento): Replace Sid by Task/DO ID
pub type Sid = TaskId;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn worker_id_capnp_v6() {
        let mut buf: Vec<u8> = Vec::new();
        let addr6: SocketAddr = "[fd75::c5a:7c4e]:1024".parse().unwrap();
        addr6.write_capnp(&mut buf);
        assert_eq!(addr6, SocketAddr::read_capnp(&mut Cursor::new(&buf)));
    }

    #[test]
    fn worker_id_capnp_v4() {
        let mut buf: Vec<u8> = Vec::new();
        let addr4: SocketAddr = "156.234.100.2:32109".parse().unwrap();
        addr4.write_capnp(&mut buf);
        assert_eq!(addr4, WorkerId::read_capnp(&mut Cursor::new(&buf)));
    }

    #[test]
    fn task_id_capnp() {
        let mut buf: Vec<u8> = Vec::new();
        let id = TaskId::new(424242, -323232);
        id.write_capnp(&mut buf);
        assert_eq!(id, TaskId::read_capnp(&mut Cursor::new(&buf)));
    }

    #[test]
    fn data_object_id_capnp() {
        let mut buf: Vec<u8> = Vec::new();
        let id = DataObjectId::new(-1424242, 1323232);
        id.write_capnp(&mut buf);
        assert_eq!(id, DataObjectId::read_capnp(&mut Cursor::new(&buf)));
    }
}

pub fn empty_worker_id() -> WorkerId {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0)
}
