#[macro_use]
extern crate log;
#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate capnp;
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
#[macro_use]
extern crate capnp_rpc;
#[macro_use]
extern crate arrayref;

pub mod common;
pub mod worker;
pub mod server;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
pub const WORKER_PROTOCOL_VERSION: i32 = 0;
pub const CLIENT_PROTOCOL_VERSION: i32 = 0;

// NOTE: Development solution to get type autocompletion and go-to-definition
pub mod capnp_gen;
pub use capnp_gen::*;

/* // NOTE: PyCharm does not support feature switching, so using comments :'(
pub mod server_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/server_capnp.rs"));
}

pub mod client_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/client_capnp.rs"));
}

pub mod common_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/common_capnp.rs"));
}

pub mod worker_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/worker_capnp.rs"));
}

pub mod datastore_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/datastore_capnp.rs"));
}
*/