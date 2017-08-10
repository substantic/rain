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

pub mod common;
pub mod worker;
pub mod server;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub mod server_capnp {
    #[cfg(feature="generated_capnp")]
    include!(concat!(env!("OUT_DIR"), "/capnp/server_capnp.rs"));
    #[cfg(not(feature="generated_capnp"))]
    include!("capnp/server_capnp.rs");
}

pub mod client_capnp {
    #[cfg(feature="generated_capnp")]
    include!(concat!(env!("OUT_DIR"), "/capnp/client_capnp.rs"));
    #[cfg(not(feature="generated_capnp"))]
    include!("capnp/client_capnp.rs");
}

pub mod common_capnp {
    #[cfg(feature="generated_capnp")]
    include!(concat!(env!("OUT_DIR"), "/capnp/common_capnp.rs"));
    #[cfg(not(feature="generated_capnp"))]
    include!("capnp/common_capnp.rs");
}

pub mod worker_capnp {
    #[cfg(feature="generated_capnp")]
    include!(concat!(env!("OUT_DIR"), "/capnp/worker_capnp.rs"));
    #[cfg(not(feature="generated_capnp"))]
    include!("capnp/worker_capnp.rs");
}

pub mod datastore_capnp {
    #[cfg(feature="generated_capnp")]
    include!(concat!(env!("OUT_DIR"), "/capnp/datastore_capnp.rs"));
    #[cfg(not(feature="generated_capnp"))]
    include!("capnp/datastore_capnp.rs");
}
