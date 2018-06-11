#[macro_use]
extern crate arrayref;
extern crate bytes;
#[macro_use]
extern crate capnp;
extern crate capnp_rpc;
extern crate chrono;
#[macro_use]
extern crate error_chain;
extern crate futures;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate nix;
extern crate rusqlite;
extern crate serde;
extern crate serde_bytes;
extern crate serde_cbor;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate sys_info;
extern crate sysconf;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_timer;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GOVERNOR_PROTOCOL_VERSION: i32 = 0;
pub const CLIENT_PROTOCOL_VERSION: i32 = 0;
pub const EXECUTOR_PROTOCOL_VERSION: i32 = 0;

pub mod comm;
pub mod errors;
pub mod logging;
pub mod sys;
pub mod types;
pub mod utils;

pub use errors::{Error, Result, ErrorKind, ResultExt};

pub mod server_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/server_capnp.rs"));
}

pub mod client_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/client_capnp.rs"));
}

pub mod common_capnp {
    use std::fmt;

    include!(concat!(env!("OUT_DIR"), "/capnp/common_capnp.rs"));

    impl fmt::Debug for DataObjectState {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "{}",
                match *self {
                    DataObjectState::Unfinished => "Unfinished",
                    DataObjectState::Finished => "Finished",
                    DataObjectState::Removed => "Removed",
                }
            )
        }
    }

    impl fmt::Debug for TaskState {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(
                f,
                "{}",
                match *self {
                    TaskState::NotAssigned => "NotAssigned",
                    TaskState::Assigned => "Assigned",
                    TaskState::Ready => "Ready",
                    TaskState::Running => "Running",
                    TaskState::Finished => "Finished",
                    TaskState::Failed => "Failed",
                }
            )
        }
    }
}

pub mod governor_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/governor_capnp.rs"));
}

pub mod monitor_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/monitor_capnp.rs"));
}

