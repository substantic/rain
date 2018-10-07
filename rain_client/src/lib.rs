extern crate capnp_rpc;
#[macro_use]
extern crate error_chain;
extern crate futures;
#[macro_use]
extern crate log;
extern crate rain_core;
extern crate serde_json;
extern crate tokio_core;

pub mod client;
pub use client::*;
