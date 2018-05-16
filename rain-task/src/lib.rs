#![allow(unused_imports)]

extern crate librain;
extern crate byteorder;
#[macro_use]
extern crate log;
#[macro_use]
extern crate error_chain;
extern crate serde_cbor;
extern crate memmap;
extern crate chrono;
extern crate env_logger;

use std::collections::HashMap;
use std::path::PathBuf;
use std::os::unix::net::UnixStream;
use std::io;
use std::default::Default;
use std::mem::swap;
use std::fs::{OpenOptions, File};
use std::io::BufWriter;
use std::path::Path;
use std::io::Write;

use librain::common::id::{TaskId, DataObjectId, SubworkerId};
use librain::common::Attributes;
use librain::worker::rpc::subworker_serde::*;
use librain::common::id::SId;

/// Maximal protocol message size (128 MB)
pub const MAX_MSG_SIZE: usize = 128 * 1024 * 1024;

/// Current protocol code name and magic string
pub const MSG_PROTOCOL: &str = "cbor-1";

/// Size limit for memory-backed objects. Larger blobs
/// get written to the filesystem.
pub const MEM_BACKED_LIMIT: usize = 128 * 1024;

#[macro_use]
mod macros;

mod framing;
use framing::*;

mod errors;
pub use errors::*;

mod subworker;
pub use subworker::*;

mod output;
pub use output::*;

mod context;
pub use context::*;

mod input;
pub use input::*;

#[cfg(test)]
mod tests;

