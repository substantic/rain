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
pub const MSG_PROTOCOL: &str = "v1-CBOR";

/// Size limit for memory-backed objects. Larger blobs
/// get written to the filesystem.
pub const MEM_BACKED_LIMIT: usize = 128 * 1024;

#[macro_use]
mod macros;

mod framing;
use framing::*;

mod errors;
use errors::*;

mod subworker;
use subworker::*;

mod output;
use output::*;

mod context;
use context::*;

mod input;
use input::*;

pub use errors::{TaskError, TaskResult};
pub use subworker::{TaskFn, Subworker};
pub use output::Output;
pub use input::DataInstance;
pub use context::Context;

#[cfg(test)]
mod tests;

