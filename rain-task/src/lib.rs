#![allow(unused_imports)]

//! A library to easily write custom Rain tasks in Rust.
//!
//! See `README.md` for more information.
//!
//! # Example
//!
//! ```rust,no_run
//! #[macro_use] // For register_task! if you want to use it
//! use rain_task::*;
//! use std::io::Write;
//!
//! // A task with a single input and single output
//! fn task_hello(_ctx: &mut Context, input: &DataInstance, output: &mut Output) -> TaskResult<()> {
//!     write!(output, "Hello {}", input.get_str()?)?;
//!     Ok(())
//! }
//!
//! fn main() {
//!     let mut s = Subworker::new("greeter"); // The subworker type name
//!     // Use a macro to register the task.
//!     // [I O] here specifies the type and order of parameters.
//!     register_task!(s, "hello", [I O], task_hello);
//!     s.run(); // Runs the subworker event loop
//! }
//! ```

extern crate byteorder;
extern crate librain;
#[macro_use]
extern crate log;
#[macro_use]
extern crate error_chain;
extern crate chrono;
extern crate env_logger;
extern crate memmap;
extern crate serde_cbor;

use std::collections::HashMap;
use std::default::Default;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::BufWriter;
use std::io::Write;
use std::mem::swap;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::path::PathBuf;

use librain::common::id::SId;
use librain::common::id::{DataObjectId, SubworkerId, TaskId};
use librain::common::Attributes;
use librain::worker::rpc::subworker_serde::*;

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

pub use context::Context;
pub use errors::{TaskError, TaskResult};
pub use input::DataInstance;
pub use output::Output;
pub use subworker::{Subworker, TaskFn};

#[cfg(test)]
mod tests;
