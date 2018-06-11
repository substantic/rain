#![allow(renamed_and_removed_lints)]

use std::{fmt, io};

/// The internal error type
error_chain!{
    types {
        Error, ErrorKind, ResultExt;
    }
    foreign_links {
        Io(::std::io::Error);
    }
}

// Explicit alias just to make some IDEs happier
pub type Result<T> = ::std::result::Result<T, Error>;

/// A string error for the task functions.
///
/// A conversion from `io::Error` is provided for convenience via conversion to string.
/// TODO: Add backtrace on error construction, possibly use error_chain.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TaskError {
    message: String,
}

impl<'a> From<&'a str> for TaskError {
    fn from(msg: &'a str) -> TaskError {
        TaskError {
            message: msg.into(),
        }
    }
}

impl From<String> for TaskError {
    fn from(msg: String) -> TaskError {
        TaskError { message: msg }
    }
}

impl From<io::Error> for TaskError {
    fn from(err: io::Error) -> TaskError {
        TaskError {
            message: format!("{}", err),
        }
    }
}

impl<'a> fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "task error: {}", self.message)
    }
}

/// A `Result` with `TaskError`.
pub type TaskResult<T> = ::std::result::Result<T, TaskError>;
