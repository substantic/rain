use serde_cbor;
use std::fmt;

// Create the Error, ErrorKind, ResultExt, and Result types
error_chain!{
    types {
        Error, ErrorKind, ResultExt;
    }
    foreign_links {
        Io(::std::io::Error);
        CBOR(serde_cbor::error::Error);
        Utf8Err(::std::str::Utf8Error);
    }
}

// Explicit alias just to make some IDEs happier
pub type Result<T> = ::std::result::Result<T, Error>;


/// A simplified string error for the task functions
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TaskError {
    message: String,
}

impl<'a> From<&'a str> for TaskError {
    fn from(msg: &'a str) -> TaskError {
        TaskError { message: msg.into() }
    }
}

impl<'a> From<String> for TaskError {
    fn from(msg: String) -> TaskError {
        TaskError { message: msg }
    }
}


impl<'a> fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "task error: {}", self.message)
    }
}

pub type TaskResult<T> = ::std::result::Result<T, TaskError>;