#![allow(renamed_and_removed_lints)]

use std::fmt;

use types::TaskId;
use utils::convert::ToCapnp;

// Create the Error, ErrorKind, ResultExt, and Result types
error_chain!{
    types {
        Error, ErrorKind, ResultExt;
    }
    foreign_links {
        Io(::std::io::Error);
        Capnp(::capnp::Error);
        CapnpNotInSchema(::capnp::NotInSchema);
        Timer(::tokio_timer::Error);
        SessionErr(SessionError);
        Utf8Err(::std::str::Utf8Error);
        Json(::serde_json::Error);
        Sqlite(::rusqlite::Error);
    }

    errors {
        Ignored {
            description("Request asked for ignored id")
        }
    }
}

// Explicit alias just to make the IDEs happier
pub type Result<T> = ::std::result::Result<T, Error>;

impl ::std::convert::From<Error> for ::capnp::Error {
    fn from(e: Error) -> Self {
        ::capnp::Error::failed(e.description().to_string())
    }
}

#[derive(Debug, Clone)]
pub struct SessionError {
    message: String,
    debug: String,
    task_id: TaskId,
}

impl SessionError {
    pub fn new(message: String, debug: String, task_id: TaskId) -> Self {
        SessionError {
            message,
            debug,
            task_id,
        }
    }

    pub fn to_capnp(&self, builder: &mut ::common_capnp::error::Builder) {
        builder.reborrow().set_message(&self.message);
        builder.reborrow().set_debug(&self.debug);
        self.task_id
            .to_capnp(&mut builder.reborrow().get_task().unwrap());
    }
}

impl ::std::error::Error for SessionError {
    fn description(&self) -> &str {
        &self.message
    }

    fn cause(&self) -> Option<&::std::error::Error> {
        None
    }
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SessionError({:?})", self.message)
    }
}
