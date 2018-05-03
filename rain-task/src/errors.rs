use serde_cbor;

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
