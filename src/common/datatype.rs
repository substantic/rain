//use super::convert::{FromCapnp, ToCapnp, WriteCapnp};

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum DataType {
    Blob,
    Directory,
}

impl DataType {
    pub fn from_capnp(value: ::datastore_capnp::DataType) -> DataType {
        match value {
            ::datastore_capnp::DataType::Blob => DataType::Blob,
            ::datastore_capnp::DataType::Directory => DataType::Directory,
        }
    }

    pub fn to_capnp(&self) -> ::datastore_capnp::DataType {
        match self {
            &DataType::Blob => ::datastore_capnp::DataType::Blob,
            &DataType::Directory => ::datastore_capnp::DataType::Directory,
        }
    }
}

/*
impl<'a> ToCapnp<'a> for DataType {
    type Builder = ::datastore_capnp::DataType::Builder<'a>;

    fn to_capnp(self: &Self, build: &mut Self::Builder) {

    }
}

impl<'a> FromCapnp<'a> for DataType {
    type Reader = ::datastore_capnp::DataType::Reader<'a>;

    fn from_capnp(reader: &'a Self::Reader) -> Self {
        match reader {
            ::datastore_capnp::DataType::Blob => DataType::Blob,
        }
    }
}*/
