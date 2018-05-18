//use super::convert::{FromCapnp, ToCapnp, WriteCapnp};

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum DataType {
    Blob,
    Directory,
}

impl DataType {
    pub fn from_capnp(value: ::common_capnp::DataType) -> DataType {
        match value {
            ::common_capnp::DataType::Blob => DataType::Blob,
            ::common_capnp::DataType::Directory => DataType::Directory,
        }
    }

    pub fn to_capnp(&self) -> ::common_capnp::DataType {
        match self {
            &DataType::Blob => ::common_capnp::DataType::Blob,
            &DataType::Directory => ::common_capnp::DataType::Directory,
        }
    }

    pub fn to_attribute(&self) -> &'static str {
        match self {
            &DataType::Blob => "blob",
            &DataType::Directory => "directory",
        }
    }

    pub fn from_attribute(name: &str) -> Self {
        match name {
            "blob" => DataType::Blob,
            "directory" => DataType::Directory,
            _ => panic!("Invalid data_type name: {:?}", name),
        }
    }
}

impl ::std::fmt::Display for DataType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match self {
            &DataType::Blob => write!(f, "blob"),
            &DataType::Directory => write!(f, "directory"),
        }
    }
}

/*
impl<'a> ToCapnp<'a> for DataType {
    type Builder = ::common_capnp::DataType::Builder<'a>;

    fn to_capnp(self: &Self, build: &mut Self::Builder) {

    }
}

impl<'a> FromCapnp<'a> for DataType {
    type Reader = ::common_capnp::DataType::Reader<'a>;

    fn from_capnp(reader: &'a Self::Reader) -> Self {
        match reader {
            ::common_capnp::DataType::Blob => DataType::Blob,
        }
    }
}*/
