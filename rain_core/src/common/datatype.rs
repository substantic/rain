#[derive(PartialEq, Eq, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DataType {
    #[serde(rename = "blob")]
    Blob,
    #[serde(rename = "dir")]
    Directory,
}

impl DataType {
    pub fn from_capnp(value: ::common_capnp::DataType) -> DataType {
        match value {
            ::common_capnp::DataType::Blob => DataType::Blob,
            ::common_capnp::DataType::Directory => DataType::Directory,
        }
    }
}

impl ::std::fmt::Display for DataType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match self {
            &DataType::Blob => write!(f, "blob"),
            &DataType::Directory => write!(f, "dir"),
        }
    }
}

impl ::std::default::Default for DataType {
    fn default() -> Self {
        DataType::Blob
    }
}
