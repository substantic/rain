use std::io::Read;
use std::path::{PathBuf, Path};


struct DataOnPath {
    path: PathBuf,
    /// If data is directory than size is sum of sizes of all blobs in directory
    size: usize
}

enum Storage {
    Memory(Vec<u8>),
    Path(DataOnPath)
}

#[derive(Copy, Clone)]
pub enum DataType {
    Blob,
    Directory,
    Stream
}

pub struct Data {
    data_type: DataType,
    storage: Storage,
}


impl Data {

    /// Create Data from vector
    pub fn from_array(data_type: DataType, data: &[u8]) -> Data {
        Data {
            data_type,
            // TODO: If data is sufficiently big, then make directly file
            storage: Storage::Memory(data.into())
        }
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    pub fn from_file(data_type: DataType, path: &Path) -> Data {
        unimplemented!()
    }

    /// Return size of data in bytes
    /// If data is directory than size is sum of sizes of all blobs in directory
    pub fn size(&self) -> usize {
        match self.storage {
            Storage::Memory(ref data) => data.len(),
            Storage::Path(ref data) => data.size
        }
    }

    /// Map data object on a given path
    /// Caller is responsible for deleteion of the path
    /// It creates a symlink to real data or new file if data only in memory
    pub fn map_to_path(&self, path: &Path) {
        unimplemented!()
    }

}