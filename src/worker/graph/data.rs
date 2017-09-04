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
    fn new(data_type: DataType, storage: Storage) -> Data {
        Data {
            data_type, storage
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

    #[inline]
    pub fn is_blob(&self) -> bool {
        match self.data_type {
            DataType::Blob => true,
            _ => false
        }
    }

}

/// Trait for building Data
pub trait DataBuilder {
    fn set_size(&mut self, size: usize);
    fn write(&mut self, data: &[u8]);
    fn build(&mut self) -> Data;
}

pub struct BlobBuilder {
    buffer: Vec<u8>
}

impl BlobBuilder {
    pub fn new() -> Self {
        BlobBuilder {
            buffer: Vec::new()
        }
    }

    pub fn write_blob(&mut self, data: &Data) {
        // TODO: Assert that data is blob
        match data.storage {
            Storage::Memory(ref bytes) => self.write(&bytes[..]),
            Storage::Path(ref path) => unimplemented!()
        }
    }
}

impl DataBuilder for BlobBuilder {

    fn set_size(&mut self, size: usize) {
        // If size bigger than a threadshold, create directly a file
        self.buffer.reserve(size);
    }

    fn write(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    fn build(&mut self) -> Data {
        Data::new(DataType::Blob, Storage::Memory(
            ::std::mem::replace(&mut self.buffer, Vec::new())))
    }
}
