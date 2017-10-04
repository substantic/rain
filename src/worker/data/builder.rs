
use super::data::{Data, Storage, DataType};

/// Trait for building Data from data stream
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
        match data.storage() {
            &Storage::Memory(ref bytes) => self.write(&bytes[..]),
            &Storage::Path(ref path) => unimplemented!()
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
