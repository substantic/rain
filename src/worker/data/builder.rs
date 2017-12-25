
use std::fs::File;
use super::data::{Data, Storage};
use errors::Result;


pub struct DataBuilder {
    buffer: Vec<u8>,
}

impl DataBuilder {
    pub fn new() -> Self {
        DataBuilder { buffer: Vec::new() }
    }

    pub fn write_blob(&mut self, data: &Data) -> Result<()> {
        // TODO: Assert that data is blob
        match data.storage() {
            &Storage::Memory(ref bytes) => self.write(&bytes[..]),
            &Storage::Path(ref path) => {
                let mem = unsafe { ::memmap::Mmap::map(&File::open(&path.path)?) }?;
                self.write(&mem);
            }
        }
        Ok(())
    }

    pub fn set_size(&mut self, size: usize) {
        // If size bigger than a threadshold, create directly a file
        self.buffer.reserve(size);
    }

    pub fn write(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub fn build(&mut self) -> Data {
        Data::new(
            Storage::Memory(::std::mem::replace(&mut self.buffer, Vec::new())),
        )
    }
}
