use std::fs::File;
use super::data::{Data, Storage};
use errors::Result;
use super::super::fs::workdir::WorkDir;
use common::DataType;

pub struct DataBuilder {
    buffer: Vec<u8>,
    data_type: DataType,
}

impl DataBuilder {
    pub fn new(_workdir: &WorkDir, data_type: DataType, expected_size: Option<usize>) -> Self {
        let mut buffer = Vec::new();
        // TODO: If size is to big, redirect to disk
        if let Some(size) = expected_size {
            buffer.reserve(size);
        }
        DataBuilder { data_type, buffer }
    }

    // TODO: Get rid of this method
    pub fn write_blob(&mut self, data: &Data) -> Result<()> {
        assert!(self.data_type == DataType::Blob && data.is_blob());
        match data.storage() {
            &Storage::Memory(ref bytes) => self.write(&bytes[..]),
            &Storage::Path(ref path) => {
                let mem = unsafe { ::memmap::Mmap::map(&File::open(&path.path)?) }?;
                self.write(&mem);
            }
        }
        Ok(())
    }

    pub fn write(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub fn build(&mut self) -> Data {
        Data::new(
            Storage::Memory(::std::mem::replace(&mut self.buffer, Vec::new())),
            self.data_type,
        )
    }
}
