
use std::sync::Arc;
use std::fs::File;
use errors::Result;
use super::{Data, Storage};

// Serialization function object into data stream

pub trait PackStream {
    fn read(&mut self, size: usize) -> (&[u8], bool);
}

// Create a new pack stream for given dataobject
pub fn new_pack_stream(data: Arc<Data>) -> Result<Box<PackStream>> {
    let data_ref = data.clone();
    Ok(match data.storage() {
        &Storage::Memory(_) => Box::new(MemoryPackStream {
            data: data_ref,
            position: 0,
        }),
        // TODO: Directory
        &Storage::Path(ref p) => Box::new(MmapPackStream {
            data: data_ref,
            position: 0,
            mmap: unsafe { ::memmap::Mmap::map(&File::open(&p.path)?) }?,
        }),
    })
}


struct MemoryPackStream {
    data: Arc<Data>,
    position: usize,
}

impl PackStream for MemoryPackStream {
    fn read(&mut self, read_size: usize) -> (&[u8], bool) {
        let start = self.position;
        let data_size = self.data.size();
        let (end, eof) = if start + read_size < data_size {
            (start + read_size, false)
        } else {
            (data_size, true)
        };

        if let &Storage::Memory(ref mem) = self.data.storage() {
            self.position = end;
            (&mem[start..end], eof)
        } else {
            unreachable!()
        }
    }
}

struct MmapPackStream {
    data: Arc<Data>,
    mmap: ::memmap::Mmap,
    position: usize,
}

impl PackStream for MmapPackStream {
    fn read(&mut self, read_size: usize) -> (&[u8], bool) {
        let start = self.position;
        let data_size = self.data.size();
        let (end, size, eof) = if start + read_size < data_size {
            (start + read_size, read_size, false)
        } else {
            (data_size, data_size - start, true)
        };
        (&self.mmap, eof)
    }
}



/*enum TransportStreamType {
    MemoryBlob,
    MMap(::memmap::Mmap)
}

struct TransportStream {
    data: Arc<Data>,
    transport_type: TransportStreamType,
    position: usize
}

impl TransportStream {
    pub fn new(data: Arc<Data>) -> Result<Self> {
        let transport_type = match data.storage {
            Storage::Memory(_) => TransportStreamType::MemoryBlob,
            Storage::Path(ref path) => TransportStreamType::MMap(
                ::memmap::Mmap::open_path(&path.path, ::memmap::Protection::Read)?)
        };
        Ok(TransportStream {
            position: 0, transport_type, data
        })
    }

    pub fn read(&mut self, size: usize) -> (&[u8], bool) {
        match self.transport_type {
            TransportStreamType::MemoryBlob
        }
    }
}*/
