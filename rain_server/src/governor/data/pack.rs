use std::fs::File;
use std::sync::Arc;
use rain_core::{errors::*};

use super::super::State;
use super::{Data, Storage};

// Serialization function object into data stream

pub trait PackStream {
    fn read(&mut self, size: usize) -> (&[u8], bool);
}

// Create a new pack stream for given dataobject
pub fn new_pack_stream(state: &State, data: Arc<Data>) -> Result<Box<PackStream>> {
    let data_ref = data.clone();
    Ok(match data.storage() {
        &Storage::Memory(_) => Box::new(MemoryPackStream {
            data: data_ref,
            position: 0,
        }),
        &Storage::Path(_) if data.is_blob() && data.size() == 0 => Box::new(EmptyPackStream {
            dummy: Default::default(),
        }),
        &Storage::Path(ref p) if data.is_blob() => Box::new(MmapPackStream {
            position: 0,
            mmap: unsafe { ::memmap::Mmap::map(&File::open(&p.path)?) }?,
        }),
        &Storage::Path(ref p) => {
            // TODO: Make tar in different thread
            assert!(data.is_directory());
            let temp_file = state.work_dir().make_temp_file();
            {
                let file = temp_file.create()?;
                let mut tar_builder = ::tar::Builder::new(file);
                tar_builder.mode(::tar::HeaderMode::Deterministic);
                tar_builder.append_dir_all(".", &p.path)?;
                tar_builder.finish()?;
            }
            Box::new(MmapPackStream {
                position: 0,
                mmap: unsafe { ::memmap::Mmap::map(&temp_file.open()?) }?,
            })
        }
    })
}

struct EmptyPackStream {
    dummy: [u8; 0],
}

impl PackStream for EmptyPackStream {
    fn read(&mut self, _read_size: usize) -> (&[u8], bool) {
        (&self.dummy, true)
    }
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
    mmap: ::memmap::Mmap,
    position: usize,
}

impl PackStream for MmapPackStream {
    fn read(&mut self, read_size: usize) -> (&[u8], bool) {
        let start = self.position;
        let data_size = self.mmap.len();
        let (end, eof) = if start + read_size < data_size {
            (start + read_size, false)
        } else {
            (data_size, true)
        };
        self.position = end;
        (&self.mmap[start..end], eof)
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
