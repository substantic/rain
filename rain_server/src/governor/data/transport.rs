use std::fs::File;
use std::sync::Arc;
use rain_core::{errors::*};

use super::super::State;
use super::{Data, Storage};

pub enum TransportView {
    Empty([u8; 0]),
    Memory(Arc<Data>),
    Mmap(::memmap::Mmap),
}

impl TransportView {
    pub fn from(state: &State, data: &Arc<Data>) -> Result<Self> {
        Ok(match data.storage() {
            &Storage::Memory(_) => TransportView::Memory(data.clone()),
            &Storage::Path(_) if data.is_blob() && data.size() == 0 => {
                TransportView::Empty(Default::default())
            }
            &Storage::Path(ref p) if data.is_blob() => {
                TransportView::Mmap(unsafe { ::memmap::Mmap::map(&File::open(&p.path)?) }?)
            }
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
                TransportView::Mmap(unsafe { ::memmap::Mmap::map(&temp_file.open()?) }?)
            }
        })
    }

    pub fn get_slice(&self) -> &[u8] {
        match self {
            &TransportView::Memory(ref data) => {
                if let &Storage::Memory(ref mem) = data.storage() {
                    &mem[..]
                } else {
                    unreachable!()
                }
            }
            &TransportView::Mmap(ref m) => &m[..],
            &TransportView::Empty(ref e) => e,
        }
    }
}
