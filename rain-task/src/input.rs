use std::sync::{Mutex, MutexGuard};
use librain::worker::rpc::subworker_serde::*;
use memmap::Mmap;
use std::mem;
use super::*;

#[derive(Debug)]
enum InputState {
    SpecMem,
    SpecMemAndFile,
    NotOpen,
    MMap(File, Mmap),
}

#[derive(Debug)]
pub struct DataInstance<'a> {
    pub(crate) spec: &'a DataObjectSpec,
    state: Mutex<InputState>,
    /// The absolute path to the existing (or potential) file or dir.
    /// NB: Must NOT be modified after DataInstance creation!
    path: PathBuf,
    order: usize,
}

impl<'a> DataInstance<'a> {
    pub(crate) fn new(spec: &'a DataObjectSpec, stage_path: &Path, order: usize) -> Self {
        let istate = match spec.location.as_ref().expect("bug: input needs a data location") {
            DataLocation::Cached => panic!("bug: cached object requested"),
            DataLocation::OtherObject(_) => panic!("bug: `OtherObject` location in input"),
            DataLocation::Memory(_) => InputState::SpecMem,
            DataLocation::Path(_) => InputState::NotOpen,
        };
        let path = if let DataLocation::Path(p) = spec.location.as_ref().unwrap() {
            stage_path.join(p)
        } else {
            stage_path.join(format!("input-{}-{}", spec.id.get_session_id(), spec.id.get_id()))
        };
        DataInstance {
            spec: spec,
            state: Mutex::new(istate),
            path: path,
            order: order,
        }
    }

    pub fn get_bytes(&self) -> Result<&'a[u8]> {
        // TODO: Check this is not a dir
        let mut guard = self.state.lock().unwrap();
        if matchvar!(*guard, InputState::SpecMem)
            || matchvar!(*guard, InputState::SpecMemAndFile) {
            if let Some(DataLocation::Memory(d)) = self.spec.location.as_ref() {
                return Ok(&d)
            } else {
                panic!("bug: spec suddenly does not contain memory location");
            }
        }
        if matchvar!(*guard, InputState::NotOpen) {
            let f = File::open(&self.path)?;
            let mmap = unsafe { Mmap::map(&f)? };
            *guard = InputState::MMap(f, mmap);
        }
        if let InputState::MMap(_, ref mmap) = *guard {
            // This is safe since the Mmap is not dealocated before the
            // containing Input<'a>.
            return Ok( unsafe { mem::transmute::<&[u8], &'a [u8]>(&*mmap) });
        }
        unreachable!();
    }

    pub fn get_path(&self) -> Result<&'a Path> {
        {
            let guard = self.state.lock().unwrap();
            if matchvar!(*guard, InputState::SpecMem) {
                unimplemented!(); // TODO: Save the file to disk
            }
        }
        // This is safe since the PathBuf is never modified after creation.
        return Ok( unsafe { mem::transmute::<&Path, &'a Path>(&self.path) });
    }

    pub fn get_str(&self) -> Result<&'a str> {
        unimplemented!()
    }

    pub fn get_content_type(&self) -> Result<&'a[u8]> {
        unimplemented!()
    }   
}

