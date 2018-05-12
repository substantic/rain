use std::sync::{Mutex, MutexGuard};
use librain::worker::rpc::subworker_serde::*;
use memmap::Mmap;
use std::mem;
use super::*;

#[allow(dead_code)] // TODO: Remove when used
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
    pub(crate) fn new(spec: &'a DataObjectSpec, work_dir: &Path, order: usize) -> Self {
        let istate = match spec.location.as_ref().expect("bug: input needs a data location") {
            DataLocation::Cached => panic!("bug: cached object requested"),
            DataLocation::OtherObject(_) => panic!("bug: `OtherObject` location in input"),
            DataLocation::Memory(_) => InputState::SpecMem,
            DataLocation::Path(_) => InputState::NotOpen,
        };
        let path = if let DataLocation::Path(p) = spec.location.as_ref().unwrap() {
            p.into()
        } else {
            work_dir.join(format!("input-{}-{}", spec.id.get_session_id(), spec.id.get_id()))
        };
        DataInstance {
            spec: spec,
            state: Mutex::new(istate),
            path: path,
            order: order,
        }
    }

    /// Get all the input bytes. In case the input is a file,
    /// it is mmap-ed the first time this is called.
    /// 
    /// Every invocation locks the input mutex.
    /// Panics on any I/O error. Returns an error if the input is a directory.
    pub fn get_bytes(&self) -> TaskResult<&'a[u8]> {
        // TODO: Check this is not a dir

        // Make sure the lock guard is dropped before panicking
        Ok((|| -> Result<&'a[u8]> {
            let mut guard = self.state.lock().unwrap();
            if matchvar!(*guard, InputState::SpecMem)
                || matchvar!(*guard, InputState::SpecMemAndFile) {
                if let Some(DataLocation::Memory(d)) = self.spec.location.as_ref() {
                    return Ok(&d)
                }
                unreachable!();
            }
            if matchvar!(*guard, InputState::NotOpen) {
                let f = File::open(&self.path)?;
                let mmap = unsafe { Mmap::map(&f)? };
                *guard = InputState::MMap(f, mmap);
            }
            if let InputState::MMap(_, ref mmap) = *guard {
                // This is safe since the Mmap is not dealocated before the
                // containing Input<'a>.
                return Ok( unsafe { mem::transmute::<&[u8], &'a [u8]>(mmap.as_ref()) });
            }
            unreachable!();
        })().expect("error reading input file"))
    }

    /// Get the path for the input file. If the input was memory backed, this
    /// will write the file to the filesystem the first time this is called.
    /// Note that even when written to disk, the data is also still kept in memory.
    /// 
    /// Every invocation locks the input mutex.
    pub fn get_path(&self) -> PathBuf {
        {
            let guard = self.state.lock().unwrap();
            if matchvar!(*guard, InputState::SpecMem) {
                unimplemented!(); // TODO: Save the file to disk
            }
        }
        // This is safe since the PathBuf is never modified after creation.
        self.path.clone()
    }

    /// Panics on any I/O error.
    /// Returns an error if the input is a directory or non-text content-type, or if
    /// the input is not valud utf-8.
    /// TODO: Needs content-type checking
    pub fn get_as_str(&self) -> TaskResult<&'a str> {
        unimplemented!()
    }

    /// TODO: Needs attributes work
    pub fn get_content_type(&self) -> String {
        unimplemented!()
    }
}

