use std::fmt;
use std::ffi::OsString;
use std::sync::{Mutex, MutexGuard};
use std::mem::swap;
use std::fs::{OpenOptions, File};
use std::io::BufWriter;
use std::path::Path;
use std::io::Write;

use librain::common::id::{TaskId, DataObjectId, SubworkerId};
use librain::common::Attributes;
use librain::worker::rpc::subworker_serde::*;
use librain::common::id::SId;

use super::{Error, Result, MAX_MSG_SIZE};

#[derive(Debug)]
enum OutputState {
    Empty,
    MemBacked(Vec<u8>),
    FileBacked(BufWriter<File>),
    Path(String),
}

/// Represents one concrete output. The output can be either empty (as is initially),
/// set to represent an existing file, set to represent an existing directory, or written
/// to as a `Write`. These three are mutually exclusive, `set_dir_path` and `set_file_path`
/// may be used only once, and not before or after `get_writer`.
/// 
/// This object is thread-safe and the internal state is guarded by a mutex. Calling
/// `get_writer` locks this mutex and holds it until the returned guard is dropped. 
/// This means fast (lockless) writes to the `Write` but you need to make sure your
/// other threads do not starve or deadlock.
#[derive(Debug)]
pub struct Output<'a> {
    /// The original output description 
    desc: &'a DataObjectSpec,
    /// Mutex holding the output state
    data: Mutex<OutputState>,
    /// The resulting attributes. Initially empty.
    attributes: Attributes,
    /// Path for the resulting file or directory (unless `MemoryBacked`)
    path: PathBuf,
    /// Order of the output in outputs
    order: usize,
}


impl<'a> fmt::Display for Output<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref label) = self.desc.label {
            write!(f, "Output #{} (ID {}, label {:?})", self.order, self.desc.id, label)
        } else {
            write!(f, "Output #{} (ID {}, no label)", self.order, self.desc.id)
        }
    }
}

impl<'a> Output<'a> {
    /// Create an output from DataObjectSpec. Internal.
    fn new(spec: &'a DataObjectSpec) -> Self {
        Output {
            desc: spec,
            data: Mutex::new(OutputState::Empty),
            attributes: Attributes::new(),
            writer_path: format!("output-{}-{}", spec.id.get_session_id(), spec.id.get_id()),
        }
    }

    /// Consume self, yielding a `DataObjectSpec` for `ResultMsg` and
    /// a flag whether the output object was cached (only possible if requested).
    /// Currently, the subworker never caches.
    fn create_output_spec(self) -> (DataObjectSpec, bool) {
        (DataObjectSpec {
            id: self.desc.id,
            label: None,
            attributes: self.attributes,
            location: Some(match self.data.into_inner().unwrap() {
                OutputState::Empty => DataLocation::Memory(Vec::new()),
                OutputState::MemBacked(data) => DataLocation::Memory(data),
                OutputState::FileBacked(f) => DataLocation::Path(self.writer_path),
                OutputState::Path(p) => DataLocation::Path(p),
            }),
            cache_hint: false, 
        }, false)
    }

    pub fn set_dir_path<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path: &Path = path.as_ref();
        // TODO: Check for self directory type
        if !path.is_dir() {
            bail!("Path {:?} given to `set_dir_path` is not a readable directory.", path);
        }
        let mut guard = self.data.lock().unwrap();
        if !matchvar!(*guard, OutputState::Empty) {
            bail!("Called `set_dir_path` on {} after being previously set.", self.desc.id)
        }
        if let Some(s) = path.to_str() {
            *guard = OutputState::Path(s.into())
        } else {
            bail!("Can't convert path {:?} to a valid unicode string.", path);
        }
        Ok(())
    }

    pub fn set_file_path<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path: &Path = path.as_ref();
        // TODO: Check for self non-directory type
        if !path.is_file() {
            bail!("Path {:?} given to `set_file_path` is not a readable regular file.", path);
        }
        let mut guard = self.data.lock().unwrap();
        if !matchvar!(*guard, OutputState::Empty) {
            bail!("Called `set_file_path` on {} after being previously set or written to.", self.desc.id)
        }
        if let Some(s) = path.to_str() {
            *guard = OutputState::Path(s.into())
        } else {
            bail!("Can't convert path {:?} to a valid unicode string.", path);
        }
        Ok(())
    }

    pub fn get_content_type(&self) -> Result<&'a str> {
        unimplemented!()
    }

    pub fn set_content_type(&self, ct: &str) -> Result<()> {
        unimplemented!()
    }

    pub fn get_writer<'b: 'a>(&'b self) -> Result<OutputWriter<'b>> {
        // TODO: Check whether it is a non-directory type
        let mut guard = self.data.lock().unwrap();
        if matchvar!(*guard, OutputState::Empty) {
            *guard = OutputState::MemBacked(Vec::new())
        }
        if matchvar!(*guard, OutputState::MemBacked(_)) ||
            matchvar!(*guard, OutputState::FileBacked(_)) {
            Ok(OutputWriter::new(guard, self.desc.id))
        } else {
            bail!("Cannot get writer for Output {:?} with already submitted file or dir path.",
                self.desc.id)
        }
    }
}

#[derive(Debug)]
pub struct OutputWriter<'a> {
    guard: MutexGuard<'a, OutputState>,
    path: &'a str,
}

impl<'a> OutputWriter<'a> {
    fn new(guard: MutexGuard<'a, OutputState>, path: &'a str) -> Self {
        OutputWriter { guard: guard, path: path }
    }

    fn convert_to_file(&mut self) -> ::std::io::Result<()> {
        let mut f = BufWriter::new(OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(path)?);
        if let OutputState::MemBacked(ref data) = *self.guard {
            f.write_all(data)?;
        } else {
            panic!("bug: invalid state for convert_to_file");
        }
        let mut os = OutputState::FileBacked(f);
        swap(&mut os, &mut *self.guard);
        Ok(())
    }

    pub fn ensure_file_based(&mut self) -> Result<()> {
        if matchvar!(*self.guard, OutputState::MemBacked(_)) {
            self.convert_to_file()?;
        }
        Ok(())
    }
}

impl<'a> Write for OutputWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> ::std::io::Result<usize> {
        // Should be Some() only for MemBacked
        let mut data_len = None;
        if let OutputState::MemBacked(ref data) = *self.guard {
            data_len = Some(data.len());
        }
        if let Some(len) = data_len {
            if len + buf.len() > MEM_BACKED_LIMIT {
                self.convert_to_file()?;
            }
        }
        match *self.guard {
            OutputState::MemBacked(ref mut data) => {
                data.write(buf).into()
            },
            OutputState::FileBacked(ref mut f) => {
                f.write(buf).into()
            },
            _ => {
                panic!("bug: invalid OutputState in OutputWriter")
            }
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let OutputState::FileBacked(ref mut f) = *self.guard {
            f.flush().into()
        } else {
            Ok(())
        }
    }
}
