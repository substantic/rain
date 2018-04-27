use std::{fmt, fs, mem};
use std::ffi::OsString;
use std::sync::{Mutex, MutexGuard};
use std::fs::{OpenOptions, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::io::Write;

use librain::common::id::{TaskId, DataObjectId, SubworkerId};
use librain::common::Attributes;
use librain::worker::rpc::subworker_serde::*;
use librain::common::id::SId;

use super::{Error, Result, MEM_BACKED_LIMIT};

#[derive(Debug)]
enum OutputState {
    Empty,
    MemBacked(Vec<u8>),
    FileBacked(BufWriter<File>),
    StagedPath,
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
    /// Path for the resulting file or directory if written to fs
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
    pub(crate) fn new(spec: &'a DataObjectSpec, stage_path: &Path, order: usize) -> Self {
        Output {
            desc: spec,
            data: Mutex::new(OutputState::Empty),
            attributes: Attributes::new(),
            path: stage_path.join(format!("output-{}-{}", spec.id.get_session_id(), spec.id.get_id())),
            order: order,
        }
    }

    /// Consume self, yielding a `DataObjectSpec` for `ResultMsg` and
    /// a flag whether the output object was cached (only possible if requested).
    /// Currently, this subworker never caches.
    /// 
    /// NOTE: The returned path may be still an open file until this Output is dropped.
    pub(crate) fn create_output_spec(self) -> (DataObjectSpec, bool) {
        (DataObjectSpec {
            id: self.desc.id,
            label: None,
            attributes: self.attributes,
            location: Some(match self.data.into_inner().unwrap() {
                OutputState::Empty => DataLocation::Memory(Vec::new()),
                OutputState::MemBacked(data) => DataLocation::Memory(data),
                OutputState::FileBacked(f) => { drop(f); DataLocation::Path(self.path) },
                OutputState::StagedPath => DataLocation::Path(self.path),
            }),
            cache_hint: false, 
        }, false)
    }

    /// Submit the given directory as the output contents.
    /// Moves the directory to the staging area.
    /// You should make sure no files in the directory are open after this operation.
    /// Not allowed if the output was submitted to.
    pub fn stage_directory<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path: &Path = path.as_ref();
        // TODO: Check for self directory type
        if !path.is_dir() {
            bail!("Path {:?} given to `stage_directory` is not a readable directory.", path);
        }
        let mut guard = self.data.lock().unwrap();
        if !matchvar!(*guard, OutputState::Empty) {
            bail!("Called `stage_directory` on {} after being previously staged.", self)
        }
        fs::rename(path, &self.path)?;
        *guard = OutputState::StagedPath;
        Ok(())
    }

    /// Submit the given file as the output contents.
    /// Moves the directory to the staging area.
    /// You should make sure no files in the directory are open after this operation.
    /// Not allowed if the output was submitted or written to.
    pub fn stage_file<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path: &Path = path.as_ref();
        // TODO: Check for self non-directory type
        if !path.is_file() {
            bail!("Path {:?} given to `stage_file` is not a readable regular file.", path);
        }
        let mut guard = self.data.lock().unwrap();
        if !matchvar!(*guard, OutputState::Empty) {
            bail!("Called `stage_file` on {} after being previously staged or written to.", self)
        }
        fs::rename(path, &self.path)?;
        *guard = OutputState::StagedPath;
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
            Ok(OutputWriter::new(guard, &self.path))
        } else {
            bail!("Cannot get writer for Output {:?} with already submitted file or dir path.",
                self.desc.id)
        }
    }
}

#[derive(Debug)]
pub struct OutputWriter<'a> {
    guard: MutexGuard<'a, OutputState>,
    path: &'a Path,
}

impl<'a> OutputWriter<'a> {
    fn new(guard: MutexGuard<'a, OutputState>, path: &'a Path) -> Self {
        OutputWriter { guard: guard, path: path }
    }

    fn convert_to_file(&mut self) -> ::std::io::Result<()> {
        let mut f = BufWriter::new(OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(self.path)?);
        if let OutputState::MemBacked(ref data) = *self.guard {
            f.write_all(data)?;
        } else {
            panic!("bug: invalid state for convert_to_file");
        }
        let mut os = OutputState::FileBacked(f);
        mem::swap(&mut os, &mut *self.guard);
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

    fn flush(&mut self) -> ::std::io::Result<()> {
        if let OutputState::FileBacked(ref mut f) = *self.guard {
            f.flush().into()
        } else {
            Ok(())
        }
    }
}
