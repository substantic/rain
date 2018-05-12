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

use super::*;

#[derive(Debug)]
enum OutputState {
    /// No output data written yet
    Empty,
    /// Small data only in memory
    MemBacked(Vec<u8>),
    /// Backed with an open file
    FileBacked(BufWriter<File>),
    /// Points to a staged file belonging to this output
    StagedPath,
    /// Other data object (may be only an input or output of this task)
    OtherObject(DataObjectId),
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
    data: OutputState,
    /// The resulting attributes. Initially empty.
    attributes: Attributes,
    /// Path for the resulting file or directory if written to fs (may not exist)
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
            data: OutputState::Empty,
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
    pub(crate) fn into_output_spec(self) -> (DataObjectSpec, bool) {
        (DataObjectSpec {
            id: self.desc.id,
            label: None,
            attributes: self.attributes,
            location: Some(match self.data {
                OutputState::Empty => DataLocation::Memory(Vec::new()),
                OutputState::MemBacked(data) => DataLocation::Memory(data),
                OutputState::FileBacked(f) => { drop(f); DataLocation::Path(self.path) },
                OutputState::StagedPath => DataLocation::Path(self.path),
                OutputState::OtherObject(id) => DataLocation::OtherObject(id),
            }),
            cache_hint: false, 
        }, false)
    }

    /// Submit the given directory as the output contents.
    /// Moves the directory to the staging area.
    /// You should make sure no files in the directory are open after this operation.
    /// 
    /// Panics if the output was submitted to and on I/O errors.
    /// Returns an error if the output is not a directory type.
    pub fn stage_directory<P: AsRef<Path>>(&mut self, path: P) -> TaskResult<()> {
        let path: &Path = path.as_ref();
        // TODO: Check for self directory type
        if !path.is_dir() {
            panic!("Path {:?} given to `stage_directory` on {} is not a readable directory.", path, self);
        }
        if !matchvar!(self.data, OutputState::Empty) {
            panic!("Called `stage_directory` on {} after being previously staged.", self)
        }
        fs::rename(path, &self.path).unwrap_or_else(|_|
            panic!("error moving directory {:?} to staging ({:?}) on {}", path, self.path, self));
        self.data = OutputState::StagedPath;
        Ok(())
    }

    /// Submit the given file as the output contents.
    /// Moves the file to the staging area.
    /// You should make sure that the file is not open after this operation.
    /// 
    /// Panics if the output was submitted or written to and on I/O errors.
    /// Returns an error if the output is not a file directory type.
    pub fn stage_file<P: AsRef<Path>>(&mut self, path: P) -> TaskResult<()> {
        let path: &Path = path.as_ref();
        // TODO: Check for self non-directory type
        if !path.is_file() {
            panic!("Path {:?} given to `stage_file` on {} is not a readable regular file.", path, self);
        }
        if !matchvar!(self.data, OutputState::Empty) {
            panic!("Called `stage_file` on {} after being previously staged or written to.", self)
        }
        fs::rename(path, &self.path).unwrap_or_else(|_|
            panic!("error moving directory {:?} to staging ({:?}) on {}", path, self.path, self));
        self.data = OutputState::StagedPath;
        Ok(())
    }

    /// Set the output to a given input data object.
    /// No data is copied in this case and the worker is informed of the pass-through.
    /// The input *must* belong to the same task (this is not checked).
    /// Not allowed if the output was submitted or written to.
    pub fn stage_input(&mut self, object: &DataInstance) -> TaskResult<()> {
        // TODO: Check for the same (dir/file) type of the input
        if !matchvar!(self.data, OutputState::Empty) {
            panic!("Called `stage_input` on {} after being previously staged or written to.", self)
        }
        self.data = OutputState::OtherObject(object.spec.id);
        Ok(())
    }

    /// Called when the task failed. Remove and forget any already-staged data including attributes.
    /// Panics on I/O error.
    pub(crate) fn cleanup_failed_task(&mut self) {
        let remove_path = match self.data {
            OutputState::FileBacked(_) | OutputState::StagedPath => true,
            _ => false,
        };
        self.data = OutputState::Empty; // Also closes any open file
        if remove_path {
            fs::remove_dir_all(&self.path).expect("error removing staged path on task failure");
        }
        self.attributes = Attributes::new();
    }

    /// TODO: To be resolved on attribute update.
    pub fn get_content_type(&self) -> Result<&'a str> {
        unimplemented!()
    }

    /// TODO: To be resolved on attribute update.
    pub fn set_content_type(&self, _ct: &str) -> Result<()> {
        unimplemented!()
    }

    /// Get a writer instance. Sets the 
    pub fn get_writer<'b: 'a>(&'b mut self) -> TaskResult<OutputWriter<'b>> {
        // TODO: Check whether it is a non-directory type
        if matchvar!(self.data, OutputState::Empty) {
            self.data = OutputState::MemBacked(Vec::new())
        }
        if matchvar!(self.data, OutputState::MemBacked(_)) ||
            matchvar!(self.data, OutputState::FileBacked(_)) {
            Ok(OutputWriter::new(&mut self.data, &self.path))
        } else {
            bail!("Cannot get writer for Output {:?} with already submitted file or dir path.",
                self.desc.id)
        }
    }
}

#[derive(Debug)]
pub struct OutputWriter<'a> {
    data: &'a mut OutputState,
    path: &'a Path,
}

impl<'a> OutputWriter<'a> {
    /// Create an output writer. The data state must be either `MemBacked` or `FileBacked`.
    fn new(data: &'a mut OutputState, path: &'a Path) -> Self {
        assert!(matchvar!(data, OutputState::MemBacked(_)) || matchvar!(data, OutputState::FileBacked(_)));
        OutputWriter { data, path }
    }

    /// Convert a MemBacked ouptut (not Empty) to a FileBacked output.
    fn convert_to_file(&mut self) -> ::std::io::Result<()> {
        assert!(matchvar!(self.data, OutputState::MemBacked(_)));
        let mut f = BufWriter::new(OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(self.path)?);
        if let OutputState::MemBacked(ref data) = self.data {
            f.write_all(data)?;
        } else {
            unreachable!();
        }
        *self.data = OutputState::FileBacked(f);
        Ok(())
    }

    /// If the output is backed by memory, it is converted to a file.
    /// Does nothing if already backed by a file.
    pub fn ensure_file_based(&mut self) -> Result<()> {
        if matchvar!(self.data, OutputState::MemBacked(_)) {
            self.convert_to_file()?;
        }
        Ok(())
    }
}

impl<'a> Write for OutputWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> ::std::io::Result<usize> {
        // Should be Some() only for MemBacked
        let mut data_len = None;
        if let OutputState::MemBacked(ref data) = self.data {
            data_len = Some(data.len());
        }
        if let Some(len) = data_len {
            if len + buf.len() > MEM_BACKED_LIMIT {
                self.convert_to_file()?;
            }
        }
        match self.data {
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
        if let OutputState::FileBacked(ref mut f) = self.data {
            f.flush().into()
        } else {
            Ok(())
        }
    }
}
