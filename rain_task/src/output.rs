use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::{fmt, fs, mem};

use librain::common::id::SId;
use librain::common::id::{DataObjectId, SubworkerId, TaskId};
use librain::common::Attributes;
use librain::common::DataType;
use librain::worker::rpc::subworker_serde::*;

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

/// One instance of output `DataObject`.
///
/// The output can be either empty (as is initially),
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
    spec: &'a DataObjectSpec,
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
        let label = self.spec
            .label
            .as_ref()
            .map(|s| s as &str)
            .unwrap_or("none");
        write!(
            f,
            "Output #{} ({:?} ID {}, label {:?})",
            self.order,
            self.get_data_type(),
            self.spec.id,
            label
        )
    }
}

impl<'a> Output<'a> {
    /// Create an output from DataObjectSpec. Internal.
    pub(crate) fn new(spec: &'a DataObjectSpec, stage_path: &Path, order: usize) -> Self {
        Output {
            spec: spec,
            data: OutputState::Empty,
            attributes: Attributes::new(),
            path: stage_path.join(format!(
                "output-{}-{}",
                spec.id.get_session_id(),
                spec.id.get_id()
            )),
            order: order,
        }
    }

    /// Consume self, yielding a `DataObjectSpec` for `ResultMsg` and
    /// a flag whether the output object was cached (only possible if requested).
    /// Currently, this subworker never caches.
    ///
    /// NOTE: The returned path may be still an open file until this Output is dropped.
    pub(crate) fn into_output_spec(self) -> (DataObjectSpec, bool) {
        (
            DataObjectSpec {
                id: self.spec.id,
                label: None,
                attributes: self.attributes,
                location: Some(match self.data {
                    OutputState::Empty => DataLocation::Memory(Vec::new()),
                    OutputState::MemBacked(data) => DataLocation::Memory(data),
                    OutputState::FileBacked(f) => {
                        drop(f);
                        DataLocation::Path(self.path)
                    }
                    OutputState::StagedPath => DataLocation::Path(self.path),
                    OutputState::OtherObject(id) => DataLocation::OtherObject(id),
                }),
                cache_hint: false,
            },
            false,
        )
    }

    /// Submit the given directory as the output contents.
    /// Moves the directory to the staging area.
    /// You should make sure no files in the directory are open after this operation.
    ///
    /// Panics if the output was submitted to and on I/O errors.
    /// Returns an error if the output is not a directory type.
    pub fn stage_directory<P: AsRef<Path>>(&mut self, path: P) -> TaskResult<()> {
        self.check_directory()?;
        let path: &Path = path.as_ref();
        if !path.is_dir() {
            panic!(
                "Path {:?} given to `stage_directory` on {} is not a readable directory.",
                path, self
            );
        }
        if !matchvar!(self.data, OutputState::Empty) {
            panic!(
                "Called `stage_directory` on {} after being previously staged.",
                self
            )
        }
        fs::rename(path, &self.path).unwrap_or_else(|_| {
            panic!(
                "error moving directory {:?} to staging ({:?}) on {}",
                path, self.path, self
            )
        });
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
        self.check_blob()?;
        let path: &Path = path.as_ref();
        if !path.is_file() {
            panic!(
                "Path {:?} given to `stage_file` on {} is not a readable regular file.",
                path, self
            );
        }
        if !matchvar!(self.data, OutputState::Empty) {
            panic!(
                "Called `stage_file` on {} after being previously staged or written to.",
                self
            )
        }
        fs::rename(path, &self.path).unwrap_or_else(|_| {
            panic!(
                "error moving directory {:?} to staging ({:?}) on {}",
                path, self.path, self
            )
        });
        self.data = OutputState::StagedPath;
        Ok(())
    }

    /// Set the output to a given input data object.
    /// No data is copied in this case and the worker is informed of the pass-through.
    /// The input *must* belong to the same task (this is not checked).
    /// Not allowed if the output was submitted or written to.
    pub fn stage_input(&mut self, object: &DataInstance) -> TaskResult<()> {
        if self.get_data_type() != object.get_data_type() {
            bail!("Can't stage input {} as output {}: data type mismatch.")
        }
        if !matchvar!(self.data, OutputState::Empty) {
            panic!(
                "Called `stage_input` on {} after being previously staged or written to.",
                self
            )
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

    /// A shorthand to check that the input is a directory.
    ///
    /// Returns `Err(TaskError)` if not a directory.
    pub fn check_directory(&self) -> TaskResult<()> {
        if self.get_data_type() == DataType::Directory {
            Ok(())
        } else {
            bail!("The output {} expects a directory.", self)
        }
    }

    /// A shorthand to check that the input is a file or data blob.
    ///
    /// Returns `Err(TaskError)` if not a blob.
    pub fn check_blob(&self) -> TaskResult<()> {
        if self.get_data_type() == DataType::Blob {
            Ok(())
        } else {
            bail!("The output {} expects a file or a data blob.", self)
        }
    }

    /// Return the object `DataType`.
    pub fn get_data_type(&self) -> DataType {
        let dt: String = self.spec
            .attributes
            .get::<String>("type")
            .expect("error parsing data_type");
        DataType::from_attribute(&dt)
    }

    /// Get the content-type of the object.
    ///
    /// Returns the type set in the subworker if any, or the type in the spec.
    /// Returns "" for directories.
    pub fn get_content_type(&self) -> String {
        if self.get_data_type() != DataType::Blob {
            return "".into();
        }
        let info = self.attributes.get::<HashMap<String, String>>("info");
        if let Ok(info2) = info {
            if let Some(s) = info2.get("content_type") {
                return s.clone();
            }
        }
        let spec = self.spec
            .attributes
            .get::<HashMap<String, String>>("spec")
            .unwrap();
        if let Some(s) = spec.get("content_type") {
            return s.clone();
        }
        "".into()
    }

    /// Sets the content type of the object.
    ///
    /// Returns an error for directories, incompatible content types and if it has been already set.
    pub fn set_content_type(&mut self, ctype: &str) -> TaskResult<()> {
        self.check_blob()?;
        // TODO: Check the content type compatibility
        let mut info = self.attributes
            .get::<HashMap<String, String>>("info")
            .unwrap_or_else(|_| HashMap::new());
        if let Some(ref s) = info.get("content_type") {
            bail!(
                "The content type of {} has been already set to {:?} (trying to set to {:?})",
                self,
                s,
                ctype
            );
        }
        info.insert("content_type".into(), ctype.into());
        self.attributes.set("info", info).unwrap();
        Ok(())
    }

    /// Convert a MemBacked ouptut (not Empty) to a FileBacked output.
    fn convert_to_file(&mut self) -> ::std::io::Result<()> {
        assert!(matchvar!(self.data, OutputState::MemBacked(_)));
        let mut f = BufWriter::new(OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&self.path)?);
        if let OutputState::MemBacked(ref data) = self.data {
            f.write_all(data)?;
        } else {
            unreachable!();
        }
        self.data = OutputState::FileBacked(f);
        Ok(())
    }

    /// If the output is empty or backed by memory, it is converted to a file.
    /// Does nothing if already backed by a file. Returns an error for
    /// staged files and inputs.
    pub fn make_file_backed(&mut self) -> TaskResult<()> {
        self.check_blob()?;
        if matchvar!(self.data, OutputState::Empty) {
            self.data = OutputState::MemBacked(Vec::new());
        }
        Ok(match self.data {
            OutputState::MemBacked(_) => self.convert_to_file()
                .expect("error writing output to file"),
            OutputState::FileBacked(_) => (),
            _ => {
                panic!(
                    "can't make output {} file backed: it has been staged with input, file or dir",
                    self
                );
            }
        })
    }
}

impl<'a> Write for Output<'a> {
    fn write(&mut self, buf: &[u8]) -> ::std::io::Result<usize> {
        if matchvar!(self.data, OutputState::Empty) {
            self.data = OutputState::MemBacked(Vec::new());
        }
        if matchvar!(self.data, OutputState::MemBacked(_)) {
            let overflow = if let OutputState::MemBacked(ref data) = self.data {
                data.len() + buf.len() > MEM_BACKED_LIMIT
            } else {
                false
            };
            if overflow {
                self.convert_to_file()?;
            }
        }
        match self.data {
            OutputState::MemBacked(ref mut data) => data.write(buf).into(),
            OutputState::FileBacked(ref mut f) => f.write(buf).into(),
            _ => panic!("can't write to output {} that has been staged with input, file or dir"),
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
