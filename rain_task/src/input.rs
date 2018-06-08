use super::*;
use librain::common::DataType;
use librain::governor::rpc::executor_serde::*;
use memmap::Mmap;
use std::sync::{Mutex, MutexGuard};
use std::{fmt, mem, str};

#[allow(dead_code)] // TODO: Remove when used
#[derive(Debug)]
enum InputState {
    SpecMem,
    SpecMemAndFile,
    NotOpen,
    MMap(File, Mmap),
}

/// One instance of input `DataObject`.
#[derive(Debug)]
pub struct DataInstance {
    pub spec: ObjectSpec,
    pub info: ObjectInfo,
    location: DataLocation,
    state: Mutex<InputState>,
    /// The absolute path to the existing (or potential) file or dir.
    /// NB: Must NOT be modified after DataInstance creation!
    path: PathBuf,
    order: usize,
}

impl fmt::Display for DataInstance {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Input #{} ({:?} ID {}, label {:?})",
            self.order,
            self.spec.data_type,
            self.spec.id,
            self.spec.label
        )
    }
}

impl DataInstance {
    pub(crate) fn new(obj: LocalObjectIn, work_dir: &Path, order: usize) -> Self {
        let location = obj.location.expect("bug: input needs a data location");
        let istate = match location {
            DataLocation::Cached => panic!("bug: cached object requested"),
            DataLocation::OtherObject(_) => panic!("bug: `OtherObject` location in input"),
            DataLocation::Memory(_) => InputState::SpecMem,
            DataLocation::Path(_) => InputState::NotOpen,
        };
        let path = if let DataLocation::Path(ref p) = &location {
            p.into()
        } else {
            work_dir.join(format!(
                "input-{}-{}",
                obj.spec.id.get_session_id(),
                obj.spec.id.get_id()
            ))
        };
        DataInstance {
            spec: obj.spec,
            info: obj.info.expect("bug: inputs needs the info attribute"),
            location: location,
            state: Mutex::new(istate),
            path: path,
            order: order,
        }
    }

    /// Get all the input bytes. In case the input is a file,
    /// it is mmap-ed the first time this is called.
    ///
    /// Note that every invocation locks the input mutex.
    ///
    /// Panics on any I/O error. Returns an error if the input is a directory.
    pub fn get_bytes<'a>(&'a self) -> TaskResult<&'a [u8]> {
        self.check_blob()?;
        // Make sure the lock guard is dropped before panicking
        Ok((|| -> Result<&'a [u8]> {
            let mut guard = self.state.lock().unwrap();
            if matchvar!(*guard, InputState::SpecMem)
                || matchvar!(*guard, InputState::SpecMemAndFile)
            {
                if let DataLocation::Memory(ref d) = self.location {
                    return Ok(d);
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
                return Ok(unsafe { mem::transmute::<&[u8], &'a [u8]>(mmap.as_ref()) });
            }
            unreachable!();
        })().expect("error reading input file"))
    }

    /// Get the path for the input file. If the input was memory backed, this
    /// will write the file to the filesystem the first time this is called.
    /// Note that even when written to disk, the data is also still kept in memory.
    ///
    /// Note that every invocation locks the input mutex.
    pub fn get_path(&self) -> PathBuf {
        {
            let guard = self.state.lock().unwrap();
            if matchvar!(*guard, InputState::SpecMem) {
                unimplemented!(); // TODO: Save the file to disk
            }
        }
        self.path.clone()
    }

    /// A shorthand to check that the input is a directory.
    ///
    /// Returns `Err(TaskError)` if not a directory.
    pub fn check_directory(&self) -> TaskResult<()> {
        if self.spec.data_type == DataType::Directory {
            Ok(())
        } else {
            bail!("Expected directory as input {}", self)
        }
    }

    /// A shorthand to check that the input is a file or data blob.
    ///
    /// Returns `Err(TaskError)` if not a blob.
    pub fn check_blob(&self) -> TaskResult<()> {
        if self.spec.data_type == DataType::Blob {
            Ok(())
        } else {
            bail!("Expected blob/file as input {}", self)
        }
    }

    /// Panics on any I/O error.
    ///
    /// Returns an error if the input is a directory or non-text content-type, or if
    /// the input is not valud utf-8. Any other encoding needs to be decoded manually.
    ///
    /// Note: checks for valid utf-8 on every call.
    pub fn get_str<'a>(&'a self) -> TaskResult<&'a str> {
        self.check_content_type("text")?;
        match str::from_utf8(self.get_bytes()?) {
            Err(e) => bail!(
                "Data supplied to {} are not utf-8 (as expected): {:?}",
                self,
                e
            ),
            Ok(s) => Ok(s),
        }
    }

    /// Check the input content-type.
    ///
    /// Return Ok if the actual type is a subtype or supertype of the given type.
    pub fn check_content_type(&self, _ctype: &str) -> TaskResult<()> {
        self.check_blob()?;
        // TODO: Actually check
        Ok(())
    }

    /// Get the content-type of the object.
    ///
    /// Returns "" for directories.
    pub fn get_content_type(&self) -> String {
        if self.spec.data_type != DataType::Blob {
            return "".into();
        }
        if self.info.content_type.len() > 0 {
            self.info.content_type.clone()
        } else {
            self.spec.content_type.clone()
        }
    }
}
