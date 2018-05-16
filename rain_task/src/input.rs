use super::*;
use librain::worker::rpc::subworker_serde::*;
use librain::common::DataType;
use memmap::Mmap;
use std::{mem, fmt, str};
use std::sync::{Mutex, MutexGuard};

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
pub struct DataInstance<'a> {
    pub(crate) spec: &'a DataObjectSpec,
    state: Mutex<InputState>,
    /// The absolute path to the existing (or potential) file or dir.
    /// NB: Must NOT be modified after DataInstance creation!
    path: PathBuf,
    order: usize,
}

impl<'a> fmt::Display for DataInstance<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let label = self.spec.label.as_ref().map(|s| s as &str).unwrap_or("none");
        write!(
            f,
            "Input #{} ({:?} ID {}, label {:?})",
            self.order, self.get_data_type(), self.spec.id, label
        )
    }
}

impl<'a> DataInstance<'a> {
    pub(crate) fn new(spec: &'a DataObjectSpec, work_dir: &Path, order: usize) -> Self {
        let istate = match spec.location
            .as_ref()
            .expect("bug: input needs a data location")
        {
            DataLocation::Cached => panic!("bug: cached object requested"),
            DataLocation::OtherObject(_) => panic!("bug: `OtherObject` location in input"),
            DataLocation::Memory(_) => InputState::SpecMem,
            DataLocation::Path(_) => InputState::NotOpen,
        };
        let path = if let DataLocation::Path(p) = spec.location.as_ref().unwrap() {
            p.into()
        } else {
            work_dir.join(format!(
                "input-{}-{}",
                spec.id.get_session_id(),
                spec.id.get_id()
            ))
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
    /// Note that every invocation locks the input mutex.
    ///
    /// Panics on any I/O error. Returns an error if the input is a directory.
    pub fn get_bytes(&self) -> TaskResult<&'a [u8]> {
        self.check_blob()?;
        // Make sure the lock guard is dropped before panicking
        Ok((|| -> Result<&'a [u8]> {
            let mut guard = self.state.lock().unwrap();
            if matchvar!(*guard, InputState::SpecMem)
                || matchvar!(*guard, InputState::SpecMemAndFile)
            {
                if let Some(DataLocation::Memory(d)) = self.spec.location.as_ref() {
                    return Ok(&d);
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

    /// Return the object `DataType`.
    pub fn get_data_type(&self) -> DataType {
        let dt = self.spec.attributes.get::<String>("type").expect("error parsing data_type");
        DataType::from_attribute(&dt)
    }

    /// A shorthand to check that the input is a directory.
    /// 
    /// Returns `Err(TaskError)` if not a directory.
    pub fn check_directory(&self) -> TaskResult<()> {
        if self.get_data_type() == DataType::Directory {
            Ok(())
        } else {
            bail!("Expected directory as input {}", self)
        }
    }

    /// A shorthand to check that the input is a file or data blob.
    /// 
    /// Returns `Err(TaskError)` if not a blob.
    pub fn check_blob(&self) -> TaskResult<()> {
        if self.get_data_type() == DataType::Blob {
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
    pub fn get_str(&self) -> TaskResult<&'a str> {
        self.check_content_type("text")?;
        match str::from_utf8(self.get_bytes()?) {
            Err(e) => bail!("Data supplied to {} are not utf-8 (as expected): {:?}", self, e),
            Ok(s) => Ok(s)
        }
    }

    /// Check the input content-type.
    /// 
    /// Return Ok if the actual type is a subtype or supertype of the given type.
    pub fn check_content_type(&self, ctype: &str) -> TaskResult<()> {
        self.check_blob()?;
        // TODO: Actually check
        Ok(())
    }

    /// Get the content-type of the object.
    /// 
    /// Returns "" for directories.
    pub fn get_content_type(&self) -> String {
        if self.get_data_type() != DataType::Blob {
            return "".into();
        }
        let info = self.spec.attributes.get::<HashMap<String, String>>("info").unwrap();
        if let Some(s) = info.get("content_type") {
            return s.clone();
        }
        let spec = self.spec.attributes.get::<HashMap<String, String>>("spec").unwrap();
        if let Some(s) = spec.get("content_type") {
            return s.clone();
        }
        "".into()
    }
}
