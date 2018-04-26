#![allow(unused_imports)]

extern crate librain;
extern crate byteorder;
#[macro_use]
extern crate log;
#[macro_use]
extern crate error_chain;
extern crate serde_cbor;


use std::collections::HashMap;
use std::path::PathBuf;
use std::os::unix::net::UnixStream;
use std::io;
use std::default::Default;
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

mod framing;
pub use framing::*;

mod errors;
pub use errors::*;

mod subworker;
pub use subworker::*;

const MEM_BACKED_LIMIT: usize = 128 * 1024;

#[derive(Debug, Default)]
pub struct Context {
    num_inputs: usize,
    num_outputs: usize,
    attributes: Attributes,
}

#[derive(Debug)]
pub struct DataInstance<'a> {
    desc: &'a DataObjectSpec,
    data: Mutex<Option<&'a[u8]>>,
}

impl<'a> DataInstance<'a> {
    pub fn get_bytes(&self) -> Result<&'a[u8]> {
        unimplemented!()
    }
    pub fn get_path(&self) -> Result<&'a Path> {
        unimplemented!()
    }
    pub fn get_str(&self) -> Result<&'a str> {
        unimplemented!()
    }
    pub fn get_content_type(&self) -> Result<&'a[u8]> {
        unimplemented!()
    }   
}

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
    /// Path for the resulting file for Writer (if a file will be used)
    writer_path: String,
    /// Order of the output in outputs
    order: usize,
}

//use std::mem::discriminant;
macro_rules! matchvar {
    ($ex: expr, $pat: pat) => {
        { if let $pat = $ex { true } else { false } }
    };
}

use std::fmt;

impl<'a> fmt::Display for Output<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref label) = self.desc.label {
            write!(f, "Output #{} (ID {}, label {:?})", self.order, self.desc.id, label)
        } else {
            write!(f, "Output #{} (ID {}, no label)", self.order, self.desc.id)
        }
    }
}

use std::ffi::OsString;
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
//                    .expect("output file name not valid utf-8")),
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
    object_id: DataObjectId,
}

impl<'a> OutputWriter<'a> {
    fn new(guard: MutexGuard<'a, OutputState>, object_id: DataObjectId) -> Self {
        OutputWriter { guard: guard, object_id: object_id }
    }

    fn convert_to_file(&mut self) -> std::io::Result<()> {
        let fname = format!("output-{}-{}", self.object_id.get_session_id(), self.object_id.get_id());
        let mut f = BufWriter::new(OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&fname)?);
        if let OutputState::MemBacked(ref data) = *self.guard {
            f.write_all(data)?;
        } else {
            panic!("bug: invalid state for convert_to_file");
        }
        let mut os = OutputState::FileBacked(f);
        swap(&mut os, &mut *self.guard);
        Ok(())
    }
}

impl<'a> Write for OutputWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
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

pub type TaskFn = Fn(&mut Context, &[DataInstance], &mut [Output]) -> Result<()>;

/*
macro_rules! count_params {
    ($icnt: ident, $ocnt: ident) => ();
    ($icnt: ident, $ocnt: ident, I $($params: tt)*) => { $icnt += 1; };
    ($icnt: ident, $ocnt: ident, O $($params: tt)*) => { $ocnt += 1; };
}

macro_rules! index_params {
    ($ins: ident, $outs: ident, $iidx: expr, $oidx: expr) => {};
    ($ins: ident, $outs: ident, $iidx: expr, $oidx: expr, I $($params: tt)*) => {
        $ins[$iidx], index_params!($ins, $outs, 1 + $iidx, $oidx, $($params:tt)*)
    };
    ($ins: ident, $outs: ident, $iidx: expr, $oidx: expr, O $($params: tt)*) => {
        $outs[$oidx], index_params!($ins, $outs, $iidx, 1 + $oidx, $($params:tt)*)
    };
}

macro_rules! add_task {
    ($subworker: expr, $name: expr, $taskfn: expr, $($params: tt)*) => ({
        $subworker.add_task($name, |ctx: &mut Context, ins: &[DataInstance], outs: &mut [Output]| {
            let mut icnt = 0u32; let mut ocnt = 0u32;
            count_params!(icnt, ocnt, $($params: tt)*);
            ctx.check_input_count(icnt)?;
            ctx.check_output_count(ocnt)?;
            $taskfn(ctx, index_params!(ins, outs, 0, 0, $($params: tt)*))
        })
    });
}
*/

#[cfg(test)]
mod tests {
    use super::*;

    fn task1(_ctx: &mut Context, _inputs: &[DataInstance], _outputs: &mut [Output]) -> Result<()>
    {
        Ok(())
    }

    fn take_closure(f: Box<Fn()>) {
    }

    #[test]
    fn it_works() {
        let a = "asdf";
        take_closure(Box::new(move || {println!("works: {}", a);} ))
    }

    fn task3(ctx_: &mut Context, in1: &DataInstance, in2: &DataInstance, out: &mut Output) -> Result<()> {
        Ok(())
    }

    #[test]
    fn session_add() {
        let mut s = Subworker::with_params("dummy", 42, "/tmp/sock".into());
        s.add_task("task1", task1);
        s.add_task("task2", |_ctx, _ins, _outs| Ok(()));
        //s.add_task2("task1b", task1).unwrap();
        //add_task!(s, "task1a", task3, I I O).unwrap();
        //s.add_task2("task2b", |i: &[u8]| vec![1u8] ).unwrap();
        s.run_task_test("task1").unwrap();
        s.run_task_test("task2").unwrap();
    }
}
