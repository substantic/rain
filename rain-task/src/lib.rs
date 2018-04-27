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

pub const MAX_MSG_SIZE: usize = 128 * 1024 * 1024;
pub const MSG_PROTOCOL: &str = "v1cbor";
pub const MEM_BACKED_LIMIT: usize = 128 * 1024;

macro_rules! matchvar {
    ($ex: expr, $pat: pat) => {
        { if let $pat = $ex { true } else { false } }
    };
}

mod framing;
use framing::*;

mod errors;
pub use errors::*;

mod subworker;
pub use subworker::*;

mod output;
pub use output::*;


#[derive(Debug, Default)]
pub struct Context {
    num_inputs: usize,
    num_outputs: usize,
    /// Task attributes
    attributes: Attributes,
    /// Absolute path to task working dir
    work_dir: PathBuf,
    /// Absolute path to staging dir with input and output objects
    stage_dir: PathBuf,
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
