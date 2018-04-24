extern crate librain;
extern crate byteorder;
#[macro_use]
extern crate log;
#[macro_use]
extern crate error_chain;

use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::os::unix::net::UnixStream;
use std::io::ErrorKind;
use std::io::{Read, Write};
use std::default::Default;

pub type SubworkerId = u32;
#[derive(Debug, Default)]
struct Attributes;

pub struct Subworker {
    subworker_id: SubworkerId,
    socket_path: PathBuf,
    tasks: HashMap<String, Box<TaskFn>>,
}

#[allow(unused_doc_comment)]
pub mod errors {
    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain!{
        types {
            Error, ErrorKind, ResultExt;
        }
        foreign_links {
            Io(::std::io::Error);
            Utf8Err(::std::str::Utf8Error);
        }
    }
    // Explicit alias just to make the IDEs happier
    pub type Result<T> = ::std::result::Result<T, Error>;
}

pub use errors::*;

impl Subworker {
    pub fn new() -> Self {
        Subworker::with_params( 
            env::var("RAIN_SUBWORKER_ID")
                .expect("Env variable RAIN_SUBWORKER_ID required")
                .parse()
                .expect("Error parsing RAIN_SUBWORKER_ID"),
            env::var_os("RAIN_SUBWORKER_SOCKET")
                .expect("Env variable RAIN_SUBWORKER_SOCKET required")
                .into())
    }

    pub fn with_params(subworker_id: SubworkerId, socket_path: PathBuf) -> Self {
        Subworker { 
            subworker_id: subworker_id,
            socket_path: socket_path,
            tasks: HashMap::new()
        }
    }

    pub fn add_task<S, F>(&mut self, task_name: S, task_fun: F)
        where S: Into<String>, F: 'static + Fn(&mut Context, &[DataInstance], &mut [Output]) -> Result<()> {
        let key: String = task_name.into();
        if self.tasks.contains_key(&key) {
            panic!("can't add task {:?}: already present", &key);
        }
        self.tasks.insert(key, Box::new(task_fun));
    }

    pub fn run(&mut self) -> Result<()> {
        info!("Connecting to worker at socket {:?} with ID {}", self.socket_path, self.subworker_id);
        let mut sock = UnixStream::connect(&self.socket_path)?;
        self.register(&mut sock)?;
        loop {
            match sock.read_u32::<LittleEndian>() {
                Ok(len) => {

                },
                Err(ref e) if e.kind() == ErrorKind::ConnectionAborted => {
                    info!("Connection closed, shutting down");
                    return Ok(());
                }
                Err(e) => {
                    Err(e)?;
                }
            }
        }
    }

    fn register(&mut self, sock: &mut UnixStream) -> Result<()> {
        sock.write_all(&[])?; // TODO
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn run_task_test<S: Into<String>>(&mut self, task_name: S) -> Result<()> {
        let key: String = task_name.into();
        match self.tasks.get(&key) {
            Some(f) => {
                let ins = vec![];
                let mut outs = vec![];
                let mut ctx: Context = Default::default(); 
                f(&mut ctx, &ins, &mut outs)
            },
            None => bail!("Task {} not found", key)
        }
    }
}

#[derive(Debug, Default)]
pub struct Context {
    num_inputs: usize,
    num_outputs: usize,
    attributes: Attributes,
}

#[derive(Debug)]
pub struct DataInstance {
}

#[derive(Debug)]
pub struct Output {

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
        let mut s = Subworker::with_params(42, "/tmp/sock".into());
        s.add_task("task1", task1);
        s.add_task("task2", |_ctx, _ins, _outs| Ok(()));
        //s.add_task2("task1b", task1).unwrap();
        //add_task!(s, "task1a", task3, I I O).unwrap();
        //s.add_task2("task2b", |i: &[u8]| vec![1u8] ).unwrap();
        s.run_task_test("task1").unwrap();
        s.run_task_test("task2").unwrap();
    }
}
