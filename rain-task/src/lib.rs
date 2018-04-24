extern crate librain;
extern crate byteorder;
#[macro_use]
extern crate log;
#[macro_use]
extern crate error_chain;
extern crate rmp_serde;

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::os::unix::net::UnixStream;
use std::io;
use std::default::Default;


use librain::common::id::{TaskId, DataObjectId, SubworkerId};
use librain::common::Attributes;
use librain::worker::rpc::subworker_serde::*;

mod socket {
    use std::os::unix::net::UnixStream;
    use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
    use super::{Result, SubworkerMessage};
    use rmp_serde;
    use std::io::{Read, Write};

    pub const MAX_MSG_SIZE: usize = 128 * 1024 * 1024;
    pub const MSG_PROTOCOL: &str = "mp-1";

    pub(crate) trait SocketExt {
        fn write_frame(&mut self, &[u8]) -> Result<()>;
        fn read_frame(&mut self) -> Result<Vec<u8>>; 
        fn write_msg(&mut self, &SubworkerMessage) -> Result<()>;
        fn read_msg(&mut self) -> Result<SubworkerMessage>; 
    }

    impl SocketExt for UnixStream {
        fn write_msg(&mut self, m: &SubworkerMessage) -> Result<()> {
            let data = rmp_serde::to_vec_named(m)?;
            self.write_frame(&data)
        }

        fn read_msg(&mut self) -> Result<SubworkerMessage> {
            let data = self.read_frame()?;
            let msg = rmp_serde::from_slice::<SubworkerMessage>(&data)?;
            Ok(msg)
        }

        fn write_frame(&mut self, data: &[u8]) -> Result<()> {
            if data.len() > MAX_MSG_SIZE {
                bail!("write_frame: message too long ({} bytes of {} allowed)", data.len(), MAX_MSG_SIZE);
            }
            self.write_u32::<LittleEndian>(data.len() as u32)?;
            self.write_all(data)?;
            Ok(())
        }

        fn read_frame(&mut self) -> Result<Vec<u8>> {
            let len = self.read_u32::<LittleEndian>()? as usize;
            if len > MAX_MSG_SIZE {
                bail!("read_frame: message too long ({} bytes of {} allowed)", len, MAX_MSG_SIZE);
            }
            let mut data = vec![0; len];
            self.read_exact(&mut data)?;
            Ok(data)
        }
    }
}

use socket::*;


pub mod errors {
    use rmp_serde;
    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain!{
        types {
            Error, ErrorKind, ResultExt;
        }
        foreign_links {
            Io(::std::io::Error);
            DecodeMP(rmp_serde::decode::Error);
            EncodeMP(rmp_serde::encode::Error);
            Utf8Err(::std::str::Utf8Error);
        }
    }
    // Explicit alias just to make the IDEs happier
    pub type Result<T> = ::std::result::Result<T, Error>;
}

pub use errors::*;


pub struct Subworker {
    subworker_id: SubworkerId,
    subworker_type: String,
    socket_path: PathBuf,
    tasks: HashMap<String, Box<TaskFn>>,
}

impl Subworker {
    pub fn new(subworker_type: &str) -> Self {
        Subworker::with_params(
            subworker_type, 
            env::var("RAIN_SUBWORKER_ID")
                .expect("Env variable RAIN_SUBWORKER_ID required")
                .parse()
                .expect("Error parsing RAIN_SUBWORKER_ID"),
            env::var_os("RAIN_SUBWORKER_SOCKET")
                .expect("Env variable RAIN_SUBWORKER_SOCKET required")
                .into())
    }

    pub fn with_params(subworker_type: &str, subworker_id: SubworkerId, socket_path: PathBuf) -> Self {
        Subworker {
            subworker_type: subworker_type.into(),
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
        let res = self.run_wrap();
        // TODO: catch connection closed gracefully
        /*
        Err(Error(ErrorKind::Io(ref e), _)) if e.kind() == io::ErrorKind::ConnectionAborted => {
            info!("Connection closed, shutting down");
            return Ok(());
        }
        */
        res
    }

    fn run_wrap(&mut self) -> Result<()> {
        info!("Connecting to worker at socket {:?} with ID {}", self.socket_path, self.subworker_id);
        let mut sock = UnixStream::connect(&self.socket_path)?;
        self.register(&mut sock)?;
        loop {
            match sock.read_msg()? {
                SubworkerMessage::Call(call_msg) => {
                    let reply = self.handle_call(call_msg)?;
                    sock.write_msg(&SubworkerMessage::Result(reply))?;
                },
                SubworkerMessage::DropCached(drop_msg) => {
                    if !drop_msg.drop.is_empty() {
                        bail!("received nonempty dropCached request with no cached objects");
                    }
                },
                msg => {
                    bail!("received invalid message {:?}", msg);
                }
            }
        }
    }

    fn register(&mut self, sock: &mut UnixStream) -> Result<()> {
        let msg = SubworkerMessage::Register(RegisterMsg {
            protocol: MSG_PROTOCOL.into(),
            subworker_id: self.subworker_id,
            subworker_type: self.subworker_type.clone(),
        });
        sock.write_msg(&msg)
    }

    fn handle_call(&mut self, call_msg: CallMsg) -> Result<ResultMsg> {
        Ok(unimplemented!()) // TODO: Implement
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
