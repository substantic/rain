use librain::common::id::{TaskId, DataObjectId, SubworkerId};
use librain::worker::rpc::subworker_serde::*;

use std::env;
use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use super::*;

/// Alias type for a subworker task function with arbitrary number of inputs
/// and outputs.
pub type TaskFn = Fn(&Context, &[DataInstance], &[Output]) -> Result<()>;

pub struct Subworker {
    subworker_id: SubworkerId,
    subworker_type: String,
    socket_path: PathBuf,
    tasks: HashMap<String, Box<TaskFn>>,
    working_path: PathBuf,
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
            tasks: HashMap::new(),
            working_path: env::current_dir().unwrap(),
        }
    }

    pub fn add_task<S, F>(&mut self, task_name: S, task_fun: F)
        where S: Into<String>, F: 'static + Fn(&Context, &[DataInstance], &[Output]) -> Result<()> {
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
                WorkerToSubworkerMessage::Call(call_msg) => {
                    let reply = self.handle_call(call_msg)?;
                    sock.write_msg(&SubworkerToWorkerMessage::Result(reply))?;
                },
                WorkerToSubworkerMessage::DropCached(drop_msg) => {
                    if !drop_msg.drop.is_empty() {
                        bail!("received nonempty dropCached request with no cached objects");
                    }
                },
            }
        }
    }

    fn register(&mut self, sock: &mut UnixStream) -> Result<()> {
        let msg = SubworkerToWorkerMessage::Register(RegisterMsg {
            protocol: MSG_PROTOCOL.into(),
            subworker_id: self.subworker_id,
            subworker_type: self.subworker_type.clone(),
        });
        sock.write_msg(&msg)
    }

    fn handle_call(&mut self, call_msg: CallMsg) -> Result<ResultMsg> {
        // TODO: Handle workdir path
        let work_dir: PathBuf = "TODO".into();

        let mut context = Context::for_call_msg(&call_msg, &work_dir)?;
        match self.tasks.get(&call_msg.method) { 
            None => bail!("Task {} not found", call_msg.method),
            Some(f) => {
                env::set_current_dir(&context.work_dir)?;
                let res = f(&context, &context.inputs, &context.outputs);
                env::set_current_dir(&self.working_path)?;
                if let Err(e) = res {
                    context.success = false;
                    context.attributes.set("error", format!("error returned from {:?}:\n{}", call_msg.method, e)).unwrap();
                }
                // TODO: cleanup of the task working dir
            },
        }
        Ok(context.into_result_msg())
    }
/*
    #[allow(dead_code)]
    pub(crate) fn run_task_test<S: Into<String>>(&mut self, task_name: S) -> Result<()> {
        let key: String = task_name.into();
        match self.tasks.get(&key) {
            Some(f) => {
                let ins = vec![];
                let mut outs = vec![];
                let mut ctx = Context::for_call_msg(); 
                f(&mut ctx, &ins, &mut outs)
            },
            None => bail!("Task {} not found", key)
        }
    }*/
}
