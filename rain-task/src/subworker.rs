use librain::common::id::{TaskId, DataObjectId, SubworkerId};
use librain::worker::rpc::subworker_serde::*;
use chrono;

use std::{env, fs};
use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use super::*;


pub const STAGING_DIR: &str = "staging";
pub const TASKS_DIR: &str = "tasks";

/// Alias type for a subworker task function with arbitrary number of inputs
/// and outputs.
pub type TaskFn = Fn(&Context, &[DataInstance], &[Output]) -> Result<()>;

pub struct Subworker {
    subworker_id: SubworkerId,
    subworker_type: String,
    socket_path: PathBuf,
    tasks: HashMap<String, Box<TaskFn>>,
    /// Subworker working directory
    working_dir: PathBuf,
    /// Subworker staging subdirectory
    staging_dir: PathBuf,
    /// Subworker subdirectory with individual tasks
    tasks_dir: PathBuf,
    /// Prevent running `Subworker::run` repeatedly
    was_run: bool,
    /// If true, failed task directories (but not outputs) are kept in "tasks/"
    keep_failed_tasks: bool,
}

impl Subworker {
    pub fn new(subworker_type: &str) -> Self {
        let id = env::var("RAIN_SUBWORKER_ID")
                .expect("Env variable RAIN_SUBWORKER_ID required")
                .parse()
                .expect("Error parsing RAIN_SUBWORKER_ID");
        let socket: PathBuf = env::var_os("RAIN_SUBWORKER_SOCKET")
                .expect("Env variable RAIN_SUBWORKER_SOCKET required")
                .into();
        let workdir = env::current_dir().unwrap();
        Subworker::with_params(subworker_type, id, &socket, &workdir)
    }

    pub fn with_params(subworker_type: &str, subworker_id: SubworkerId, socket_path: &Path, working_dir: &Path) -> Self {
        Subworker {
            subworker_type: subworker_type.into(),
            subworker_id,
            socket_path: socket_path.into(),
            tasks: HashMap::new(),
            staging_dir: working_dir.join(STAGING_DIR),
            tasks_dir: working_dir.join(TASKS_DIR),
            working_dir: working_dir.into(),
            was_run: false,
            keep_failed_tasks: false,
        }
    }

    pub fn add_task<S, F>(&mut self, task_name: S, task_fun: F)
        where S: Into<String>, F: 'static + Fn(&Context, &[DataInstance], &[Output]) -> Result<()> {
        let key: String = task_name.into();
        if self.tasks.contains_key(&key) {
            panic!("can't add task named {:?}: already present", &key);
        }
        self.tasks.insert(key, Box::new(task_fun));
    }

    pub fn run(&mut self) -> Result<()> {
        if self.was_run {
            panic!("Subworker::run may only be ran once");
        }
        self.was_run = true;
        // Prepare the directories
        if self.staging_dir.exists() || self.tasks_dir.exists() {
            bail!("Subworker must be ran in a clean directory (workdir: {:?})", self.working_dir);
        }
        fs::create_dir(&self.staging_dir)?;
        fs::create_dir(&self.tasks_dir)?;
        // Connect to socket
        info!("Connecting to worker at socket {:?} with ID {}", self.socket_path, self.subworker_id);
        if !self.socket_path.exists() {
            bail!("Socket file not found at {:?}", self.socket_path);
        }
        // Change directory to prevent too long socket pathnames
        env::set_current_dir(self.socket_path.parent().unwrap())?;
        let mut sock = UnixStream::connect(&self.socket_path.file_name().unwrap())?;
        env::set_current_dir(&self.working_dir)?;
        // Register and run the task loop, catching any errors
        let res = (|| {
            self.register(&mut sock)?;
            loop {
                match sock.read_msg()? {
                    WorkerToSubworkerMessage::Call(call_msg) => {
                        let reply = self.handle_call(call_msg)?;
                        sock.write_msg(&SubworkerToWorkerMessage::Result(reply))?;
                    },
                    WorkerToSubworkerMessage::DropCached(drop_msg) => {
                        if !drop_msg.objects.is_empty() {
                            bail!("received nonempty dropCached request with no cached objects");
                        }
                    },
                }
            }
        })();
        match res {
            Err(Error(ErrorKind::Io(ref e), _))
                if (e.kind() == io::ErrorKind::ConnectionAborted) ||
                   (e.kind() == io::ErrorKind::UnexpectedEof) => {
                info!("Connection closed, shutting down");
                return Ok(());
            },
            other => other
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
        let task_name = format!("{}-task-{}_{}", chrono::Local::now().format("%Y%m%d-%H%M%S"),
            call_msg.task.get_session_id(), call_msg.task.get_id());
        let task_dir = self.tasks_dir.join(task_name);
        let mut context = Context::for_call_msg(&call_msg, &self.staging_dir, &task_dir)?;
        match self.tasks.get(&call_msg.method) { 
            None => bail!("Task {} not found", call_msg.method),
            Some(f) => {
                fs::create_dir(&task_dir)?;
                env::set_current_dir(&task_dir)?;
                debug!("Calling {:?} in {:?}", call_msg.method, task_dir);
                let res = f(&context, &context.inputs, &context.outputs);
                env::set_current_dir(&self.working_dir)?;
                if let Err(ref e) = res {
                    debug!("Method {:?} in {:?} failed: {}", call_msg.method, task_dir, e);
                    context.success = false;
                    context.attributes.set("error", format!("error returned from call to {:?} (in {:?}):\n{}", call_msg.method, task_dir, e)).unwrap();
                    // Clean already staged/open outputs
                    for mut o in context.outputs.iter_mut() {
                        o.cleanup_failed_task()?;
                    }
                    if !self.keep_failed_tasks {
                        // cleanup of the task working dir
                        fs::remove_dir_all(task_dir)?;
                    }
                } else {
                    debug!("Method {:?} finished", call_msg.method);
                    // cleanup of the task working dir
                    fs::remove_dir_all(task_dir)?;
                }
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
