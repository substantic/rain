use chrono;
use librain::common::id::{DataObjectId, SubworkerId, TaskId};
use librain::worker::rpc::subworker_serde::*;

use super::*;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::{env, fs};

pub const STAGING_DIR: &str = "staging";
pub const TASKS_DIR: &str = "tasks";

/// Alias type for a subworker task function with arbitrary number of inputs
/// and outputs.
pub type TaskFn = Fn(&mut Context, &[DataInstance], &mut [Output]) -> TaskResult<()> + Send + Sync;
//pub trait TaskFn: Fn(&mut Context, &[DataInstance], &mut [Output]) -> TaskResult<()> + Send + Sync {}

/// The subworker event loop and the set of registered tasks.
pub struct Subworker {
    /// An identifier for the local worker
    subworker_id: SubworkerId,
    /// Any name given to the subworker type (denoting the group of provided tasks)
    subworker_type: String,
    /// Path to the socket (should be absolute)
    socket_path: PathBuf,
    /// Registered task functions
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
    /// Create a subworker based on env variables `RAIN_SUBWORKER_ID` and `RAIN_SUBWORKER_SOCKET`
    /// and working dir in the current directory. See also `Subworker::with_params`.
    ///
    /// Panics when either env variable is missing or invalid.
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

    /// Creates a Sbworker with the given attributes. Note that the attributes are only
    /// recorded at this point and no initialization is performed.
    pub fn with_params(
        subworker_type: &str,
        subworker_id: SubworkerId,
        socket_path: &Path,
        working_dir: &Path,
    ) -> Self {
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

    /// Register task function.
    ///
    /// The provided function must accept a list of inpts and outputs, expanding them manually.
    /// For functions accepting individual inputs and outputs, see the macro `register_task!`.
    /// The accepted function type is equivalent to `'static + TaskFn`.
    ///
    /// Panics when a task with the same name has been registered previously.
    pub fn register_task<S, F>(&mut self, task_name: S, task_fun: F)
    where
        S: Into<String>,
        F: 'static
            + Fn(&mut Context, &[DataInstance], &mut [Output]) -> TaskResult<()>
            + Send
            + Sync,
    {
        let key: String = task_name.into();
        if self.tasks.contains_key(&key) {
            panic!("can't add task named {:?}: already present", &key);
        }
        self.tasks.insert(key, Box::new(task_fun));
    }

    /// Run the subworker loop, connecting to the worker, registering and handling requests
    /// until the connection is closed. May be only called once.
    ///
    /// Panics on any error outside of the task functions. See `README.md` for rationale.
    pub fn run(&mut self) {
        if self.was_run {
            panic!("Subworker::run may only be ran once");
        }
        self.was_run = true;
        // Prepare the directories
        if self.staging_dir.exists() || self.tasks_dir.exists() {
            panic!(
                "Subworker must be ran in a clean directory (workdir: {:?})",
                self.working_dir
            );
        }
        fs::create_dir(&self.staging_dir).expect("error creating staging dir");
        fs::create_dir(&self.tasks_dir).expect("error creating tasks dir");
        // Connect to socket
        info!(
            "Connecting to worker at socket {:?} with ID {}",
            self.socket_path, self.subworker_id
        );
        if !self.socket_path.exists() {
            panic!("Socket file not found at {:?}", self.socket_path);
        }
        // Change directory to prevent too long socket pathnames
        env::set_current_dir(self.socket_path.parent().unwrap())
            .expect("error chdir to socket dir");
        let mut sock = UnixStream::connect(&self.socket_path.file_name().unwrap())
            .expect("error opening socket");
        env::set_current_dir(&self.working_dir).expect("error chdir to working dir");
        // Register and run the task loop, catching any errors
        let res = (|| {
            let regmsg = SubworkerToWorkerMessage::Register(self.register());
            sock.write_msg(&regmsg)?;
            loop {
                match sock.read_msg()? {
                    WorkerToSubworkerMessage::Call(call_msg) => {
                        let reply = self.handle_call(call_msg);
                        sock.write_msg(&SubworkerToWorkerMessage::Result(reply))?;
                    }
                    WorkerToSubworkerMessage::DropCached(drop_msg) => {
                        if !drop_msg.objects.is_empty() {
                            panic!("received nonempty dropCached request with no cached objects");
                        }
                    }
                }
            }
        })();
        match res {
            Err(Error(ErrorKind::Io(ref e), _))
                if (e.kind() == io::ErrorKind::ConnectionAborted)
                    || (e.kind() == io::ErrorKind::UnexpectedEof) =>
            {
                info!("Connection closed, shutting down");
            }
            other => other.expect("error in the message loop"),
        }
    }

    /// Create a register message to the worker.
    fn register(&mut self) -> RegisterMsg {
        RegisterMsg {
            protocol: MSG_PROTOCOL.into(),
            subworker_id: self.subworker_id,
            subworker_type: self.subworker_type.clone(),
        }
    }

    /// Handle one call msg: decode, run the task function, cleanup finished task
    /// (and already staged files on failure), create reply message.
    ///
    /// Panics on any IO error. TaskErrors are handled and returned to the worker.
    fn handle_call(&mut self, call_msg: CallMsg) -> ResultMsg {
        let task_name = format!(
            "{}-task-{}_{}",
            chrono::Local::now().format("%Y%m%d-%H%M%S"),
            call_msg.task.get_session_id(),
            call_msg.task.get_id()
        );
        let task_dir = self.tasks_dir.join(task_name);
        let mut context = Context::for_call_msg(&call_msg, &self.staging_dir, &task_dir);
        match self.tasks.get(&call_msg.method) {
            None => {
                debug!(
                    "Task {:?} not found in subworker {:?}",
                    call_msg.method, self.subworker_type
                );
                context.success = false;
                context
                    .attributes
                    .set(
                        "error",
                        format!(
                            "Task {:?} not found in subworker {:?}",
                            call_msg.method, self.subworker_type
                        ),
                    )
                    .unwrap();
            }
            Some(f) => {
                fs::create_dir(&task_dir).expect("error creating task dir");
                // Call the method function with context
                let res = context.call_with_context(f.borrow());
                // Check and handle in-task errors
                env::set_current_dir(&self.working_dir).expect("error on chdir to work dir");
                if let Err(ref e) = res {
                    debug!(
                        "Method {:?} in {:?} failed: {}",
                        call_msg.method, task_dir, e
                    );
                    context.success = false;
                    context
                        .attributes
                        .set(
                            "error",
                            format!(
                                "error returned from call to {:?} (in {:?}):\n{}",
                                call_msg.method, task_dir, e
                            ),
                        )
                        .unwrap();
                    // Clean already staged/open outputs
                    for mut o in context.outputs.iter_mut() {
                        o.cleanup_failed_task();
                    }
                    if !self.keep_failed_tasks {
                        // cleanup of the task working dir
                        fs::remove_dir_all(task_dir)
                            .expect("error removing the task direcotry after failure");
                    }
                } else {
                    debug!("Method {:?} finished", call_msg.method);
                    // cleanup of the task working dir
                    fs::remove_dir_all(task_dir)
                        .expect("error removing the finished task direcotry");
                }
            }
        }
        context.into_result_msg()
    }
}
