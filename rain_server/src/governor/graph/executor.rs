use rain_core::{errors::*, sys::*, types::*};
use rain_core::comm::{ResultMsg, GovernorToExecutorMessage, DropCachedMsg, CallMsg};
use common::{Sender};
use std::fs::File;
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::Path;
use std::process::{Command, Stdio};

use governor::graph::Task;
use wrapped::WrappedRcRefCell;

pub struct Executor {
    executor_id: ExecutorId,
    executor_type: String,
    control: Option<Sender>,
    work_dir: ::tempdir::TempDir,
    finish_sender: Option<::futures::unsync::oneshot::Sender<ResultMsg>>,
}

pub type ExecutorRef = WrappedRcRefCell<Executor>;

impl Executor {
    #[inline]
    pub fn executor_type(&self) -> &str {
        &self.executor_type
    }

    #[inline]
    pub fn id(&self) -> ExecutorId {
        self.executor_id
    }

    #[inline]
    pub fn work_dir(&self) -> &Path {
        self.work_dir.path()
    }
}

pub fn get_log_tails(out_log_name: &Path, err_log_name: &Path, size: u64) -> String {
    format!(
        "{}\n\n{}\n",
        read_tail(&out_log_name, size)
            .map(|s| format!("Content of stdout (last {} bytes):\n{}", size, s))
            .unwrap_or_else(|e| format!("Cannot read stdout: {}", e.description())),
        read_tail(&err_log_name, size)
            .map(|s| format!("Content of stderr (last {} bytes):\n{}", size, s))
            .unwrap_or_else(|e| format!("Cannot read stderr: {}", e.description()))
    )
}

impl Executor {
    // Kill executor, if the process is already killed than nothing happens
    pub fn kill(&mut self) {
        if self.control.is_none() {
            debug!("Killing already killed executor");
        }
        self.control = None;
    }

    pub fn get_log_tails(&self, log_dir: &LogDir, size: u64) -> String {
        let (out_log_name, err_log_name) = log_dir.executor_log_paths(self.executor_id);
        get_log_tails(&out_log_name, &err_log_name, size)
    }

    pub fn pick_finish_sender(&mut self) -> Option<::futures::unsync::oneshot::Sender<ResultMsg>> {
        ::std::mem::replace(&mut self.finish_sender, None)
    }

    pub fn send_remove_cached_objects(&self, object_ids: &[DataObjectId]) {
        let control = self.control.as_ref().clone().unwrap();
        let message = GovernorToExecutorMessage::DropCached(DropCachedMsg {
            objects: object_ids.into(),
        });
        control.send(::serde_cbor::to_vec(&message).unwrap());
    }

    pub fn send_task(
        &mut self,
        task: &Task,
        executor_ref: &ExecutorRef,
    ) -> ::futures::unsync::oneshot::Receiver<ResultMsg> {
        let control = self.control.as_ref().clone().unwrap();
        let message = GovernorToExecutorMessage::Call(CallMsg {
            spec: task.spec.clone(),
            inputs: task
                .inputs
                .iter()
                .map(|i| i.get().create_input_spec(executor_ref))
                .collect(),
            outputs: task
                .outputs
                .iter()
                .map(|o| o.get().create_output_spec())
                .collect(),
        });
        control.send(::serde_cbor::to_vec(&message).unwrap());

        assert!(self.finish_sender.is_none()); // Not task is running
        let (sender, receiver) = ::futures::unsync::oneshot::channel();
        self.finish_sender = Some(sender);
        receiver
    }
}

impl ::std::fmt::Debug for ExecutorRef {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "Executor id={}", self.get().executor_id)
    }
}

impl ExecutorRef {
    pub fn new(
        executor_id: ExecutorId,
        executor_type: String,
        control: Sender,
        work_dir: ::tempdir::TempDir,
    ) -> Self {
        Self::wrap(Executor {
            executor_id,
            executor_type,
            control: Some(control),
            work_dir,
            finish_sender: None,
        })
    }
}

pub fn executor_command(
    executor_dir: &::tempdir::TempDir,
    socket_path: &Path,
    log_dir: &LogDir,
    executor_id: ExecutorId,
    program_name: &str,
    program_args: &[String],
) -> Result<Command> {
    let (log_path_out, log_path_err) = log_dir.executor_log_paths(executor_id);
    info!("Executor stdout log: {:?}", log_path_out);
    info!("Executor stderr log: {:?}", log_path_err);

    // --- Open log files ---
    let log_path_out_id = File::create(log_path_out)
        .expect("Executor log cannot be opened")
        .into_raw_fd();
    let log_path_err_id = File::create(log_path_err)
        .expect("Executor log cannot be opened")
        .into_raw_fd();

    let log_path_out_pipe = unsafe { Stdio::from_raw_fd(log_path_out_id) };
    let log_path_err_pipe = unsafe { Stdio::from_raw_fd(log_path_err_id) };

    // --- Start process ---
    let mut command = Command::new(program_name);

    command
        .args(program_args)
        .stdout(log_path_out_pipe)
        .stderr(log_path_err_pipe)
        .env("RAIN_EXECUTOR_SOCKET", socket_path)
        .env("RAIN_EXECUTOR_ID", executor_id.to_string())
        .current_dir(executor_dir.path());
    Ok(command)
}
