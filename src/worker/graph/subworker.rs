use std::process::{Command, Stdio};
use std::fs::File;
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::Path;

use common::id::{DataObjectId, SubworkerId};
use common::wrapped::WrappedRcRefCell;
use common::fs::LogDir;
use common::comm::Sender;
use worker::graph::Task;
use worker::rpc::subworker_serde::{CallMsg, DropCachedMsg, ResultMsg};
use worker::rpc::subworker_serde::WorkerToSubworkerMessage;

use errors::Result;

pub struct Subworker {
    subworker_id: SubworkerId,
    subworker_type: String,
    control: Option<Sender>,
    work_dir: ::tempdir::TempDir,
    finish_sender: Option<::futures::unsync::oneshot::Sender<ResultMsg>>,
}

pub type SubworkerRef = WrappedRcRefCell<Subworker>;

impl Subworker {
    #[inline]
    pub fn subworker_type(&self) -> &str {
        &self.subworker_type
    }

    #[inline]
    pub fn id(&self) -> SubworkerId {
        self.subworker_id
    }

    #[inline]
    pub fn work_dir(&self) -> &Path {
        self.work_dir.path()
    }
}

impl Subworker {
    // Kill subworker, if the process is already killed than nothing happens
    pub fn kill(&mut self) {
        if self.control.is_none() {
            debug!("Killing already killed subworker");
        }
        self.control = None;
    }

    pub fn pick_finish_sender(&mut self) -> Option<::futures::unsync::oneshot::Sender<ResultMsg>> {
        ::std::mem::replace(&mut self.finish_sender, None)
    }

    pub fn send_remove_cached_objects(&self, object_ids: &[DataObjectId]) {
        let control = self.control.as_ref().clone().unwrap();
        let message = WorkerToSubworkerMessage::DropCached(DropCachedMsg {
            objects: object_ids.into(),
        });
        control.send(::serde_cbor::to_vec(&message).unwrap());
    }

    pub fn send_task(
        &mut self,
        task: &Task,
        method: String,
        subworker_ref: &SubworkerRef,
    ) -> ::futures::unsync::oneshot::Receiver<ResultMsg> {
        let control = self.control.as_ref().clone().unwrap();
        let message = WorkerToSubworkerMessage::Call(CallMsg {
            task: task.id,
            method,
            attributes: task.attributes.clone(),
            inputs: task.inputs
                .iter()
                .map(|i| i.object.get().create_input_spec(&i.label, subworker_ref))
                .collect(),
            outputs: task.outputs
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

impl ::std::fmt::Debug for SubworkerRef {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "Subworker id={}", self.get().subworker_id)
    }
}

impl SubworkerRef {
    pub fn new(
        subworker_id: SubworkerId,
        subworker_type: String,
        control: Sender,
        work_dir: ::tempdir::TempDir,
    ) -> Self {
        Self::wrap(Subworker {
            subworker_id,
            subworker_type,
            control: Some(control),
            work_dir,
            finish_sender: None,
        })
    }
}

pub fn subworker_command(
    subworker_dir: &::tempdir::TempDir,
    socket_path: &Path,
    log_dir: &LogDir,
    subworker_id: SubworkerId,
    program_name: &str,
    program_args: &[String],
) -> Result<Command> {
    let (log_path_out, log_path_err) = log_dir.subworker_log_paths(subworker_id);
    info!("Subworker stdout log: {:?}", log_path_out);
    info!("Subworker stderr log: {:?}", log_path_err);

    // --- Open log files ---
    let log_path_out_id = File::create(log_path_out)
        .expect("Subworker log cannot be opened")
        .into_raw_fd();
    let log_path_err_id = File::create(log_path_err)
        .expect("Subworker log cannot be opened")
        .into_raw_fd();

    let log_path_out_pipe = unsafe { Stdio::from_raw_fd(log_path_out_id) };
    let log_path_err_pipe = unsafe { Stdio::from_raw_fd(log_path_err_id) };

    // --- Start process ---
    let mut command = Command::new(program_name);

    command
        .args(program_args)
        .stdout(log_path_out_pipe)
        .stderr(log_path_err_pipe)
        .env("RAIN_SUBWORKER_SOCKET", socket_path)
        .env("RAIN_SUBWORKER_ID", subworker_id.to_string())
        .current_dir(subworker_dir.path());
    Ok(command)
}
