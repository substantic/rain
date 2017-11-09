
use std::process::{Command, Stdio};
use std::cell::RefCell;
use std::rc::Rc;
use std::fs::File;
use std::os::unix::io::{FromRawFd, IntoRawFd};

use common::id::SubworkerId;
use common::wrapped::WrappedRcRefCell;
use worker::{StateRef};
use worker::fs::workdir::WorkDir;
use subworker_capnp::subworker_upstream;
use capnp::capability::Promise;
use futures::Future;

use errors::Result;

pub struct Subworker {
    subworker_id: SubworkerId,
    subworker_type: String,
    control: ::subworker_capnp::subworker_control::Client
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
    pub fn control(&self) -> &::subworker_capnp::subworker_control::Client {
        &self.control
    }
}

impl SubworkerRef {

    pub fn new(
        subworker_id: SubworkerId,
        subworker_type: String,
        control: ::subworker_capnp::subworker_control::Client) -> Self {
            Self::wrap(Subworker {
                subworker_id,
                subworker_type,
                control
            })
    }
}


pub fn subworker_command(work_dir: &WorkDir,
                         subworker_id: SubworkerId,
                         subworker_type: &str,
                         program_name: &str,
                         program_args: &[String]) -> Result<(Command, ::tempdir::TempDir)>
{
    let (log_path_out, log_path_err) = work_dir.subworker_log_paths(subworker_id);
    let subworker_dir = work_dir.make_subworker_work_dir(subworker_id)?;

    info!("Staring new subworker type={} id={}", subworker_type, subworker_id);
    info!("Subworker stdout log: {:?}", log_path_out);
    info!("Subworker stderr log: {:?}", log_path_err);

    // --- Open log files ---
    let log_path_out_id = File::create(log_path_out)
        .expect("Subworker log cannot be opened").into_raw_fd();
    let log_path_err_id = File::create(log_path_err)
        .expect("Subworker log cannot be opened").into_raw_fd();

    let log_path_out_pipe = unsafe { Stdio::from_raw_fd(log_path_out_id) };
    let log_path_err_pipe = unsafe { Stdio::from_raw_fd(log_path_err_id) };

    // --- Start process ---
    let mut command = Command::new(program_name);

    command.args(program_args)
        .stdout(log_path_out_pipe)
        .stderr(log_path_err_pipe)
        .env("RAIN_SUBWORKER_SOCKET", work_dir.subworker_listen_path())
        .env("RAIN_SUBWORKER_ID", subworker_id.to_string())
        .current_dir(subworker_dir.path());
    Ok((command, subworker_dir))
}