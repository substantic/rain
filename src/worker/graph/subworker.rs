
use std::process::{Command, Stdio};
use std::cell::RefCell;
use std::rc::Rc;
use std::fs::File;
use std::os::unix::io::{FromRawFd, IntoRawFd};

use common::id::SubworkerId;
use common::wrapped::WrappedRcRefCell;
use worker::State;
use subworker_capnp::subworker_upstream;
use capnp::capability::Promise;
use tokio_process::CommandExt;
use futures::Future;

struct Inner {
    subworker_id: SubworkerId,
    control: ::subworker_capnp::subworker_control::Client
}

pub type Subworker = WrappedRcRefCell<Inner>;


impl Subworker {

    pub fn new(
        subworker_id: SubworkerId,
        control: ::subworker_capnp::subworker_control::Client) -> Self {
            Self::wrap(Inner {
                subworker_id,
                control
            })
    }

    pub fn id(&self) -> SubworkerId {
        self.get().subworker_id
    }
}


pub fn start_python_subworker(state: &State) -> SubworkerId
{
    let subworker_id = state.make_subworker_id();
    let (log_path_out, log_path_err) = state.subworker_log_paths(subworker_id);

    info!("Staring new subworker {}", subworker_id);
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
    let handle = state.handle();
    let future = Command::new("python3")
        .arg("-m")
        .arg("rain.subworker")
        .stdout(log_path_out_pipe)
        .stderr(log_path_err_pipe)
        .env("RAIN_SUBWORKER_SOCKET", state.subworker_listen_path())
        .env("RAIN_SUBWORKER_ID", subworker_id.to_string())
        .status_async(&handle)
        .and_then(move |status| {
            error!("Subworker {} terminated with exit code: {}", subworker_id, status);
            panic!("Subworker terminated; TODO handle this situation");
            Ok(())
        })
        .map_err(|e| panic!("Spawning subworker failed: {:?}"));
    handle.spawn(future);

    subworker_id
}