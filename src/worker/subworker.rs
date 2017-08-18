
use std::process::{Command, Stdio};
use std::cell::RefCell;
use std::rc::Rc;
use std::fs::File;
use std::os::unix::io::{FromRawFd, IntoRawFd};

use common::id::Id;
use worker::state::State;
use subworker_capnp::subworker_upstream;

use capnp::capability::Promise;
use tokio_process::CommandExt;
use futures::Future;

struct Inner {

}

#[derive(Clone)]
struct Subworker {
    inner: Rc<RefCell<Inner>>
}

impl Subworker {

    pub fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(Inner {

            }))
        }
    }

}

pub struct SubworkerUpstreamImpl {
    state: State,
}

impl SubworkerUpstreamImpl {
    pub fn new(state: &State) -> Self {
        Self { state: state.clone() }
    }
}

impl Drop for SubworkerUpstreamImpl {
    fn drop(&mut self) {
        panic!("Lost connection to subworker");
    }
}

impl subworker_upstream::Server for SubworkerUpstreamImpl {

    /*
    fn get_worker_resources(&mut self,
              _params: worker_control::GetWorkerResourcesParams,
              mut results: worker_control::GetWorkerResourcesResults)
              -> Promise<(), ::capnp::Error> {
        results.get().set_n_cpus(self.state.get_n_cpus());
        Promise::ok(())
    }*/
}


pub fn start_python_subworker(state: &State) -> Id
{
    let subworker_id = state.get_new_id();
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