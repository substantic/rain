use std::fs::File;
use std::process::{Command, Stdio};
use tokio_process::CommandExt;
use futures::Future;
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::Path;
use std::io::Read;

use super::TaskResult;
use worker::graph::TaskRef;
use worker::state::State;
use errors::Result;

fn read_stderr(path: &Path) -> Result<String> {
    // TODO: If the file is too big, truncate the beginning
    let mut file = File::open(path)?;
    let mut s = String::new();
    file.read_to_string(&mut s)?;
    Ok(s)
}

#[derive(Deserialize)]
struct RunConfigInput {
    pub path: String,
    pub write: bool,
}

#[derive(Deserialize)]
struct RunConfig {
    pub args: Vec<String>,
    pub in_paths: Vec<RunConfigInput>,
    pub out_paths: Vec<String>,
}

pub fn task_run(state: &mut State, task_ref: TaskRef) -> TaskResult {
    let state_ref = state.self_ref();
    let config: RunConfig = task_ref.get().attributes.get("config")?;

    let (dir, future, stderr_path) = {
        // Parse arguments
        let name = config.args.get(0).ok_or_else(|| "Arguments are empty")?;
        let task = task_ref.get();

        let dir = state.work_dir().make_task_temp_dir(task.id)?;

        // Map inputs
        let mut in_io = Stdio::null();

        for (iconfig, input) in config.in_paths.iter().zip(&task.inputs) {
            let obj = input.object.get();
            if iconfig.write {
                obj.data().write_to_path(&dir.path().join(&iconfig.path))?;
            } else {
                obj.data().link_to_path(&dir.path().join(&iconfig.path))?;
            }
            if iconfig.path == "+in" {
                let in_id = File::open(dir.path().join("+in"))?.into_raw_fd();
                in_io = unsafe { Stdio::from_raw_fd(in_id) };
            }
        }

        // Create files for stdout/stderr
        let out_id = File::create(dir.path().join("+out"))
            .expect("File for stdout cannot be opened")
            .into_raw_fd();
        let stderr_path = dir.path().join("+err");
        let err_id = File::create(&stderr_path)
            .expect("File for stderr cannot be opened")
            .into_raw_fd();

        let out_io = unsafe { Stdio::from_raw_fd(out_id) };
        let err_io = unsafe { Stdio::from_raw_fd(err_id) };

        debug!("Starting command: {}", name);

        let future = Command::new(&name)
            .args(&config.args[1..])
            .stdin(in_io)
            .stdout(out_io)
            .stderr(err_io)
            .current_dir(dir.path())
            .status_async2(state.handle())?;

        (dir, future, stderr_path)
    };

    Ok(Box::new(future.map_err(|e| e.into()).and_then(
        move |status| {
            if !status.success() {
                let stderr = match read_stderr(&stderr_path) {
                    Ok(s) => format!("Stderr: {}\n", s),
                    Err(e) => format!(
                        "Stderr could not be obtained: {}",
                        ::std::error::Error::description(&e)
                    ),
                };
                match status.code() {
                    Some(code) => bail!("Program exit with exit code {}\n{}", code, stderr),
                    None => bail!("Program terminated by signal\n{}", stderr),
                }
            }
            {
                let state = state_ref.get();
                let task = task_ref.get();

                for (path, dataobj) in config.out_paths.iter().zip(&task.outputs) {
                    let abs_path = dir.path().join(path);
                    dataobj.get_mut().set_data_by_fs_move(
                        &abs_path,
                        Some(path),
                        &state.work_dir(),
                    )?;
                }
            }
            Ok(())
        },
    )))
}
