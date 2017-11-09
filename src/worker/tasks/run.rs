use std::fs::File;
use std::sync::Arc;
use std::process::{Command, Stdio};
use tokio_process::CommandExt;
use futures::Future;
use std::os::unix::io::{FromRawFd, IntoRawFd};

use super::{TaskContext, TaskResult};
use worker::state::State;
use worker::data::{Data, DataType};
use errors::{Result, Error};




pub fn task_run(context: TaskContext, state: &State) -> TaskResult
{
    let (dir, future) = {
        let config = &context.task.get().task_config;
        let mut cursor = ::std::io::Cursor::new(&config);

        let reader = ::capnp::serialize_packed::read_message(
            &mut cursor,
            ::capnp::message::ReaderOptions::new())?;

        let run_config = reader.get_root::<::tasks_capnp::run_task::Reader>()?;

        // Parse arguments
        let rargs: ::std::result::Result<Vec<_>, ::capnp::Error> = run_config.get_args()?.iter().collect();
        let args = rargs?;
        let name = args.get(0).ok_or_else(|| "Arguments are empty")?;
        let task = context.task.get();

        let dir = state.work_dir().make_task_temp_dir(task.id)?;

        // Map inputs
        let mut in_io = Stdio::null();

        for (path, input) in run_config.get_input_paths()?.iter().zip(&task.inputs) {
            let path = path?;
            let obj = input.object.get();
            obj.data().map_to_path(&dir.path().join(path))?;
            if path == "+in" {
                let in_id = File::open(dir.path().join("+in"))?.into_raw_fd();
                in_io = unsafe { Stdio::from_raw_fd(in_id) };
            }
        }

        // Create files for stdout/stderr
        let out_id = File::create(dir.path().join("+out"))
            .expect("File for stdout cannot be opened").into_raw_fd();
        let err_id = File::create(dir.path().join("+err"))
            .expect("File for stderr cannot be opened").into_raw_fd();

        let out_io = unsafe { Stdio::from_raw_fd(out_id) };
        let err_io = unsafe { Stdio::from_raw_fd(err_id) };

        debug!("Starting command: {}", name);

        let future = Command::new(&name)
            .args(&args[1..])
            .stdin(in_io)
            .stdout(out_io)
            .stderr(err_io)
            .current_dir(dir.path())
            .status_async2(state.handle())?;

        (dir, future)
    };

    Ok(Box::new(future.map_err(|e| e.into())
        .and_then(move |status| {
            if !status.success() {
                match status.code() {
                    Some(code) => bail!("Program exit with exit code {}", code),
                    None => bail!("Program terminated by signal")
                }
            }
            {
                let state = context.state.get();
                let config = &context.task.get().task_config;
                let task = context.task.get();

                let mut cursor = ::std::io::Cursor::new(&config);
                let reader = ::capnp::serialize_packed::read_message(
                    &mut cursor,
                    ::capnp::message::ReaderOptions::new())?;
                let run_config = reader.get_root::<::tasks_capnp::run_task::Reader>()?;

                for (path, dataobj) in run_config.get_output_paths()?.iter().zip(&task.outputs) {
                    let path = dir.path().join(path?);
                    if !path.is_file() {
                        bail!("Output '{}' not found");
                    }
                    let target_path = state.work_dir().new_path_for_dataobject();
                    let data = Data::new_by_fs_move(&path, target_path)?;
                    let mut obj = dataobj.get_mut();
                    obj.set_data(Arc::new(data));
                }
            }
            Ok(context)
        })))
}
