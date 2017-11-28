
use std::sync::Arc;
use std::path::Path;

use super::{TaskContext, TaskResult};
use worker::state::State;
use worker::data::{Data, DataBuilder, BlobBuilder};
use futures::{Future, IntoFuture, future};
use errors::Result;
use bytes::{Buf, LittleEndian};

/// Task that merge all input blobs and merge them into one blob
pub fn task_concat(context: TaskContext, state: &State) -> TaskResult {
    let inputs = context.task.get().inputs();

    for (i, input) in inputs.iter().enumerate() {
        if !input.is_blob() {
            bail!("Input {} object is not blob", i);
        }
    }

    Ok(Box::new(future::lazy(move || {
        let result_size: usize = inputs.iter().map(|d| d.size()).sum();
        let mut builder = BlobBuilder::new();
        builder.set_size(result_size);
        for input in inputs {
            builder.write_blob(&input);
        }
        let result = builder.build();
        let output = context.task.get().output(0);
        output.get_mut().set_data(Arc::new(result));
        Ok(context)
    })))
}

/// Task that returns the input argument after a given number of milliseconds
pub fn task_sleep(context: TaskContext, state: &State) -> TaskResult {
    context.task.get().check_number_of_args(1)?;
    let sleep_ms = {
        let task = context.task.get();
        ::std::io::Cursor::new(&task.task_config[..]).get_i32::<LittleEndian>()
    };
    debug!("Starting sleep task for {} ms", sleep_ms);
    let duration = ::std::time::Duration::from_millis(sleep_ms as u64);
    Ok(Box::new(state.timer().sleep(duration)
                .map_err(|e| e.into())
                .map(move |()| {
                    {
                        let task = context.task.get();
                        let output = task.output(0);
                        output.get_mut().set_data(task.input(0));
                    }
                    context
                })))
}

/// Open external file
pub fn task_open(context: TaskContext, state: &State) -> TaskResult {
    context.task.get().check_number_of_args(0)?;
    Ok(Box::new(future::lazy(move || {
        {
            let task = context.task.get();
            let path = Path::new(::std::str::from_utf8(&task.task_config)?);
            if !path.is_absolute() {
                bail!("Path {:?} is not absolute", path);
            }
            let target_path = context.state.get().work_dir().new_path_for_dataobject();
            let data = Data::new_by_fs_copy(&path, target_path)?;
            let output = context.task.get().output(0);
            output.get_mut().set_data(Arc::new(data));
        }
        Ok(context)
    })))
}
