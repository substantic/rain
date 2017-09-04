
use std::sync::Arc;

use super::{TaskContext, TaskResult};
use worker::state::State;
use worker::graph::{DataBuilder, BlobBuilder};
use futures::{Future, IntoFuture};
use errors::Result;
use bytes::{Buf, LittleEndian};

/// Task that merge all input blobs and merge them into one blob
pub fn task_merge(context: TaskContext, state: &State) -> TaskResult
{
    let inputs = context.inputs();

    for (i, input) in inputs.iter().enumerate() {
        if !input.is_blob() {
            bail!("Input {} object is not blob", i);
        }
    }

    let result_size : usize = inputs.iter().map(|d| d.size()).sum();
    let mut builder = BlobBuilder::new();
    builder.set_size(result_size);
    for input in inputs {
        builder.write_blob(&input);
    }
    let result = builder.build();

    context.object_finished(0, Arc::new(result));
    Ok(Box::new(Ok(context).into_future()))
}

/// Task that returns the input argument after a given number of milliseconds
pub fn task_sleep(context: TaskContext, state: &State) -> TaskResult
{
    context.check_number_of_args(1)?;
    let sleep_ms = {
        let task = context.task.get();
        ::std::io::Cursor::new(&task.task_config[..]).get_i32::<LittleEndian>()
    };
    debug!("Starting sleep task for {} ms", sleep_ms);
    let duration = ::std::time::Duration::from_millis(sleep_ms as u64);
    Ok(Box::new(state.timer().sleep(duration)
                .map_err(|e| e.into())
                .map(move |()| {
                    context.object_finished(0, context.input(0));
                    context
                })))
}
