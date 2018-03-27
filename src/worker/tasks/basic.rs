use std::sync::Arc;
use std::path::Path;

use super::TaskResult;
use common::DataType;
use worker::state::State;
use worker::graph::TaskRef;
use worker::data::{Data, DataBuilder};
use futures::{future, Future};

/// Task that merge all input blobs and merge them into one blob
pub fn task_concat(state: &mut State, task_ref: TaskRef) -> TaskResult {
    let inputs = {
        let task = task_ref.get();
        task.inputs_data()
    };

    for (i, input) in inputs.iter().enumerate() {
        if !input.is_blob() {
            bail!("Input {} object is not blob", i);
        }
    }

    let state_ref = state.self_ref();

    Ok(Box::new(future::lazy(move || {
        let result_size: usize = inputs.iter().map(|d| d.size()).sum();
        let mut builder = DataBuilder::new(
            &state_ref.get().work_dir(),
            DataType::Blob,
            Some(result_size),
        );
        for input in inputs {
            builder.write_blob(&input).unwrap();
        }
        let result = builder.build();
        let output = task_ref.get().output(0);
        output.get_mut().set_data(Arc::new(result));
        Ok(())
    })))
}

/// Task that returns the input argument after a given number of milliseconds
pub fn task_sleep(state: &mut State, task_ref: TaskRef) -> TaskResult {
    let sleep_ms: u64 = {
        let task = task_ref.get();
        task.check_number_of_args(1)?;
        task.attributes.get("config")?
    };
    debug!("Starting sleep task for {} ms", sleep_ms);
    let duration = ::std::time::Duration::from_millis(sleep_ms);
    Ok(Box::new(
        state
            .timer()
            .sleep(duration)
            .map_err(|e| e.into())
            .map(move |()| {
                {
                    let task = task_ref.get();
                    let output = task.output(0);
                    output.get_mut().set_data(task.input_data(0));
                }
                ()
            }),
    ))
}

#[derive(Deserialize)]
struct OpenConfig {
    path: String,
}

/// Open external file
pub fn task_open(state: &mut State, task_ref: TaskRef) -> TaskResult {
    {
        let task = task_ref.get();
        task.check_number_of_args(0)?;
    }
    let state_ref = state.self_ref();
    Ok(Box::new(future::lazy(move || {
        {
            let task = task_ref.get();
            let config: OpenConfig = task.attributes.get("config")?;
            let path = Path::new(&config.path);
            if !path.is_absolute() {
                bail!("Path {:?} is not absolute", path);
            }
            let target_path = state_ref.get().work_dir().new_path_for_dataobject();
            let data = Data::new_by_fs_copy(&path, target_path)?;
            let output = task_ref.get().output(0);
            output.get_mut().set_data(Arc::new(data));
        }
        Ok(())
    })))
}

#[derive(Deserialize)]
struct ExportConfig {
    path: String,
}

/// Export internal file to external file system
pub fn task_export(_: &mut State, task_ref: TaskRef) -> TaskResult {
    {
        let task = task_ref.get();
        task.check_number_of_args(1)?;
    }
    Ok(Box::new(future::lazy(move || {
        let task = task_ref.get();
        let config: ExportConfig = task.attributes.get("config")?;
        let path = Path::new(&config.path);
        if !path.is_absolute() {
            bail!("Path {:?} is not absolute", path);
        }
        let input = task.input_data(0);
        input.export_to_path(path)
    })))
}
