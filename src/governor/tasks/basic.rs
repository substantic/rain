use std::path::Path;
use std::sync::Arc;

use super::TaskResult;
use common::DataType;
use errors::ErrorKind;
use futures::{future, Future};
use governor::data::{Data, DataBuilder};
use governor::graph::TaskRef;
use governor::state::State;

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
        let state = state_ref.get();
        let work_dir = state.work_dir();
        let mut builder = DataBuilder::new(work_dir, DataType::Blob, Some(result_size));
        for input in inputs {
            builder.write_blob(&input).unwrap();
        }
        let result = builder.build(work_dir);
        let output = task_ref.get().output(0);
        output.get_mut().set_data(Arc::new(result))?;
        Ok(())
    })))
}

/// Task that returns the input argument after a given number of milliseconds
pub fn task_sleep(_state: &mut State, task_ref: TaskRef) -> TaskResult {
    let sleep_ms: u64 = {
        let task = task_ref.get();
        task.check_number_of_args(1)?;
        task.spec.parse_config()?
    };
    let now = ::std::time::Instant::now();
    debug!("Starting sleep task for {} ms", sleep_ms);
    let duration = ::std::time::Duration::from_millis(sleep_ms);
    Ok(Box::new(
        ::tokio_timer::Delay::new(now + duration)
            .map_err(|e| e.into())
            .and_then(move |()| {
                {
                    let task = task_ref.get();
                    let output = task.output(0);
                    output.get_mut().set_data(task.input_data(0))?;
                }
                Ok(())
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
            let config: OpenConfig = task.spec.parse_config()?;
            let path = Path::new(&config.path);
            if !path.is_absolute() {
                bail!("Path {:?} is not absolute", path);
            }
            let metadata = &::std::fs::metadata(&path)
                .map_err(|_| ErrorKind::Msg(format!("Path '{}' not found", config.path)))?;
            let target_path = state_ref.get().work_dir().new_path_for_dataobject();
            let data = Data::new_by_fs_copy(
                &path,
                metadata,
                target_path,
                state_ref.get().work_dir().data_path(),
            )?;
            let output = task_ref.get().output(0);
            output.get_mut().set_data(Arc::new(data))?;
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
        let config: ExportConfig = task.spec.parse_config()?;
        let path = Path::new(&config.path);
        if !path.is_absolute() {
            bail!("Path {:?} is not absolute", path);
        }
        let input = task.input_data(0);
        input.write_to_path(path)
    })))
}

#[derive(Deserialize)]
struct MakeDirectoryConfig {
    paths: Vec<String>,
}

/// Make directory
pub fn task_make_directory(state: &mut State, task_ref: TaskRef) -> TaskResult {
    let state_ref = state.self_ref();
    Ok(Box::new(future::lazy(move || {
        let state = state_ref.get();
        let task = task_ref.get();
        let dir = state.work_dir().make_task_temp_dir(task.spec.id)?;
        let main_dir = dir.path().join("newdir");
        ::std::fs::create_dir(&main_dir)?;

        let config: MakeDirectoryConfig = task.spec.parse_config()?;
        task.check_number_of_args(config.paths.len())?;
        for (ref path, ref data) in config.paths.iter().zip(task.inputs_data().iter()) {
            let p = Path::new(path);
            if !p.is_relative() {
                bail!("Path '{}' is not relative", path);
            }
            let target_path = main_dir.join(&p);
            ::std::fs::create_dir_all(&target_path.parent().unwrap())?;
            data.link_to_path(&target_path)?;
        }
        let output = task.output(0);
        let mut obj = output.get_mut();
        obj.set_data_by_fs_move(&main_dir, None, state.work_dir())
    })))
}

#[derive(Deserialize)]
struct SliceDirectoryConfig {
    path: String,
}

/// Make directory
pub fn task_slice_directory(state: &mut State, task_ref: TaskRef) -> TaskResult {
    let state_ref = state.self_ref();
    Ok(Box::new(future::lazy(move || {
        let state = state_ref.get();
        let task = task_ref.get();
        task.check_number_of_args(1)?;
        let config: SliceDirectoryConfig = task.spec.parse_config()?;
        let data = task.input_data(0);
        let dir = state.work_dir().make_task_temp_dir(task.spec.id)?;
        let main_dir = dir.path().join("newdir");
        data.link_to_path(&main_dir).unwrap();
        let path = main_dir.join(&config.path);
        let output = task.output(0);
        let mut obj = output.get_mut();
        obj.set_data_by_fs_move(&path, Some(&config.path), state.work_dir())
    })))
}
