use std::path::{Path, PathBuf};
use std::cell::Cell;

use common::id::{SId, SubworkerId, TaskId};
use errors::Result;
use super::tempfile::TempFileName;

pub struct WorkDir {
    path: PathBuf,
    id_counter: Cell<u64>,
    data_path: PathBuf,
}

impl WorkDir {
    pub fn new(path: PathBuf) -> Self {
        ::std::fs::create_dir(path.join("data")).unwrap();
        ::std::fs::create_dir(path.join("tasks")).unwrap();
        ::std::fs::create_dir(path.join("tmp")).unwrap();
        ::std::fs::create_dir(path.join("subworkers")).unwrap();
        ::std::fs::create_dir(path.join("subworkers/work")).unwrap();
        // Canonilize is very imporant here,
        // We often check if symlinks goes to data dir
        let path = ::std::fs::canonicalize(path).unwrap();
        WorkDir {
            data_path: path.join("data"),
            path,
            id_counter: Cell::new(0),
        }
    }

    /// Get path to unix socket where worker is listening
    pub fn subworker_listen_path(&self) -> PathBuf {
        self.path.join(Path::new("subworkers/listen"))
    }

    /// Create subworker working directory
    pub fn make_subworker_work_dir(&self, id: SubworkerId) -> Result<::tempdir::TempDir> {
        ::tempdir::TempDir::new_in(self.path.join("subworkers/work"), &format!("{}", id))
            .map_err(|e| e.into())
    }

    pub fn make_temp_file(&self) -> TempFileName {
        TempFileName::new(self.path.join(format!("tmp/{}", self.new_id())))
    }

    pub fn make_temp_dir(&self, prefix: &str) -> Result<::tempdir::TempDir> {
        ::tempdir::TempDir::new_in(
            self.path.join("tmp"),
            prefix
        ).map_err(|e| e.into())
    }

    /// Create temporary directory for task
    pub fn make_task_temp_dir(&self, task_id: TaskId) -> Result<::tempdir::TempDir> {
        self.make_temp_dir(&format!("{}-{}", task_id.get_session_id(), task_id.get_id()))
    }

    fn new_id(&self) -> u64 {
        let value = self.id_counter.get();
        self.id_counter.set(value + 1);
        value
    }

    pub fn new_path_for_dataobject(&self) -> PathBuf {
        self.data_path
            .join(Path::new(&format!("{}", self.new_id())))
    }

    pub fn data_path(&self) -> &Path {
        &self.data_path
    }
}
