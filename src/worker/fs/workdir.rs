
use std::path::{Path, PathBuf};

use common::id::{SubworkerId, TaskId, SId, DataObjectId};
use errors::Result;



pub struct WorkDir {
    path: PathBuf
}

impl WorkDir {
    pub fn new(path: PathBuf) -> Self {
        ::std::fs::create_dir(path.join("data")).unwrap();
        ::std::fs::create_dir(path.join("tasks")).unwrap();
        ::std::fs::create_dir(path.join("subworkers")).unwrap();
        ::std::fs::create_dir(path.join("subworkers/logs")).unwrap();
        ::std::fs::create_dir(path.join("subworkers/work")).unwrap();
        WorkDir {
            path
        }
    }

    /// Get path to unix socket where worker is listening
    pub fn subworker_listen_path(&self) -> PathBuf {
        self.path.join(Path::new("subworkers/listen"))
    }


    /// Get path to logs for subworker
    pub fn subworker_log_paths(&self, id: SubworkerId) -> (PathBuf, PathBuf) {
        let out = self.path.join(Path::new(&format!("subworkers/logs/subworker-{}.out",
                                                          id)));
        let err = self.path.join(Path::new(&format!("subworkers/logs/subworker-{}.err",
                                                          id)));
        (out, err)
    }

    /// Create subworker working directory
    pub fn make_subworker_work_dir(&self, id: SubworkerId) -> Result<::tempdir::TempDir> {
        ::tempdir::TempDir::new_in(self.path.join("subworkers/work"),
                                   &format!("{}", id))
            .map_err(|e| e.into())
    }

    /// Create temporary directory for task
    pub fn make_task_temp_dir(&self, task_id: TaskId) -> Result<::tempdir::TempDir> {
        ::tempdir::TempDir::new_in(self.path.join("tasks"),
                                   &format!("{}-{}", task_id.get_session_id(), task_id.get_id()))
            .map_err(|e| e.into())
    }

    pub fn path_for_dataobject(&self, id: &DataObjectId) -> PathBuf {
        self.path.join(Path::new(&format!("data/{}-{}", id.get_session_id(), id.get_id())))
    }

}