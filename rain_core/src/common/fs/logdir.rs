use std::path::{Path, PathBuf};

use common::id::ExecutorId;

pub struct LogDir {
    path: PathBuf,
}

impl LogDir {
    pub fn new(path: PathBuf) -> Self {
        let sw = path.join("executors");
        if !sw.exists() {
            ::std::fs::create_dir(&sw).unwrap();
        }
        LogDir { path }
    }

    /// Get path to logs for executor
    pub fn executor_log_paths(&self, id: ExecutorId) -> (PathBuf, PathBuf) {
        let out = self
            .path
            .join(Path::new(&format!("executors/executor-{}.out", id)));
        let err = self
            .path
            .join(Path::new(&format!("executors/executor-{}.err", id)));
        (out, err)
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }
}
