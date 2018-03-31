use std::path::{Path, PathBuf};
use std::fs::File;
use errors::Result;

pub struct TempFileName {
    path: PathBuf,
}

impl TempFileName {
    pub fn new(path: PathBuf) -> Self {
        TempFileName { path }
    }

    pub fn create(&self) -> Result<File> {
        Ok(File::create(&self.path)?)
    }

    pub fn open(&self) -> Result<File> {
        Ok(File::open(&self.path)?)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFileName {
    fn drop(&mut self) {
        let _ = ::std::fs::remove_file(&self.path);
    }
}
