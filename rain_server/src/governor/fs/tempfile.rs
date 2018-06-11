use rain_core::errors::*;
use std::fs::File;
use std::path::{Path, PathBuf};

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
