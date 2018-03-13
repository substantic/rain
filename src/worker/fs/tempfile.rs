use std::path::PathBuf;

pub struct TempFileName {
    path: PathBuf
}

impl TempFileName {
    pub fn new(path: PathBuf) -> Self {
        TempFileName { path }
    }
}

impl Drop for TempFileName {

    fn drop(&mut self) {
        let _ = ::std::fs::remove_file(&self.path);
    }
}