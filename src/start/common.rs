
use std::path::PathBuf;

pub enum Readiness {
    /// Ready file is a file that
    /// at is created when a process is ready
    WaitingForReadyFile(PathBuf),
    IsReady,
}
