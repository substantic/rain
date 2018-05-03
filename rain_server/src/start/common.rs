use std::path::PathBuf;
use nix::unistd::getpid;
use std::path::Path;
use errors::Result;
use std::fs::create_dir_all;
use std::error::Error;

pub enum Readiness {
    /// Ready file is a file that
    /// at is created when a process is ready
    WaitingForReadyFile(PathBuf),
    IsReady,
}

pub fn default_working_directory() -> PathBuf {
    let pid = getpid();
    let hostname = ::common::sys::get_hostname();
    PathBuf::from("/tmp/rain-work").join(format!("worker-{}-{}", hostname, pid))
}

pub fn default_logging_directory(basename: &str) -> PathBuf {
    let pid = getpid();
    let hostname = ::common::sys::get_hostname();
    PathBuf::from("/tmp/rain-logs").join(format!("{}-{}-{}", basename, hostname, pid))
}
pub fn ensure_directory(dir: &Path, name: &str) -> Result<()> {
    if !dir.exists() {
        debug!("{} not found, creating ... {:?}", name, dir);
        if let Err(e) = create_dir_all(dir) {
            bail!(format!(
                "{} {:?} cannot by created: {}",
                name,
                dir,
                e.description()
            ));
        }
    } else if !dir.is_dir() {
        bail!("{} {:?} exists but it is not a directory", name, dir);
    }
    Ok(())
}
