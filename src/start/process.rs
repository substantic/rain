use std::process::{Child, Command, Stdio};
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::path::Path;

use librain::errors::Result;

use start::common::Readiness;

/// Struct that represents a process running under a starter
/// It is wrapper over `std::process::Child` with a string name
/// This string name indicates the name of logs in log dir
/// The class also monitors readiness of the process signalled by
/// ready file.

pub struct Process {
    /// Name of process, it is used for log names name.out/name.err
    name: String,

    /// Process handler
    child: Child,

    /// State of process readiness, it is changed in "check" function
    ready: Readiness,
}

impl Process {
    pub fn spawn(
        log_dir: &Path,
        name: &str,
        ready: Readiness,
        command: &mut Command,
    ) -> Result<Self> {
        let log_path_out_id =
            ::std::fs::File::create(log_dir.join(&format!("{}.out", name)))?.into_raw_fd();
        let log_path_err_id =
            ::std::fs::File::create(log_dir.join(&format!("{}.err", name)))?.into_raw_fd();

        let log_path_out_pipe = unsafe { Stdio::from_raw_fd(log_path_out_id) };
        let log_path_err_pipe = unsafe { Stdio::from_raw_fd(log_path_err_id) };

        command.stdout(log_path_out_pipe);
        command.stderr(log_path_err_pipe);

        Ok(Self {
            name: name.to_string(),
            child: command.spawn()?,
            ready,
        })
    }

    pub fn id(&self) -> u32 {
        self.child.id()
    }

    pub fn kill(&mut self) -> Result<()> {
        if let Readiness::WaitingForReadyFile(ref path) = self.ready {
            if path.exists() {
                use std::error::Error;
                // This error is non fatal, so we just log an error and continue
                match ::std::fs::remove_file(path) {
                    Ok(_) => debug!("Ready file of killed process removed"),
                    Err(e) => error!(
                        "Cannot remove ready file for killed process: {}",
                        e.description()
                    ),
                }
            }
        }
        self.child.kill()?;
        Ok(())
    }

    pub fn check_run(&mut self) -> Result<()> {
        if let Some(exit_code) = self.child.try_wait()? {
            bail!(
                "Process '{1}' terminated with exit code {0}; \
                 process outputs can be found in {1}.{{out/err}}",
                exit_code,
                self.name
            );
        }
        Ok(())
    }

    pub fn check_ready(&mut self) -> Result<bool> {
        self.check_run()?;

        // The following code is a little-bit weird because of borrow-checker
        match self.ready {
            Readiness::IsReady => return Ok(true),
            Readiness::WaitingForReadyFile(ref path) => {
                if path.exists() {
                    ::std::fs::remove_file(path)?;
                } else {
                    return Ok(false);
                }
            }
        };

        info!("Process '{}' is ready", self.name);
        // Here we can get only when we changed the readiness status
        self.ready = Readiness::IsReady;
        Ok(true)
    }
}
