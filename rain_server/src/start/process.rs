use std::path::Path;
use std::process::{Child, Command};
use error_chain::bail;

use rain_core::errors::Result;

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
        command.stdout(::std::fs::File::create(
            log_dir.join(&format!("{}.out", name)),
        )?);
        command.stderr(::std::fs::File::create(
            log_dir.join(&format!("{}.err", name)),
        )?);

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
                    Ok(_) => log::debug!("Ready file of killed process removed"),
                    Err(e) => log::error!(
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

        log::info!("Process '{}' is ready", self.name);
        // Here we can get only when we changed the readiness status
        self.ready = Readiness::IsReady;
        Ok(true)
    }
}
