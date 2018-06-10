use std::error::Error;
use std::io::BufRead;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use rain_core::errors::Result;
use start::common::Readiness;
use std::io::BufReader;

pub struct User {
    pub username: String,
    pub password: String,
}

pub struct RemoteProcess {
    name: String,
    host: String,
    pid: i32,
    readiness: Readiness,
}

impl RemoteProcess {
    pub fn new(name: String, host: &str, readiness: Readiness) -> Self {
        RemoteProcess {
            name,
            host: host.to_string(),
            pid: 0,
            readiness,
        }
    }

    fn create_ssh_command(&self) -> Command {
        let mut command = Command::new("ssh");
        command
            .arg("-o StrictHostKeyChecking=no")
            .arg(&self.host)
            .arg("/bin/sh")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    pub fn run_ssh_first_line(&self, command: &str) -> Result<String> {
        let mut child = self.create_ssh_command()
            .spawn()
            .map_err(|e| format!("Start of 'ssh' failed: {}", e.description()))?;
        {
            let stdin = child.stdin.as_mut().unwrap();
            stdin.write_all(command.as_bytes())?;
            stdin.flush()?;
        }
        let mut output = String::new();
        {
            let mut reader = BufReader::new(child.stdout.as_mut().unwrap());
            reader.read_line(&mut output)?;
        }

        if output.is_empty() {
            let out = child.wait_with_output()?;
            bail!("ssh failed with: {}", ::std::str::from_utf8(&out.stderr)?);
        }

        Ok(output)
    }

    pub fn run_ssh(&self, command: &str) -> Result<(String, String)> {
        let mut child = self.create_ssh_command()
            .spawn()
            .map_err(|e| format!("Start of 'ssh' failed: {}", e.description()))?;
        {
            let stdin = child.stdin.as_mut().unwrap();
            stdin.write_all(command.as_bytes())?;
            stdin.flush()?;
        }

        let output = child.wait_with_output()?;
        let stderr = ::std::str::from_utf8(&output.stderr)?;
        let stdout = ::std::str::from_utf8(&output.stdout)?;
        if !output.status.success() {
            bail!("Connection to remote site failed: {}", stderr);
        }
        Ok((stdout.to_string(), stderr.to_string()))
    }

    pub fn start(&mut self, command: &str, cwd: &Path, log_dir: &Path) -> Result<()> {
        /* Shell script that has following goals:
        - check that log files are accessible
        - start a new sub-shell with desired command
        - return PID of the new shell and then terminate
        */

        let log_out = log_dir.join(&format!("{}.out", self.name));
        let log_err = log_dir.join(&format!("{}.err", self.name));

        let shell_cmd = format!(
            "\n
mkdir -p {log_dir:?} || (echo \"Error: Cannot create log directory\"; exit 1)\n
touch {log_out:?} || (echo \"Error: Cannot create log file\"; exit 1)\n
touch {log_err:?} || (echo \"Error: Cannot create log file\"; exit 1)\n
({{\n
    cd {cwd:?} || exit 1;\n
    {command}\n
    }} > {log_out:?} 2> {log_err:?})&\n
    echo \"Ok: $!\"\n
    ",
            cwd = cwd,
            command = command,
            log_dir = log_dir,
            log_out = log_out,
            log_err = log_err
        );

        let stdout = self.run_ssh_first_line(&shell_cmd)?;

        if stdout.starts_with("Ok: ") {
            self.pid = stdout[4..]
                .trim()
                .parse()
                .map_err(|_| format!("Internal error, value is not integer: {}", stdout))?;
        } else if stdout.starts_with("Error: ") {
            bail!(
                "Remote process at {}, the following error occurs: {}",
                self.host,
                &stdout[7..]
            );
        } else {
            bail!("Invalid line obtained from remote process: '{}'", stdout);
        }
        Ok(())
    }

    pub fn check_ready(&mut self) -> Result<bool> {
        let mut shell_cmd = format!(
            "ps -p {pid} > /dev/null || (echo 'Not running'; exit 1)\n",
            pid = self.pid
        );
        let is_ready = match self.readiness {
            Readiness::IsReady => true,
            Readiness::WaitingForReadyFile(ref path) => {
                shell_cmd += &format!("rm {:?} && echo 'Ready' && exit 0\n", path);
                false
            }
        };
        shell_cmd += "echo 'Ok'";

        let (stdout, _stderr) = self.run_ssh(&shell_cmd)?;
        Ok(match stdout.trim() {
            "Ok" => is_ready,
            "Ready" => {
                info!("Remote process {} is ready", self.name);
                self.readiness = Readiness::IsReady;
                true
            }
            "Not Running" => bail!("Remote process {} is not running", self.name),
            output => bail!(
                "Unexpected output from remote process {}: {}",
                self.name,
                output
            ),
        })
    }

    pub fn kill(&mut self) -> Result<()> {
        let shell_cmd = match self.readiness {
            Readiness::IsReady => format!("pkill -P {pid}; exit 0", pid = self.pid),
            Readiness::WaitingForReadyFile(ref path) => format!(
                "pkill -P {pid}; rm -f {ready_file:?}; exit 0",
                pid = self.pid,
                ready_file = path
            ),
        };
        self.run_ssh(&shell_cmd)?;
        Ok(())
    }
}
