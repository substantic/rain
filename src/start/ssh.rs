use ssh2::Session;

use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::io::BufRead;

use librain::errors::{Result};
use std::io::BufReader;
use start::common::{Readiness};

pub struct SshAuth {
    pub username: String,
    pub password: String,
}

impl SshAuth {
    pub fn new() -> Self {
        SshAuth {
            username: env!("USER").to_string(),
            password: "".to_string(),
        }
    }

    pub fn set_username(&mut self, username: &str) {
        self.username = username.to_string();
    }

    pub fn set_password(&mut self, password: &str) {
        self.password = password.to_string();
    }
}

pub struct SshProcess {
    name: String,
    host: String,
    stream: ::std::net::TcpStream,
    session: Session,
    pid: i32,
    readiness: Readiness
}

impl SshProcess {
    pub fn new(name: String, host: &str, auth: &SshAuth, readiness: Readiness) -> Result<Self> {
        let tcp = if host.contains(":") {
            ::std::net::TcpStream::connect(host)?
        } else {
            ::std::net::TcpStream::connect((host, 22))?
        };
        let mut sess = Session::new().unwrap();

        sess.handshake(&tcp)?;
        sess.userauth_password(&auth.username, &auth.password)?;

        Ok(Self {
            name: name,
            host: host.to_string(),
            stream: tcp,
            session: sess,
            pid: 0,
            readiness: readiness
        })
    }

    pub fn start(&mut self, command: &str, cwd: &Path, log_dir: &Path) -> Result<()> {

       /* Shell script that has following goals:
        - check that log files are accessible
        - start a new sub-shell with desired command
        - return PID of the new shell and then terminate
        */

        let log_out = log_dir.join(&format!("{}.out", self.name));
        let log_err = log_dir.join(&format!("{}.err", self.name));

        let shell_cmd = format!("\n\
touch {log_out:?} || (echo \"Error: Cannot create log file\"; exit 1)
touch {log_err:?} || (echo \"Error: Cannot create log file\"; exit 1)
({{
    cd {cwd:?} || exit 1;\n\
    {command}\n\
    }} > {log_out:?} 2> {log_err:?})&\n\
    echo \"Ok: $!\"\n\
    ", cwd=cwd, command=command, log_out=log_out, log_err=log_err);

        let mut channel = self.session.channel_session()?;
        channel.exec("/bin/sh")?;
        channel.write_all(shell_cmd.as_bytes())?;
        channel.send_eof();
        let mut reader = BufReader::new(channel);
        let mut first_line = String::new();
        reader.read_line(&mut first_line)?;

        if first_line.starts_with("Ok: ") {
            self.pid = first_line[4..]
                .trim()
                .parse()
                .map_err(|e| format!("Internal error, value is not integer: {}", first_line))?;
        } else if first_line.starts_with("Error: ") {
            bail!("Remote process at {}, the following error occurs: {}",
                  self.host, &first_line[7..]);
        } else {
            bail!("Invalid line obtained from remote process: {}", first_line);
        }
        Ok(())
    }

    pub fn check_still_running(&self) -> Result<()>
    {
        let mut channel = self.session.channel_session()?;
        channel.exec(&format!("ps -p {}", self.pid))?;
        channel.wait_eof()?;
        channel.wait_close()?;
        if channel.exit_status()? != 0 {
            bail!("Remote process '{0}' terminated; \
                   process outputs can be found at remote side {0}.{{out/err}}",
                  self.name)
        }
        Ok(())
    }

    pub fn remove_file(&self, path: &Path) -> Result<bool> {
        let mut channel = self.session.channel_session()?;
        channel.exec(&format!("rm {:?}", path))?;
        channel.wait_eof()?;
        channel.wait_close()?;
        Ok(channel.exit_status()? == 0)
    }

    pub fn check_ready(&mut self) -> Result<bool> {
        self.check_still_running()?;

        match self.readiness {
            Readiness::IsReady => return Ok(true),
            Readiness::WaitingForReadyFile(ref path) => {
                if !self.remove_file(path)? {
                    return Ok(false);
                }
            }
        }
        info!("Process '{}' is ready", self.name);
        self.readiness = Readiness::IsReady;
        Ok(true)
    }

    pub fn kill(&mut self) -> Result<()> {
        let mut channel = self.session.channel_session()?;
        channel.exec(&format!("pkill -P {}", self.pid))?;
        channel.wait_eof()?;
        channel.wait_close()?;

        if let Readiness::WaitingForReadyFile(ref path) = self.readiness {
            self.remove_file(path)?; // we do care about return value
        }
        Ok(())
    }

}
