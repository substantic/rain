
use std::process::Command;
use std::path::{Path, PathBuf};
use std::net::SocketAddr;
use start::common::{Readiness};
use start::process::{Process};
use start::ssh::{SshProcess, SshAuth};
use librain::errors::{Error, Result};

use nix::unistd::{gethostname, getpid};
use std::io::BufReader;
use std::io::BufRead;
use std::fs::File;


/// Starts server & workers
pub struct Starter {
    /// Number of local worker that will be spawned
    n_local_workers: u32,

    /// Listening address of server
    server_listen_address: SocketAddr,

    /// Directory where logs are stored (absolute path)
    log_dir: PathBuf,

    /// Spawned and running processes
    processes: Vec<Process>,

    /// Spawned and running processes
    remote_processes: Vec<SshProcess>,

    /// Ssh authentication
    auth: SshAuth,
}

fn read_host_file(path: &Path) -> Result<Vec<String>> {
    let file = BufReader::new(File::open(path)
        .map_err(|e| format!("Cannot open worker host file {:?}: {}", path,
                             ::std::error::Error::description(&e)))?);
    let mut result = Vec::new();
    for line in file.lines() {
        let line = line?;
        let trimmed_line = line.trim();
        if (!trimmed_line.is_empty() && !trimmed_line.starts_with("#")) {
            result.push(trimmed_line.to_string());
        }
    }
    Ok(result)
}

impl Starter {
    pub fn new(local_workers: u32, server_listen_address: SocketAddr, log_dir: &Path) -> Self {
        Self {
            n_local_workers: local_workers,
            server_listen_address,
            log_dir: ::std::env::current_dir().unwrap().join(log_dir), // Make it absolute
            processes: Vec::new(),
            remote_processes: Vec::new(),
            auth: SshAuth::new(),
        }
    }

    pub fn has_processes(&self) -> bool {
        !self.processes.is_empty()
    }

    pub fn read_auth_file(&mut self, path: &Path) -> Result<()> {
        let mut file = BufReader::new(File::open(path)
            .map_err(|e| format!("Cannot open auth file {:?}: {}", path,
                                ::std::error::Error::description(&e)))?);
        let mut username = String::new();
        let mut password = String::new();
        file.read_line(&mut username)?;
        file.read_line(&mut password)?;

        self.auth.set_username(username.trim());
        self.auth.set_password(password.trim());
        Ok(())
    }

    /// Main method of starter that launch everything
    pub fn start(&mut self,
                worker_host_file: Option<&Path>) -> Result<()> {

        let worker_hosts = if let Some(ref path) = worker_host_file {
            read_host_file(path)?
        } else {
            Vec::new()
        };

        if self.n_local_workers == 0 && worker_hosts.is_empty() {
            bail!("No workers are specified.");
        }

        self.start_server()?;
        self.busy_wait_for_ready()?;

        if self.n_local_workers > 0 {
            self.start_local_workers()?;
        }
        if !worker_hosts.is_empty() {
            self.start_remote_workers(&worker_hosts)?;
        }
        self.busy_wait_for_ready()?;
        Ok(())
    }

    /// Path to "rain" binary
    pub fn local_rain_program(&self) -> String {
        ::std::env::args().nth(0).unwrap()
    }

    fn spawn_process(
        &mut self,
        name: &str,
        ready_file: &Path,
        command: &mut Command,
    ) -> Result<&Process> {
        self.processes.push(Process::spawn(
            &self.log_dir,
            name,
            Readiness::WaitingForReadyFile(ready_file.to_path_buf()),
            command,
        )?);
        Ok(&self.processes.last().unwrap())
    }

    /// Create a temporory filename
    fn create_tmp_filename(&self, name: &str) -> PathBuf {
        ::std::env::temp_dir().join(format!("rain-{}-{}", getpid(), name))
    }

    fn start_server(&mut self) -> Result<()> {
        let ready_file = self.create_tmp_filename("server-ready");
        let rain = self.local_rain_program();
        let server_address = format!("{}", self.server_listen_address);
        info!("Starting local server ({})", server_address);
        let process = self.spawn_process(
            "server",
            &ready_file,
            Command::new(rain)
                .arg("server")
                .arg("--listen")
                .arg(&server_address)
                .arg("--ready_file")
                .arg(&ready_file),
        )?;
        info!("Server pid = {}", process.id());
        Ok(())
    }

    fn start_remote_workers(&mut self, worker_hosts: &Vec<String>) -> Result<()>
    {
        info!("Starting {} remote worker(s)", worker_hosts.len());
        let rain = self.local_rain_program(); // TODO: configurable path for remotes
        let dir = ::std::env::current_dir().unwrap(); // TODO: Do it configurable
        let server_address = self.server_address();

        for (i, host) in worker_hosts.iter().enumerate() {
            info!("Connecting to {} (remote log dir: {:?})", host, self.log_dir);
            let ready_file = self.create_tmp_filename(&format!("worker-{}-ready", i));
            let name = format!("worker-{}", i);
            let mut process = SshProcess::new(
                name, host, &self.auth,
                Readiness::WaitingForReadyFile(ready_file.to_path_buf()))?;
            let command = format!(
                "{rain} worker {server_address} --ready_file {ready_file:?}",
                rain=rain, server_address=server_address, ready_file=ready_file);
            process.start(&command, &dir, &self.log_dir)?;
            self.remote_processes.push(process);
        }
        Ok(())
    }

    fn server_address(&self) -> String {
        let mut buf = [0u8; 256];
        gethostname(&mut buf).unwrap();
        format!("{}:{}",
                ::std::str::from_utf8(&buf).unwrap(),
                self.server_listen_address.port())
    }

    fn start_local_workers(&mut self) -> Result<()> {
        info!("Starting {} local workers", self.n_local_workers);
        let server_address = self.server_address();
        let rain = self.local_rain_program();
        for i in 0..self.n_local_workers {
            let ready_file = self.create_tmp_filename(&format!("worker-{}-ready", i));
            let process = self.spawn_process(
                &format!("worker-{}", i),
                &ready_file,
                Command::new(&rain)
                    .arg("worker")
                    .arg(&server_address)
                    .arg("--ready_file")
                    .arg(&ready_file),
            )?;
        }
        Ok(())
    }

    /// Waits until all processes are ready
    pub fn busy_wait_for_ready(&mut self) -> Result<()> {
        let mut timeout_ms = 50; // Timeout, it it increased every cycle upto 1.5 seconds
        while 0 != self.check_all_ready()? {
            ::std::thread::sleep(::std::time::Duration::from_millis(timeout_ms));
            if timeout_ms < 1500 {
                timeout_ms += 50;
            }
        }
        Ok(())
    }

    /// Checks that all registered processes are still running
    /// and check if their ready_files are not createn
    pub fn check_all_ready(&mut self) -> Result<u32> {
        let mut not_ready = 0u32;
        // Here we intentionally goes through all processes
        // even we found first non-ready one, since we also
        // want to check that other processes are not terminated
        for mut process in &mut self.processes {
            if !process.check_ready()? {
                not_ready += 1;
            }
        }

        for mut process in &mut self.remote_processes {
            if !process.check_ready()? {
                not_ready += 1;
            }
        }
        Ok(not_ready)
    }

    /// This is cleanup method, so we want to silent errors
    pub fn kill_all(&mut self) {
        for mut process in ::std::mem::replace(&mut self.processes, Vec::new()) {
            match process.kill() {
                Ok(()) => {}
                Err(e) => debug!("Kill failed: {}", e.description()),
            };
        }

        for mut process in ::std::mem::replace(&mut self.remote_processes, Vec::new()) {
            match process.kill() {
                Ok(()) => {}
                Err(e) => debug!("Kill failed: {}", e.description()),
            };
        }
    }
}
