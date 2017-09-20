
use std::process::Command;
use std::path::{Path, PathBuf};
use std::net::SocketAddr;
use start::process::{Process, Readiness};

use librain::errors::{Error, Result};

use nix::unistd::getpid;


/// Starts server & workers
pub struct Starter {
    /// Number of local worker that will be spawned
    n_local_workers: u32,

    /// Listening address of server
    server_listen_address: SocketAddr,

    /// Directory where logs are stored
    log_dir: PathBuf,

    /// Spawned and running processes
    processes: Vec<Process>,
}


impl Starter {
    pub fn new(local_workers: u32, server_listen_address: SocketAddr, log_dir: PathBuf) -> Self {
        Self {
            n_local_workers: local_workers,
            server_listen_address,
            log_dir,
            processes: Vec::new(),
        }
    }

    /// Main method of starter that launch everything
    pub fn start(&mut self) -> Result<()> {
        self.start_server()?;
        self.busy_wait_for_ready()?;
        self.start_local_workers()?;
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

    fn start_local_workers(&mut self) -> Result<()> {
        info!("Starting {} local workers", self.n_local_workers);
        let rain = self.local_rain_program();
        let server_address: String = format!("127.0.0.1:{}", self.server_listen_address.port());
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
        while (0 != self.check_all_ready()?) {
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
    }
}
