use super::client::Client;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Command, Stdio};

pub struct LocalCluster {
    listen_addr: SocketAddr,
    binary: PathBuf,
}

impl LocalCluster {
    pub fn new(binary: &str) -> Result<Self, Box<Error>> {
        let mut cluster = LocalCluster {
            binary: PathBuf::from(binary),
            listen_addr: SocketAddr::new("127.0.0.1".parse()?, 7210),
        };
        cluster.start()?;

        Ok(cluster)
    }

    fn start(&mut self) -> Result<(), Box<Error>> {
        Command::new(&self.binary)
            .arg("start")
            .arg("--listen")
            .arg(self.listen_addr.to_string())
            .arg("--simple")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?
            .wait()?;

        Ok(())
    }

    pub fn create_client(&self) -> Result<Client, Box<Error>> {
        Client::new(self.listen_addr)
    }
}

impl Drop for LocalCluster {
    #![allow(unused_must_use)]
    fn drop(&mut self) {
        Client::new(self.listen_addr).unwrap().terminate_server();
    }
}
