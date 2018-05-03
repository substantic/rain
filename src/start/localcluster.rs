use start::starter::{Starter, StarterConfig};
use std::net::SocketAddr;
use std::error::Error;
use std::iter;
use client::client::Client;
use start::common::{default_logging_directory, ensure_directory};

pub struct LocalCluster {
    listen_addr: SocketAddr,
    starter: Starter,
}

impl LocalCluster {
    pub fn new(worker_count: usize, listen_port: u16, http_port: u16) -> Result<Self, Box<Error>> {
        let listen_addr = SocketAddr::new("127.0.0.1".parse()?, listen_port);
        let http_addr = SocketAddr::new("127.0.0.1".parse()?, http_port);

        let log_dir = default_logging_directory("rain");
        ensure_directory(&log_dir, "logging directory")?;
        let workers = iter::repeat(Some(1)).take(worker_count).collect();

        let config = StarterConfig::new(
            workers,
            listen_addr,
            http_addr,
            &log_dir,
            "".to_owned(),
            false,
            vec![],
        );

        let mut cluster = LocalCluster {
            listen_addr,
            starter: Starter::new(config),
        };
        cluster.starter.start()?;

        Ok(cluster)
    }

    pub fn create_client(&self) -> Result<Client, Box<Error>> {
        Client::new(self.listen_addr)
    }
}

impl Drop for LocalCluster {
    fn drop(&mut self) {
        self.starter.kill_all();
    }
}