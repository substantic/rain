extern crate librain;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate tokio_core;
extern crate env_logger;
extern crate num_cpus;
extern crate nix;

use std::process::exit;
use std::path::{Path, PathBuf};
use std::error::Error;

use librain::{server, worker, VERSION};
use clap::ArgMatches;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};

const DEFAULT_SERVER_PORT: u16 = 7210;
const DEFAULT_WORKER_PORT: u16 = 0;
const CLIENT_PROTOCOL_VERSION: i32 = 0;
const WORKER_PROTOCOL_VERSION: i32 = 0;


fn parse_listen_arg(args: &ArgMatches, default_port: u16) -> SocketAddr {
    if !args.is_present("LISTEN") {
        return SocketAddr::new(IpAddr::V4(
            Ipv4Addr::new(0, 0, 0, 0)), default_port)
    }

    value_t!(args, "LISTEN", SocketAddr).unwrap_or_else(|_| {
        match(value_t!(args, "LISTEN", IpAddr)) {
            Ok(ip) => SocketAddr::new(ip, default_port),
            _ => SocketAddr::new(
                  IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                  value_t_or_exit!(args, "LISTEN", u16))
        }
    })
}


fn run_server(_global_args: &ArgMatches, cmd_args: &ArgMatches) {
    let listen_address = parse_listen_arg(cmd_args, DEFAULT_SERVER_PORT);
    info!("Starting Rain {} server at port {}", VERSION, listen_address);

    let mut tokio_core = tokio_core::reactor::Core::new().unwrap();
    let state = server::state::State::new(tokio_core.handle(), listen_address);
    state.start();
    loop {
        tokio_core.turn(None);
        state.turn();
    }
}


// Creates a working directory of the following scheme prefix + "/rain/" + base_name + process_pid
// It checks that 'prefix' exists, but not the full path
fn make_working_directory(prefix: &Path, base_name: &str) -> Result<PathBuf, String> {
    if !prefix.exists() {
        return Err(format!("Working directory prefix {:?} does not exists", prefix));
    }

    if !prefix.is_dir() {
        return Err(format!("Working directory prefix {:?} is not directory", prefix));
    }

    let pid = nix::unistd::getpid();
    let work_dir = prefix.join("rain").join(
        format!("{}{}", base_name, pid));

    if work_dir.exists() {
        return Err(format!("Working directory {:?} already exists", work_dir));
    }

    debug!("Creating working directory {:?}", work_dir);
    if let Err(e) = std::fs::create_dir_all(work_dir.clone()) {
        return Err(format!("Working directory {:?} cannot by created: {}",
                           work_dir, e.description()));
    }
    Ok(work_dir)
}


fn run_worker(_global_args: &ArgMatches, cmd_args: &ArgMatches) {
    let listen_address = parse_listen_arg(cmd_args, DEFAULT_WORKER_PORT);
    let server_address = value_t!(cmd_args, "SERVER_ADDRESS", SocketAddr).unwrap_or_else(|_| {
        SocketAddr::new(
            value_t_or_exit!(cmd_args, "SERVER_ADDRESS", IpAddr),
            DEFAULT_SERVER_PORT,
        )
    });

    let cpus = if cmd_args.is_present("CPUS") {
        value_t_or_exit!(cmd_args, "CPUS", u32)
    } else {
        debug!("Detecting number of cpus");
        let cpus = num_cpus::get();
        if (cpus < 1) {
            error!("Autodetection of CPUs failed. Use --cpus argument.");
            exit(1);
        }
        cpus as u32
    };

    let work_dir_prefix = Path::new(cmd_args.value_of("WORK_DIR")
        .unwrap_or("/tmp"));

    let work_dir = make_working_directory(work_dir_prefix, "worker-")
        .unwrap_or_else(|e| {
            error!("{}", e);
            exit(1);
        });

    info!("Starting Rain {} as worker", VERSION);
    info!("Resources: {} cpus", cpus);
    info!("Working directory: {:?}", work_dir);

    let mut tokio_core = tokio_core::reactor::Core::new().unwrap();
    let state =  worker::state::State::new(tokio_core.handle(), work_dir, cpus);
    state.start(server_address, listen_address);
    loop {
        tokio_core.turn(None);
        state.turn();
    }
}

fn main() {
    // Temporary simple logger for better module log control, default level is INFO
    // TODO: replace with Fern or log4rs later
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init().unwrap();

    let args: ArgMatches = clap_app!(Rain =>
        (version: VERSION)
        (about: "Task-based workflow manager and executor (server and worker binary).")
        //(@arg debug: --debug "Enables debug mode (not much effect now - use RUST_LOG)")
        (@subcommand server =>
            (about: "Start a server, waiting for workers and clients.")
            (@arg LISTEN: -l --listen +takes_value "Listening port or port/address/address:port (default 0.0.0.0:7210)")
            )
        (@subcommand worker =>
            (about: "Start a worker and connect to a given server.")
            (@arg SERVER_ADDRESS: +required "Server address ADDR[:PORT] (default port is 7210)")
            (@arg LISTEN: -l --listen +takes_value "Listening port/address/address:port (default = 0.0.0.0:autoassign)")
            (@arg CPUS: --cpus +takes_value "Number of cpus (default = autoassign)")
            (@arg WORK_DIR: --workdir +takes_value "Working directory (default = /tmp)")
            )
        ).get_matches();

    //let debug = args.is_present("debug");

    match args.subcommand() {
        ("server", Some(ref cmd_args)) => run_server(&args, cmd_args),
        ("worker", Some(ref cmd_args)) => run_worker(&args, cmd_args),
        _ => {
            error!("No subcommand provided.");
            ::std::process::exit(1);
        }
    }
}
