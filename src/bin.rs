extern crate librain;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate tokio_core;
extern crate env_logger;

use librain::{server, worker, VERSION};
use clap::ArgMatches;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};

const DEFAULT_SERVER_PORT: u16 = 7210;
const DEFAULT_WORKER_PORT: u16 = 0;
const CLIENT_PROTOCOL_VERSION: i32 = 0;
const WORKER_PROTOCOL_VERSION: i32 = 0;

fn run_server(_global_args: &ArgMatches, cmd_args: &ArgMatches) {
    let port = value_t!(cmd_args, "PORT", u16).unwrap_or(DEFAULT_SERVER_PORT);
    info!("Starting Rain {} server at port {}", VERSION, port);

    let mut tokio_core = tokio_core::reactor::Core::new().unwrap();
    let state = server::state::State::new(tokio_core.handle(), port);
    state.start();
    loop {
        tokio_core.turn(None);
        state.turn();
    }
}


fn run_worker(_global_args: &ArgMatches, cmd_args: &ArgMatches) {
    let port = value_t!(cmd_args, "PORT", u16).unwrap_or(DEFAULT_WORKER_PORT);
    let server_address = value_t!(cmd_args, "SERVER_ADDRESS", SocketAddr).unwrap_or_else(|_| {
        SocketAddr::new(
            value_t_or_exit!(cmd_args, "SERVER_ADDRESS", IpAddr),
            DEFAULT_SERVER_PORT,
        )
    });
    info!("Starting Rain {} as worker", VERSION);

    let listen_address =    SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);

    let mut tokio_core = tokio_core::reactor::Core::new().unwrap();
    let state =  worker::state::State::new(tokio_core.handle(), 1);
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
            (@arg PORT: -p --port +takes_value "Listening port (default 7210)")
            )
        (@subcommand worker =>
            (about: "Start a worker and connect to a given server.")
            (@arg SERVER_ADDRESS: +required "Server address ADDR[:PORT] (default port is 7210)")
            (@arg PORT: -p --port +takes_value "Listening port (default 0 = autoassign)")
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
