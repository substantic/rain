extern crate librain;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate tokio_core;
extern crate env_logger;

use librain::{server, VERSION};
use clap::{ArgMatches};
use std::net::{SocketAddr, IpAddr};

const DEFAULT_SERVER_PORT: u16 = 7210;
const DEFAULT_WORKER_PORT: u16 = 0;

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
    let server_addr = value_t!(cmd_args, "SERVER_ADDRESS", SocketAddr).unwrap_or_else(
            |_| SocketAddr::new(value_t_or_exit!(cmd_args, "SERVER_ADDRESS", IpAddr), DEFAULT_SERVER_PORT)
        );
    info!("Starting Rain {} worker at port {} with upstream {}", VERSION, port, server_addr);
    // TODO: Actually run :-)
    //let mut tokio_core = tokio_core::reactor::Core::new().unwrap();
}

fn run_client(_global_args: &ArgMatches, cmd_args: &ArgMatches) {
    let server_addr = value_t!(cmd_args, "SERVER_ADDRESS", SocketAddr).unwrap_or_else(
            |_| SocketAddr::new(value_t_or_exit!(cmd_args, "SERVER_ADDRESS", IpAddr), DEFAULT_SERVER_PORT)
        );
    info!("Starting Rain {} client for server {}", VERSION, server_addr);
    // TODO: Actually run :-)
    //let mut tokio_core = tokio_core::reactor::Core::new().unwrap();
}


fn main() {
    // Temporary simple logger for better module log control, default level is INFO
    // TODO: replace with Fern or log4rs later
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init().unwrap();

    let args :ArgMatches = clap_app!(Rain =>
        (version: VERSION)
        (about: "Task-based workflow manager and executor (server and worker binary).")
        //(@arg debug: --debug "Enables debug mode (not much effect now - use RUST_LOG)")
        (@subcommand server =>
            (about: "Start a server, waiting for workers and clients.")
            (@arg PORT: -p --port +takes_value "Listening port (default 7210)")
            )
        (@subcommand worker =>
            (about: "Start a worker and connect to a given server.")
            (@arg SERVER_ADDRESS: +required "Server address ADDR[:PORT] (default port is 0 = autoassign)")
            (@arg PORT: -p --port +takes_value "Listening port (default 0)")
            )
        (@subcommand client =>
            (about: "Connect to upstream server as a client.")
            (@arg SERVER_ADDRESS: +required "Server address ADDR[:PORT] (default port is 7210)")
            )
        ).get_matches();

    //let debug = args.is_present("debug");

    match args.subcommand() {
        ("server", Some(ref cmd_args)) => run_server(&args, cmd_args),
        ("worker", Some(ref cmd_args)) => run_worker(&args, cmd_args),
        ("client", Some(ref cmd_args)) => run_client(&args, cmd_args),
        _ => {
            error!("No subcommand provided.");
            ::std::process::exit(1);
        },
    }
}
