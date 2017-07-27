
mod common;
mod worker;
mod server;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate bitflags;
extern crate capnp;
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
#[macro_use]
extern crate capnp_rpc;

pub mod gate_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/gate_capnp.rs"));
}

pub mod client_capnp {
    include!(concat!(env!("OUT_DIR"), "/capnp/client_capnp.rs"));
}


fn main() {
    // Command line usage:
    // SERVER:
    // ./rain server <PORT>
    // WORKER:
    // ./rain worker <SERVER_ADDRESS>

    let matches = clap_app!(Rain =>
        (version: VERSION)
        (about: "TODO")
        (@arg debug: --debug "Enables debug mode")
        (@subcommand server =>
            (about: "")
            (@arg PORT: +required "Listening port"))
        (@subcommand client =>
            (about: "Connect to an upstream")
            (@arg SERVER_ADDRESS: +required "TODO")))
        .get_matches();

    let debug = matches.is_present("debug");

    info!("Rain {}", VERSION);
    if debug {
        info!("Debug mode enabled");
    }

    let mut tokio_core = tokio_core::reactor::Core::new().unwrap();

    // SERVER
    if let Some(server_matches) = matches.subcommand_matches("server") {
        let port = value_t_or_exit!(server_matches.value_of("PORT"), u16);
        let state = server::state::State::new(tokio_core.handle(), port);
        state.start();
        println!("PORT = {}", port);
        loop {
            tokio_core.turn(None);
            state.turn();
        }
    }
}
