extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .file("capnp/common.capnp")
        .file("capnp/server.capnp")
        .file("capnp/client.capnp")
        .file("capnp/worker.capnp")
        .file("capnp/monitor.capnp")
        .run()
        .expect("schema compiler command");
}
