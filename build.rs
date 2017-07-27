extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("capnp/gate.capnp")
        .file("capnp/client.capnp")
        .file("capnp/graph.capnp")
        .run().expect("schema compiler command");
}
