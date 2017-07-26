extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("capnp/graph.capnp")
        // can be chained as in: .file("capnp/service.capnp")
        .run().expect("schema compiler command");
}