extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
//        .src_prefix("capnp")
        .file("capnp/common.capnp")
        .file("capnp/gate.capnp")
        .file("capnp/client.capnp")
        .file("capnp/graph.capnp")
        .file("capnp/datastore.capnp")
        .file("capnp/worker.capnp")
        .run().expect("schema compiler command");
}
/*
        .file("capnp/common.capnp")
        .file("capnp/gate.capnp")
        .file("capnp/client.capnp")
        .file("capnp/graph.capnp")
        .file("capnp/datastore.capnp")
        .file("capnp/worker.capnp")
        */
