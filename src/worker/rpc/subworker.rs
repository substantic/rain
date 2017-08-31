use worker::StateRef;
use worker::graph::SubworkerRef;
use subworker_capnp::subworker_upstream;
use futures::Future;
use capnp;
use capnp::capability::Promise;


use SUBWORKER_PROTOCOL_VERSION;

pub struct SubworkerUpstreamImpl {
    state: StateRef,
}

impl SubworkerUpstreamImpl {
    pub fn new(state: &StateRef) -> Self {
        Self { state: state.clone() }
    }
}

impl Drop for SubworkerUpstreamImpl {
    fn drop(&mut self) {
        panic!("Lost connection to subworker");
    }
}

impl subworker_upstream::Server for SubworkerUpstreamImpl {

    fn register(&mut self,
              params: subworker_upstream::RegisterParams,
              mut _results: subworker_upstream::RegisterResults)
              -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());

        if params.get_version() != SUBWORKER_PROTOCOL_VERSION {
            return Promise::err(capnp::Error::failed(
                format!("Invalid subworker protocol; expected = {}",
                        SUBWORKER_PROTOCOL_VERSION)));
        }

        let control = pry!(params.get_control());
        let subworker = SubworkerRef::new(params.get_subworker_id(), control);
        self.state.add_subworker(subworker);
        Promise::ok(())
    }
}
