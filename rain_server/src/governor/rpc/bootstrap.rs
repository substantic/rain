use capnp::capability::Promise;
use common::convert::FromCapnp;
use common::id::DataObjectId;
use governor::StateRef;
use governor_capnp::governor_bootstrap;

impl GovernorBootstrapImpl {
    pub fn new(state: &StateRef) -> Self {
        GovernorBootstrapImpl {
            state: state.clone(),
        }
    }
}

pub struct GovernorBootstrapImpl {
    state: StateRef,
}

impl governor_bootstrap::Server for GovernorBootstrapImpl {
    fn fetch(
        &mut self,
        params: governor_bootstrap::FetchParams,
        mut results: governor_bootstrap::FetchResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let id = DataObjectId::from_capnp(&pry!(params.get_id()));
        let offset = params.get_offset() as usize;
        let size = params.get_size() as usize;
        let mut state = self.state.get_mut();

        let transport_view = state.get_transport_view(id);
        let mut results = results.get();

        if transport_view.is_none() {
            debug!("Governor responding 'not here' for id={}", id);
            results.get_status().set_not_here(());
            return Promise::ok(());
        }
        let transport_view = transport_view.unwrap();
        let slice = transport_view.get_slice();

        results.reborrow().get_status().set_ok(());

        if offset < slice.len() {
            let end = if offset + size < slice.len() {
                offset + size
            } else {
                slice.len()
            };
            debug!("Sending range [{}..{}]", offset, end);
            results.set_data(slice[offset..end].into());
        } else {
            debug!("Fetch out of range");
        }

        if params.get_include_info() {
            let obj_ref = state.graph.objects.get(&id).unwrap();
            let obj = obj_ref.get();
            results.set_transport_size(slice.len() as u64);
            results.set_info(&::serde_json::to_string(&obj.info).unwrap());
        }
        Promise::ok(())
    }
}
