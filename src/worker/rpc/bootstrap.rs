use capnp::capability::Promise;
use worker_capnp::worker_bootstrap;
use common::id::DataObjectId;
use common::convert::FromCapnp;
use worker::StateRef;

impl WorkerBootstrapImpl {
    pub fn new(state: &StateRef) -> Self {
        WorkerBootstrapImpl {
            state: state.clone(),
        }
    }
}

pub struct WorkerBootstrapImpl {
    state: StateRef,
}

impl worker_bootstrap::Server for WorkerBootstrapImpl {
    fn fetch(
        &mut self,
        params: worker_bootstrap::FetchParams,
        mut results: worker_bootstrap::FetchResults,
    ) -> Promise<(), ::capnp::Error> {
        let params = pry!(params.get());
        let id = DataObjectId::from_capnp(&pry!(params.get_id()));
        let offset = params.get_offset() as usize;
        let size = params.get_size() as usize;
        let mut state = self.state.get_mut();

        let transport_view = state.get_transport_view(id);
        let mut results = results.get();

        if transport_view.is_none() {
            debug!("Worker responding 'not here' for id={}", id);
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

        if params.get_include_metadata() {
            let mut metadata = results.get_metadata().unwrap();
            metadata.set_size(slice.len() as i64);
            let obj_ref = state.graph.objects.get(&id).unwrap();
            let obj = obj_ref.get();
            metadata.set_data_type(obj.data_type.to_capnp());
            obj.attributes
                .to_capnp(&mut metadata.get_attributes().unwrap());
        }
        Promise::ok(())
    }
}
