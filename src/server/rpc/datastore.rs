use datastore_capnp::data_store;
use server::state::StateRef;

pub struct DataStoreImpl {
    state: StateRef,
}

impl DataStoreImpl {
    pub fn new(state: &StateRef) -> Self {
        Self { state: state.clone() }
    }
}

impl data_store::Server for DataStoreImpl {
}
