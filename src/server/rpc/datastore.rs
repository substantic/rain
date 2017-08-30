use datastore_capnp::data_store;
use server::state::State;

pub struct DataStoreImpl {
    state: State,
}

impl DataStoreImpl {
    pub fn new(state: &State) -> Self {
        Self { state: state.clone() }
    }
}

impl data_store::Server for DataStoreImpl {
}
