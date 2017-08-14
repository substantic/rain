use capnp::capability::Promise;
use capnp;
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

    /*
    fn pull_data(
        &mut self,
        _: data_store::PullDataParams,
        mut results: data_store::PullDataResults,
    ) -> Promise<(), ::capnp::Error> {
        Promise::ok(())
    }
    */
}
