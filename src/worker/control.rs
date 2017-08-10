
use worker::state::State;
use worker_capnp::worker_control;
use capnp::capability::Promise;
use std::process::exit;

pub struct WorkerControlImpl {
    state: State,
}

impl WorkerControlImpl {
    pub fn new(state: &State) -> Self {
        Self { state: state.clone() }
    }
}

impl Drop for WorkerControlImpl {
    fn drop(&mut self) {
        error!("Lost connection to the server");
        // exit(1);
    }
}

impl worker_control::Server for WorkerControlImpl {

    /*fn submit(&mut self,
              params: worker_service::SubmitParams,
              mut _results: worker_service::SubmitResults)
              -> Promise<(), ::capnp::Error> {
        let tasks = pry!(pry!(params.get()).get_tasks());
        debug!("New task submission ({} tasks)", tasks.len());
        for t in tasks.iter() {
            let task = Task::new(t.get_id(),
                                 t.get_procedure_id(),
                                 Vec::new(),
                                 pry!(t.get_config()).to_vec());
            self.core.add_task(task);
        }
        Promise::ok(())
    }*/
}
