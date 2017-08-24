use futures::unsync::oneshot::Sender;

use common::wrapped::WrappedRcRefCell;
use common::id::WorkerId;
use common::RcSet;
use server::task::Task;
use server::dataobj::DataObject;

pub struct Inner {
    /// Unique ID, here the registration socket address.
    id: WorkerId,

    /// Assigned tasks. The task state is stored in the `Task`.
    tasks: RcSet<Task>,

    /// Obects fully located on the worker.
    located: RcSet<DataObject>,

    /// Objects located or assigned to appear on the worker. Superset of `located`.
    assigned: RcSet<DataObject>,

    /// Control interface
    control: ::worker_capnp::worker_control::Client,

    // Resources. TODO: Extract into separate struct
    n_cpus: u32,
    free_n_cpus: u32,
}

pub type Worker = WrappedRcRefCell<Inner>;


// TODO: Functional cleanup of code below

impl Worker {
    pub fn push_task(&self, task: &Task) {
        self.get();
    }

    pub fn new(worker_id: WorkerId,
               control: ::worker_capnp::worker_control::Client,
               n_cpus: u32) -> Self {
        Self::wrap(Inner {
            id: worker_id,
            tasks: Default::default(),
            located: Default::default(),
            assigned: Default::default(),
            control: control,
            n_cpus: n_cpus,
            free_n_cpus: n_cpus,
        })
    }

    #[inline]
    pub fn get_id(&self) -> WorkerId {
        self.get().id
    }
}
