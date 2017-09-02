use futures::unsync::oneshot::Sender;
use std::net::SocketAddr;
use std::fmt;

use common::wrapped::WrappedRcRefCell;
use common::RcSet;
use common::id::WorkerId;
use common::resources::Resources;
use super::{TaskRef, DataObjectRef, Graph};
use errors::Result;

pub struct Worker {
    /// Unique ID, here the registration socket address.
    id: WorkerId,

    /// Assigned tasks. The task state is stored in the `Task`.
    pub(super) tasks: RcSet<TaskRef>,

    /// Obects fully located on the worker.
    pub(super) located: RcSet<DataObjectRef>,

    /// Objects located or assigned to appear on the worker. Superset of `located`.
    pub(super) assigned: RcSet<DataObjectRef>,

    /// Control interface. Optional for testing and modelling.
    control: Option<::worker_capnp::worker_control::Client>,

    // Resources. TODO: Extract resources into separate struct
    resources: Resources,
    free_resources: Resources,
}

pub type WorkerRef = WrappedRcRefCell<Worker>;

impl WorkerRef {
    pub fn new(graph: &mut Graph,
               address: SocketAddr,
               control: Option<::worker_capnp::worker_control::Client>,
               resources: Resources) -> Result<Self> {
        if graph.workers.contains_key(&address) {
            bail!("Graph already contains worker {}", address);
        }
        debug!("Creating worker {}", address);
        let s = WorkerRef::wrap(Worker {
            id: address,
            tasks: Default::default(),
            located: Default::default(),
            assigned: Default::default(),
            control: control,
            resources: resources.clone(),
            free_resources: resources,
        });
        // add to graph
        graph.workers.insert(s.get().id, s.clone());
        Ok(s)
    }

    pub fn delete(self, graph: &mut Graph) {
        debug!("Deleting worker {}", self.get_id());
        // remove from objects
        for o in self.get_mut().assigned.iter() {
            assert!(o.get_mut().assigned.remove(&self));
        }
        for o in self.get_mut().located.iter() {
            assert!(o.get_mut().located.remove(&self));
        }
        // remove from tasks
        for t in self.get_mut().tasks.iter() {
            debug_assert!(t.get_mut().assigned == Some(self.clone()));
            t.get_mut().assigned = None;
        }
        // remove from graph
        graph.workers.remove(&self.get().id).unwrap();
        // assert that we hold the last reference, then drop it
        assert_eq!(self.get_num_refs(), 1);
    }
    /// Return the object ID in graph.
    pub fn get_id(&self) -> WorkerId { self.get().id }
}

impl fmt::Debug for WorkerRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "WorkerRef {}", self.get_id())
    }
}

impl fmt::Debug for Worker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Worker")
            .field("id", &self.id)
            .field("tasks", &self.tasks)
            .field("located", &self.located)
            .field("assigned", &self.assigned)
            .field("resources", &self.resources)
            .field("free_resources", &self.free_resources)
            .finish()
    }
}