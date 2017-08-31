use futures::unsync::oneshot::Sender;
use std::net::SocketAddr;

use common::wrapped::WrappedRcRefCell;
use common::RcSet;
use common::id::WorkerId;
use common::resources::Resources;
use super::{TaskRef, DataObjectRef, Graph};

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
               resources: Resources) -> Self {
        let s = WorkerRef::wrap(Worker {
            id: address,
            tasks: Default::default(),
            located: Default::default(),
            assigned: Default::default(),
            control: control,
            resources: resources.clone(),
            free_resources: resources,
        });
        debug!("Creating worker {}", s.get_id());
        // add to graph
        graph.workers.insert(s.get().id, s.clone());
        s
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
