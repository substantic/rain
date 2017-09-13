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
    pub(super) assigned_tasks: RcSet<TaskRef>,

    /// Scheduled tasks. Superset of `assigned_tasks`.
    pub(super) scheduled_tasks: RcSet<TaskRef>,

    /// Scheduled tasks that are also ready but not yet assigned. Disjoint from
    /// `assigned_tasks`, subset of `scheduled_tasks`.
    pub(super) scheduled_ready_tasks: RcSet<TaskRef>,

    /// Obects fully located on the worker.
    pub(super) located_objects: RcSet<DataObjectRef>,

    /// Objects located or assigned to appear on the worker. Superset of `located`.
    pub(super) assigned_objects: RcSet<DataObjectRef>,

    /// Objects scheduled to appear here. Any objects in `located_objects` but not here
    /// are to be removed from the worker.
    pub(super) scheduled_objects: RcSet<DataObjectRef>,

    /// Control interface. Optional for testing and modelling.
    pub (in super::super) control: Option<::worker_capnp::worker_control::Client>,

    // Resources. TODO: Extract resources into separate struct
    resources: Resources,
    free_resources: Resources,
}

pub type WorkerRef = WrappedRcRefCell<Worker>;

impl Worker {

    #[inline]
    pub fn id(&self) -> &WorkerId {
        &self.id
    }
}


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
            assigned_tasks: Default::default(),
            scheduled_tasks: Default::default(),
            scheduled_ready_tasks: Default::default(),
            located_objects: Default::default(),
            assigned_objects: Default::default(),
            scheduled_objects: Default::default(),
            control: control,
            resources: resources.clone(),
            free_resources: resources,
        });
        // add to graph
        graph.workers.insert(s.get().id, s.clone());
        Ok(s)
    }

    /// Check for state and relationships consistency. Only explores adjacent objects but still
    /// may be slow.
    pub fn check_consistency(&self) -> Result<()> {
        let s = self.get();
        // refs
        for oref in s.located_objects.iter() {
            if !oref.get().located.contains(self) {
                bail!("located_object ref {:?} inconsistency in {:?}", oref, s)
            }
        }
        for oref in s.scheduled_objects.iter() {
            if !oref.get().scheduled.contains(self) {
                bail!("scheduled_object ref {:?} inconsistency in {:?}", oref, s)
            }
        }
        for oref in s.assigned_objects.iter() {
            if !oref.get().assigned.contains(self) {
                bail!("assigned_object ref {:?} inconsistency in {:?}", oref, s)
            }
        }
        for tref in s.assigned_tasks.iter() {
            if tref.get().assigned != Some(self.clone()) {
                bail!("assigned task ref {:?} inconsistency in {:?}", tref, s)
            }
        }
        for tref in s.scheduled_tasks.iter() {
            if tref.get().scheduled != Some(self.clone()) {
                bail!("scheduled task ref {:?} inconsistency in {:?}", tref, s)
            }
        }
        for tref in s.scheduled_ready_tasks.iter() {
            if tref.get().scheduled != Some(self.clone()) {
                bail!("scheduled_ready task ref {:?} inconsistency in {:?}", tref, s)
            }
        }
        Ok(())
    }

    pub fn delete(self, graph: &mut Graph) {
        debug!("Deleting worker {}", self.get_id());
        // remove from objects
        for o in self.get_mut().assigned_objects.iter() {
            assert!(o.get_mut().assigned.remove(&self));
        }
        for o in self.get_mut().located_objects.iter() {
            assert!(o.get_mut().located.remove(&self));
        }
        // remove from tasks
        for t in self.get_mut().assigned_tasks.iter() {
            t.get_mut().assigned = None;
        }
        for t in self.get_mut().scheduled_tasks.iter() {
            t.get_mut().scheduled = None;
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
            .field("tasks", &self.assigned_tasks)
            .field("located", &self.located_objects)
            .field("assigned", &self.assigned_objects)
            .field("resources", &self.resources)
            .field("free_resources", &self.free_resources)
            .finish()
    }
}
