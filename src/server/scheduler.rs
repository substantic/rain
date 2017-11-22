use std::collections::hash_map::HashMap;
use std::clone::Clone;
use super::graph::{DataObjectRef, TaskRef, WorkerRef, Graph, TaskState};
use common::id::SId;
use common::RcSet;

#[derive(Default, Clone, Debug)]
pub struct UpdatedOut {
    /// Tasks with updatet state
    pub(in super::super) tasks: RcSet<TaskRef>,
    /// Worker-DataObject updated pairs, grouped by worker
    pub(in super::super) objects: HashMap<WorkerRef, RcSet<DataObjectRef>>,
}

#[derive(Default, Clone, Debug)]
pub struct UpdatedIn {
    /// Newly submitted Tasks.
    pub(in super::super) new_tasks: RcSet<TaskRef>,
    /// Newly submitted DataObjects.
    pub(in super::super) new_objects: RcSet<DataObjectRef>,
    /// Old Tasks with changed state. Includes changes originating from workers, clients
    /// and the server assigning Task to Worker. Scheduler-requested operations
    /// (unscheduled already Assigned or Running tasks) are not included.
    pub(in super::super) tasks: RcSet<TaskRef>,
    /// Old DataObjects with changed state. Includes changes originating from workers, clients
    /// and the server assigning Object to Worker. Scheduler-requested operations
    /// (unscheduled already Assigned or Finished object) are not included.
    pub(in super::super) objects: HashMap<DataObjectRef, RcSet<WorkerRef>>,
}

impl UpdatedIn {
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty() && self.objects.is_empty() && self.new_tasks.is_empty() &&
            self.new_objects.is_empty()
    }
}

/// Scheduler interface. The Extra types are the types of a scheduler-specific attribute
/// `s` in each node for any use by the scheduler.
// TODO: Possibly add as template parameter and add the extras to the graph objects.
pub trait Scheduler {
    type TaskExtra;
    type DataObjectExtra;
    type WorkerExtra;
    type SessionExtra;
    type ClientExtra;

    fn schedule(&mut self, graph: &mut Graph, updated: &UpdatedIn) -> UpdatedOut;
}

#[derive(Default, Clone, Debug)]
pub struct RandomScheduler {}

impl Scheduler for RandomScheduler {
    type TaskExtra = ();
    type DataObjectExtra = ();
    type WorkerExtra = ();
    type SessionExtra = ();
    type ClientExtra = ();

    fn schedule(&mut self, graph: &mut Graph, updated: &UpdatedIn) -> UpdatedOut {
        let mut up_out: UpdatedOut = Default::default();
        if graph.workers.is_empty() {
            warn!("Scheduler is running with empty workers -- not doing anything.");
            return up_out;
        }

        for tref in updated.new_tasks.iter() {
            let mut t = tref.get_mut();
            if t.scheduled.is_none() {
                let w = random_worker(graph, t.id.get_id() as usize);
                w.get_mut().scheduled_tasks.insert(tref.clone());
                if t.state == TaskState::Ready {
                    w.get_mut().scheduled_ready_tasks.insert(tref.clone());
                }
                t.scheduled = Some(w);
                up_out.tasks.insert(tref.clone());
            }
        }

        for oref in updated.new_objects.iter() {
            let mut o = oref.get_mut();
            let needed = (!o.consumers.is_empty()) || o.client_keep;
            if o.scheduled.is_empty() && (needed || o.id.get_id() % 3 == 1) {
                let w = if let Some(ref prod) = o.producer {
                    prod.get().scheduled.clone().unwrap()
                } else {
                    random_worker(graph, o.id.get_id() as usize)
                };
                w.get_mut().scheduled_objects.insert(oref.clone());
                o.scheduled.insert(w.clone());
                up_out
                    .objects
                    .entry(w)
                    .or_insert(Default::default())
                    .insert(oref.clone());
            }
        }

        up_out
    }
}

fn random_worker(g: &mut Graph, seed: usize) -> WorkerRef {
    let ws: Vec<_> = g.workers.values().collect();
    assert!(ws.len() > 0);
    ws[seed % ws.len()].clone()
}
