use std::collections::hash_map::HashMap;
use super::graph::{DataObjectRef, TaskRef, WorkerRef, Graph};
use common::id::SId;
use common::RcSet;

#[derive(Default, Clone, Debug)]
pub struct UpdatedOut {
    /// Tasks with updatet state
    tasks: RcSet<TaskRef>,
    /// Worker-DataObject updated pairs, grouped by worker
    objects: HashMap<WorkerRef, RcSet<DataObjectRef>>,
}

#[derive(Default, Clone, Debug)]
pub struct UpdatedIn {
    /// Newly submitted Tasks.
    new_tasks: RcSet<TaskRef>,
    /// Newly submitted DataObjects.
    new_objects: RcSet<DataObjectRef>,
    /// Old Tasks with changed state. Includes changes originating from workers, clients
    /// and the server assigning Task to Worker. Scheduler-requested operations
    /// (unscheduled already Assigned or Running tasks) are not included.
    tasks: RcSet<TaskRef>,
    /// Old DataObjects with changed state. Includes changes originating from workers, clients
    /// and the server assigning Object to Worker. Scheduler-requested operations
    /// (unscheduled already Assigned or Finished object) are not included.
    objects: HashMap<DataObjectRef, RcSet<WorkerRef>>,
}

/// Scheduler interface. The Extra types are the types of a scheduler-specific attribute
/// `s` in each node for any use by the scheduler.
// TODO: Add the extras to the graph objects. Add Scheduler as Graph type parameter.
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

        for tr in updated.new_tasks.iter() {
            let mut t = tr.get_mut();
            if t.scheduled.is_none() {
                let w = random_worker(graph, t.id.get_id() as usize);
                t.scheduled = Some(w);
                up_out.tasks.insert(tr.clone());
            }
        }

        for or in updated.new_objects.iter() {
            let mut o = or.get_mut();
            let needed = (!o.consumers.is_empty()) || o.client_keep;
            if o.scheduled.is_empty() && (needed || o.id.get_id() % 3 == 1) {
                let w = if let Some(ref prod) = o.producer {
                    prod.get().scheduled.clone().unwrap()
                } else {
                    random_worker(graph, o.id.get_id() as usize)
                };
                o.scheduled.insert(w.clone());
                up_out.objects.entry(w).or_insert(Default::default()).insert(or.clone());
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