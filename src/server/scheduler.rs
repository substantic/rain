use std::collections::hash_map::HashMap;
use std::clone::Clone;
use super::graph::{DataObjectRef, TaskRef, WorkerRef, Graph, TaskState};
use common::id::SId;
use common::RcSet;
use server::graph::SessionRef;

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

    pub fn clear(&mut self) {
        self.new_tasks = Default::default();
        self.new_objects = Default::default();
        self.tasks.clear();
        self.objects.clear();
    }

    pub fn remove_task(&mut self, task_ref: &TaskRef) {
        self.new_tasks.remove(task_ref);
        self.tasks.remove(task_ref);
    }
}

/// Scheduler interface. The Extra types are the types of a scheduler-specific attribute
/// `s` in each node for any use by the scheduler.
// TODO: Possibly add as template parameter and add the extras to the graph objects.
/*pub trait Scheduler {
    type TaskExtra;
    type DataObjectExtra;
    type WorkerExtra;
    type SessionExtra;
    type ClientExtra;

    fn schedule(&mut self, graph: &mut Graph, updated: &UpdatedIn) -> UpdatedOut;
}*/

#[derive(Default, Clone, Debug)]
pub struct ReactiveScheduler {
    ready_tasks: RcSet<TaskRef>,
}


impl ReactiveScheduler {
    /*type TaskExtra = ();
    type DataObjectExtra = ();
    type WorkerExtra = ();
    type SessionExtra = ();
    type ClientExtra = ();*/

    fn pick_best(&self, graph: &mut Graph) -> Option<(TaskRef, WorkerRef)> {
        let mut best_worker = None;
        let mut best_score = 0;
        let mut best_task = None;

        let n_workers = graph.workers.len() as i64;

        for tref in &self.ready_tasks {
            let t = tref.get();
            let mut total_size = 0;
            for input in &t.inputs {
                let o = input.object.get();
                total_size += o.size.unwrap() * o.scheduled.len();
            }
            let neg_avg_size = -(total_size as i64) / n_workers;
            //debug!("!!! {} AVG SIZE {}", t.id, -neg_avg_size);

            for (_, wref) in &graph.workers {
                let w = wref.get();
                let cpus = t.resources.cpus();
                if cpus + w.active_resources <= w.resources.cpus() &&
                   t.resources.is_subset_of(&w.resources) {
                    let mut score = neg_avg_size + cpus as i64 * 5000i64;
                    for input in &t.inputs {
                        let o = input.object.get();
                        if o.scheduled.contains(wref) {
                            score += o.size.unwrap() as i64;
                        }
                    }
                    if best_score < score || best_worker.is_none() {
                        best_score = score;
                        best_worker = Some(wref.clone());
                        best_task = Some(tref.clone());
                    }
                }
            }
        }
        if let Some(wref) = best_worker {
            Some((best_task.unwrap(), wref))
        } else {
            None
        }
    }

    pub fn clear_session(&mut self, session: &SessionRef)
    {
        let s = session.get();
        for tref in &s.tasks {
            self.ready_tasks.remove(&tref);
        }
    }

    pub fn schedule(&mut self, graph: &mut Graph, updated: &UpdatedIn) -> UpdatedOut {

        let mut up_out: UpdatedOut = Default::default();

        if graph.workers.is_empty() {
            return up_out
        }

        for tref in &updated.new_tasks {
            let mut t = tref.get_mut();
            if t.state == TaskState::Ready {
                debug!("Scheduler: New ready task {}", t.id);
                let r = self.ready_tasks.insert(tref.clone());
                assert!(r);
            }
        }

        for tref in &updated.tasks {
            let mut t = tref.get_mut();
            if t.state == TaskState::Ready {
                debug!("Scheduler: New ready task {}", t.id);
                let r = self.ready_tasks.insert(tref.clone());
                assert!(r);
            }
        }

        debug!("Scheduler started");

        while let Some((tref, wref)) = self.pick_best(graph) {
            {
                let mut w = wref.get_mut();
                let mut t = tref.get_mut();

                assert!(t.state == TaskState::Ready);
                w.active_resources += t.resources.cpus();
                w.scheduled_tasks.insert(tref.clone());

                // Scheduler "picks" only ready tasks, so we do need to test readiness of task
                w.scheduled_ready_tasks.insert(tref.clone());

                t.scheduled = Some(wref.clone());

                debug!("Scheduler: {} -> {}", t.id, w.id());
                for oref in &t.outputs {

                    w.scheduled_objects.insert(oref.clone());
                    oref.get_mut().scheduled.insert(wref.clone());

                    up_out
                        .objects
                        .entry(wref.clone())
                        .or_insert(Default::default())
                        .insert(oref.clone());
                }
            }
            self.ready_tasks.remove(&tref);
            up_out.tasks.insert(tref);
        }
        up_out

        /*if graph.workers.is_empty() {
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
        }*/


    }
}

/*
fn random_worker(g: &mut Graph, seed: usize) -> WorkerRef {
    let ws: Vec<_> = g.workers.values().collect();
    assert!(ws.len() > 0);
    ws[seed % ws.len()].clone()
}
*/