    use std::net::SocketAddr;
use std::collections::HashMap;
use std::time::Duration;

use futures::{Future, Stream};
use tokio_core::reactor::Handle;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_timer;
use capnp_rpc::{twoparty, rpc_twoparty_capnp};

use errors::Result;
use common::RcSet;
use common::id::{SessionId, WorkerId, DataObjectId, TaskId, ClientId, SId};
use common::rpc::new_rpc_system;
use server::graph::{Graph, WorkerRef, DataObjectRef, TaskRef, SessionRef, ClientRef,
                    DataObjectState, DataObjectType, TaskState, TaskInput, SessionError};
use server::rpc::ServerBootstrapImpl;
use server::scheduler::{Scheduler, RandomScheduler, UpdatedIn, UpdatedOut};
use common::convert::ToCapnp;
use common::wrapped::WrappedRcRefCell;
use common::resources::Resources;
use common::{ConsistencyCheck, Attributes};

use hyper::server::Http;
use server::http::RequestHandler;

use common::logger::logger::Logger;
use common::logger::sqlite_logger::SQLiteLogger;

const LOGGING_INTERVAL: u64 = 1; // Logging interval in seconds

/// How long should be ID from worker ignored when it is task/object is unassigned
const IGNORE_ID_TIME_SECONDS: u64 = 30;

pub struct State {
    // Contained objects
    pub(super) graph: Graph,

    /// If true, next "turn" the scheduler is executed
    need_scheduling: bool,

    /// Listening port and address.
    listen_address: SocketAddr,

    /// Tokio core handle.
    handle: Handle,

    stop_server: bool,

    updates: UpdatedIn,

    /// Workers that will checked by reactor in the next turn()
    underload_workers: RcSet<WorkerRef>,

    scheduler: RandomScheduler,

    self_ref: Option<StateRef>,

    pub logger: Box<Logger>,

    timer: tokio_timer::Timer,
}

impl State {
    /// Add new worker, register it in the graph
    pub fn add_worker(
        &mut self,
        address: SocketAddr,
        control: Option<::worker_capnp::worker_control::Client>,
        resources: Resources,
    ) -> Result<WorkerRef> {
        debug!("New worker {}", address);
        if self.graph.workers.contains_key(&address) {
            bail!("State already contains worker {}", address);
        }
        let w = WorkerRef::new(address, control, resources);
        self.graph.workers.insert(w.get_id(), w.clone());
        self.underload_workers.insert(w.clone());
        self.logger.add_new_worker_event(w.get_id());
        Ok(w)
    }

    /// Remove the worker from the graph, forcefully unassigning all tasks and objects.
    /// TODO: better specs and context of worker removal
    pub fn remove_worker(&mut self, worker: &WorkerRef) -> Result<()> {
        unimplemented!() /*
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
        */

    }

    /// Put the worker into a failed state, unassigning all tasks and objects.
    /// Needs a lot of cleanup and recovery to avoid panic. Now just panics :-)
    pub fn fail_worker(&mut self, worker: &mut WorkerRef, cause: String) -> Result<()> {
        debug!("Failing worker {} with cause {:?}", worker.get_id(), cause);
        assert!(worker.get_mut().error.is_none());
        worker.get_mut().error = Some(cause.clone());
        // TODO: Cleanup and recovery if possible
        self.logger.add_worker_failed_event(
            worker.get_id(),
            cause.clone(),
        );
        panic!("Worker {} error: {:?}", worker.get_id(), cause);
    }

    /// Add new client, register it in the graph
    pub fn add_client(&mut self, address: SocketAddr) -> Result<ClientRef> {
        debug!("New client {}", address);
        if self.graph.clients.contains_key(&address) {
            bail!("State already contains client {}", address);
        }
        let c = ClientRef::new(address);
        self.graph.clients.insert(c.get().id, c.clone());
        self.logger.add_new_client_event(c.get().id);
        Ok(c)
    }

    /// Remove Client and its (owned) sessions. Called on client disconnect,
    /// so assume the client is inaccesible.
    pub fn remove_client(&mut self, client: &ClientRef) -> Result<()> {
        // remove owned sessions
        let sessions = client
            .get()
            .sessions
            .iter()
            .map(|x| x.clone())
            .collect::<Vec<_>>();
        for s in sessions {
            self.remove_session(&s)?;
        }
        // remove from graph
        self.graph.clients.remove(&client.get_id()).unwrap();
        self.logger.add_removed_client_event(
            client.get_id(),
            String::from("client disconnected"),
        );
        Ok(())
    }

    /// Create a new session fr a client, register it in the graph.
    pub fn add_session(&mut self, client: &ClientRef) -> Result<SessionRef> {
        let s = SessionRef::new(self.graph.new_session_id(), client);
        self.graph.sessions.insert(s.get_id(), s.clone());
        Ok(s)
    }

    /// Helper for .remove_session() and .fail_session(). Remove all session tasks,
    /// objects and cancel all finish hooks.
    fn clear_session(&mut self, s: &SessionRef) -> Result<()> {
        let tasks = s.get_mut().tasks.clone();
        for t in tasks {
            t.unschedule();
            self.remove_task(&t)?;
        }
        let objects = s.get_mut().objects.clone();
        for o in objects {
            o.get_mut().client_keep = false;
            o.unschedule();
            self.remove_object(&o)?;
        }
        // Remove all finish hooks
        s.get_mut().finish_hooks.clear();
        Ok(())
    }

    /// Remove a session and all the tasks and objects, both from the graph and from the workers,
    /// cancel all the finish hooks.
    pub fn remove_session(&mut self, session: &SessionRef) -> Result<()> {
        debug!(
            "Removing session {} of client {}",
            session.get_id(),
            session.get().client.get_id()
        );
        // remove children objects
        self.clear_session(session)?;
        // remove from graph
        self.graph.sessions.remove(&session.get_id()).unwrap();
        // unlink
        session.unlink();
        Ok(())
    }

    /// Put the session into a failed state, removing all tasks and objects,
    /// cancelling all finish_hooks.
    pub fn fail_session(&mut self, session: &SessionRef, cause: String) -> Result<()> {
        debug!(
            "Failing session {} of client {} with cause {:?}",
            session.get_id(),
            session.get().client.get_id(),
            cause
        );
        assert!(session.get_mut().error.is_none());
        session.get_mut().error = Some(SessionError::new(cause));
        // Remove all tasks + objects (with their finish hooks)
        self.clear_session(session)
    }

    /// Add a new object, register it in the graph and the session.
    pub fn add_object(
        &mut self,
        session: &SessionRef,
        id: DataObjectId,
        object_type: DataObjectType,
        client_keep: bool,
        label: String,
        data: Option<Vec<u8>>,
        attributes: Attributes,
    ) -> Result<DataObjectRef> {
        if self.graph.objects.contains_key(&id) {
            bail!("State already contains object with id {}", id);
        }
        let oref = DataObjectRef::new(
            session,
            id,
            object_type,
            client_keep,
            label,
            data,
            attributes,
        );
        // add to graph
        self.graph.objects.insert(oref.get_id(), oref.clone());
        // add to updated objects
        self.updates.new_objects.insert(oref.clone());
        oref.check_consistency_opt().unwrap(); // non-recoverable
        Ok(oref)
    }

    /// Remove the object from the graph and workers (with RPC calls).
    /// Fails with no change in the graph if there are any tasks linked to the object.
    pub fn remove_object(&mut self, oref: &DataObjectRef) -> Result<()> {
        oref.check_consistency_opt().unwrap(); // non-recoverable
        // unassign the object
        let ws = oref.get().assigned.clone();
        for w in ws {
            self.unassign_object(oref, &w);
        }
        // unlink from owner, consistency checks
        oref.unlink();
        // remove from graph
        self.graph.objects.remove(&oref.get_id()).unwrap();
        Ok(())
    }

    /// Add the task to the graph, checking consistency with adjacent objects.
    /// All the inputs+outputs must already be present.
    pub fn add_task(
        &mut self,
        session: &SessionRef,
        id: TaskId,
        inputs: Vec<TaskInput>,
        outputs: Vec<DataObjectRef>,
        task_type: String,
        task_config: Vec<u8>,
        attributes: Attributes,
        resources: Resources,
    ) -> Result<TaskRef> {
        if self.graph.tasks.contains_key(&id) {
            bail!("Task {} already in the graph", id);
        }
        let tref = TaskRef::new(
            session,
            id,
            inputs,
            outputs,
            task_type,
            task_config,
            attributes,
            resources,
        )?;
        // add to graph
        self.graph.tasks.insert(tref.get_id(), tref.clone());
        // add to scheduler updates
        self.updates.new_tasks.insert(tref.clone());
        tref.check_consistency_opt().unwrap(); // non-recoverable
        Ok(tref)
    }

    /// Remove task from the graph, from the workers and unlink from adjacent objects.
    /// WARNING: May leave objects without producers. You should check for them after removing all
    /// the tasks and objects in bulk.
    pub fn remove_task(&mut self, tref: &TaskRef) -> Result<()> {
        //tref.check_consistency_opt().unwrap(); // non-recoverable

        // unassign from worker
        if tref.get().assigned.is_some() {
            self.unassign_task(tref);
        }
        // Unlink from parent and objects.
        tref.unlink();
        // Remove from graph
        self.graph.tasks.remove(&tref.get_id()).unwrap();
        Ok(())
    }

    pub fn worker_by_id(&self, id: WorkerId) -> Result<WorkerRef> {
        match self.graph.workers.get(&id) {
            Some(w) => Ok(w.clone()),
            None => Err(format!("Worker {:?} not found", id))?,
        }
    }

    pub fn client_by_id(&self, id: ClientId) -> Result<ClientRef> {
        match self.graph.clients.get(&id) {
            Some(c) => Ok(c.clone()),
            None => Err(format!("Client {:?} not found", id))?,
        }
    }

    pub fn session_by_id(&self, id: SessionId) -> Result<SessionRef> {
        match self.graph.sessions.get(&id) {
            Some(s) => Ok(s.clone()),
            None => Err(format!("Session {:?} not found", id))?,
        }
    }

    pub fn object_by_id(&self, id: DataObjectId) -> Result<DataObjectRef> {
        match self.graph.objects.get(&id) {
            Some(o) => Ok(o.clone()),
            None => Err(format!("Object {:?} not found", id))?,
        }
    }

    // same as object_by_id but also check if session not failed
    // if object not found it tries to at least find session by session_id and
    // check if it does not failed
    pub fn object_by_id_check_session(&self, id: DataObjectId) -> Result<DataObjectRef> {
        match self.graph.objects.get(&id) {
            Some(o) => {
                let obj = o.get();
                if obj.session.get().is_failed() {
                    return Err(obj.session.get().get_error().clone().unwrap().into());
                }
                Ok(o.clone())
            }
            None => {
                let session = self.session_by_id(id.get_session_id())?;
                if session.get().is_failed() {
                    return Err(session.get().get_error().clone().unwrap().into());
                } else {
                    return Err(format!("Object {:?} not found", id).into());
                }
            }
        }
    }

    pub fn task_by_id(&self, id: TaskId) -> Result<TaskRef> {
        match self.graph.tasks.get(&id) {
            Some(t) => Ok(t.clone()),
            None => Err(format!("Task {:?} not found", id))?,
        }
    }

    // same as task_by_id but also check if session not failed
    // if task not found it tries to at least find session by session_id and
    // check if it does not failed
    pub fn task_by_id_check_session(&self, id: TaskId) -> Result<TaskRef> {
        match self.graph.tasks.get(&id) {
            Some(t) => {
                let task = t.get();
                if task.session.get().is_failed() {
                    return Err(task.session.get().get_error().clone().unwrap().into());
                }
                Ok(t.clone())
            }
            None => {
                let session = self.session_by_id(id.get_session_id())?;
                if session.get().is_failed() {
                    return Err(session.get().get_error().clone().unwrap().into());
                } else {
                    return Err(format!("Task {:?} not found", id).into());
                }
            }
        }
    }


    /// Verify submit integrity: all objects have either data or producers, acyclicity.
    pub fn verify_submit(&mut self, tasks: &[TaskRef], objects: &[DataObjectRef]) -> Result<()> {
        // TODO: Check acyclicity
        // Every object must have data or a single producer
        for oref in objects.iter() {
            let o = oref.get();
            if o.producer.is_some() && o.data.is_some() {
                bail!(
                    "Object {} submitted with both producer task {} and data of size {}",
                    o.id,
                    o.producer.as_ref().unwrap().get_id(),
                    o.data.as_ref().unwrap().len()
                );
            }
            if o.producer.is_none() && o.data.is_none() {
                bail!("Object {} submitted with neither producer nor data.", o.id);
            }
        }
        // Verify every submitted object
        for oref in objects.iter() {
            oref.check_consistency()?;
        }
        // Verify every submitted task
        for tref in tasks.iter() {
            tref.check_consistency()?;
        }

        self.check_consistency_opt().unwrap(); // non-recoverable
        Ok(())
    }

    /// Assign a `Finished` object to a worker and send the object metadata.
    /// Panics if the object is already assigned on the worker or not Finished.
    pub fn assign_object(&mut self, object: &DataObjectRef, wref: &WorkerRef) {
        assert_eq!(object.get().state, DataObjectState::Finished);
        assert!(!object.get().assigned.contains(wref));
        object.check_consistency_opt().unwrap(); // non-recoverable
        wref.check_consistency_opt().unwrap(); // non-recoverable
        let empty_worker_id = ::common::id::empty_worker_id();

        // Create request
        let mut req = wref.get().control.as_ref().unwrap().add_nodes_request();
        {
            let mut new_objects = req.get().init_new_objects(1);
            let mut co = &mut new_objects.borrow().get(0);
            let o = object.get();
            o.to_worker_capnp(&mut co);
            let placement = o.located
                .iter()
                .next()
                .map(|w| w.get().id().clone())
                .unwrap_or_else(|| {
                    // If there is no placement, then server is the source of datobject
                    assert!(o.data.is_some());
                    empty_worker_id.clone()
                });
            placement.to_capnp(&mut co.borrow().get_placement().unwrap());
            co.set_assigned(true);
        }

        self.handle.spawn(
            req.send().promise.map(|_| ()).map_err(|e| {
                panic!("[assign_object] Send failed {:?}", e)
            }),
        );

        object.get_mut().assigned.insert(wref.clone());
        wref.get_mut().assigned_objects.insert(object.clone());
        object.check_consistency_opt().unwrap(); // non-recoverable
        wref.check_consistency_opt().unwrap(); // non-recoverable
    }

    // Remove object from workers (not server)
    pub fn purge_object(&mut self, object: &DataObjectRef) {
        object.unschedule();
        let assigned = object.get().assigned.clone();
        for worker in assigned {
            self.unassign_object(object, &worker);
        }
    }

    /// Unassign an object from a worker and send the unassign call.
    /// Panics if the object is not assigned on the worker.
    pub fn unassign_object(&mut self, object: &DataObjectRef, wref: &WorkerRef) {
        debug!("unassign_object {:?} at {:?}", object, wref);
        assert!(object.get().assigned.contains(wref));
        object.check_consistency_opt().unwrap(); // non-recoverable
        wref.check_consistency_opt().unwrap(); // non-recoverable

        // Create request
        let mut req = wref.get()
            .control
            .as_ref()
            .unwrap()
            .unassign_objects_request();
        {
            let mut objects = req.get().init_objects(1);
            let co = &mut objects.borrow().get(0);
            object.get_id().to_capnp(co);
        }

        {
            let object_id = object.get().id;
            wref.get_mut().ignored_objects.insert(object_id);
            let wref2 = wref.clone();
            let duration = ::std::time::Duration::from_secs(IGNORE_ID_TIME_SECONDS);
            let clean_id_future = self.timer.sleep(duration).map(move |()| {
                wref2.get_mut().ignored_objects.remove(&object_id);
            }).map_err(|e| panic!("Cleaning ignored id failed {}", e));

            let o2 = object.clone();
            let w2 = wref.clone();
            self.handle.spawn(
                req.send()
                    .promise
                    .join(clean_id_future)
                    .map(|_| ())
                    .map_err(move |e| {
                        panic!(
                            "Sending unassign_object {:?} to {:?} failed {:?}",
                            o2,
                            w2,
                            e
                        )
                    }),
            );
        }

        object.get_mut().assigned.remove(wref);
        wref.get_mut().assigned_objects.remove(object);
        object.get_mut().located.remove(wref); // may not be present
        wref.get_mut().located_objects.remove(object); // may not be present
        if object.get().assigned.is_empty() {
            if object.get().state == DataObjectState::Finished {
                object.get_mut().state = DataObjectState::Removed;
                assert!(object.get().scheduled.is_empty());
                assert!(!object.get().client_keep);
            }
        }

        object.check_consistency_opt().unwrap(); // non-recoverable
        wref.check_consistency_opt().unwrap(); // non-recoverable
    }

    /// Assign and send the task to the worker it is scheduled for.
    /// Panics when the task is not scheduled or not ready.
    /// Assigns output objects to the worker, input objects are not assigned.
    pub fn assign_task(&mut self, task: &TaskRef) {
        task.check_consistency_opt().unwrap(); // non-recoverable

        {
            // lexical scoping for `t`
            let mut t = task.get_mut();
            assert!(t.scheduled.is_some());
            assert!(t.assigned.is_none());

            // Collect input objects: pairs (object, worker_id) where worker_id is placement of object
            let mut objects: Vec<(DataObjectRef, WorkerId)> = Vec::new();

            let wref = t.scheduled.as_ref().unwrap().clone();
            t.assigned = Some(wref.clone());
            let worker_id = wref.get_id();
            let empty_worker_id = ::common::id::empty_worker_id();
            debug!("Assiging task id={} to worker={}", t.id, worker_id);

            for input in t.inputs.iter() {
                let o = input.object.get_mut();
                if !o.assigned.contains(&wref) {
                    // Just take first placement
                    let placement = o.located
                        .iter()
                        .next()
                        .map(|w| w.get().id().clone())
                        .unwrap_or_else(|| {
                            // If there is no placement, then server is the source of datobject
                            assert!(o.data.is_some());
                            empty_worker_id.clone()
                        });
                    objects.push((input.object.clone(), placement));
                }
            }

            for output in t.outputs.iter() {
                objects.push((output.clone(), worker_id.clone()));
                output.get_mut().assigned.insert(wref.clone());
                wref.get_mut().assigned_objects.insert(output.clone());
            }

            // Create request
            let mut req = wref.get().control.as_ref().unwrap().add_nodes_request();

            // Serialize objects
            {
                let mut new_objects = req.get().init_new_objects(objects.len() as u32);
                for (i, &(ref object, placement)) in objects.iter().enumerate() {
                    let mut co = &mut new_objects.borrow().get(i as u32);
                    placement.to_capnp(&mut co.borrow().get_placement().unwrap());
                    let obj = object.get();
                    obj.to_worker_capnp(&mut co);
                    // only assign output tasks - they are all assigned
                    co.set_assigned(obj.assigned.contains(&wref));
                }
            }

            // Serialize the task
            {
                let new_tasks = req.get().init_new_tasks(1);
                t.to_worker_capnp(&mut new_tasks.get(0));
            }

            self.handle.spawn(
                req.send().promise.map(|_| ()).map_err(|e| {
                    panic!("[assign_task] Send failed {:?}", e)
                }),
            );

            wref.get_mut().assigned_tasks.insert(task.clone());
            wref.get_mut().scheduled_ready_tasks.remove(task);
            t.assigned = Some(wref.clone());
            t.state = TaskState::Assigned;

            /*
            for oref in t.outputs.iter() {
                oref.get_mut().assigned.insert(wref.clone());
                wref.get_mut().assigned_objects.insert(oref.clone());
            }*/
        }
        task.check_consistency_opt().unwrap(); // non-recoverable
    }

    /// Unassign task from the worker it is assigned to and send the unassign call.
    /// Panics when the task is not assigned to the given worker or scheduled there.
    pub fn unassign_task(&mut self, task: &TaskRef) {
        let wref = task.get().assigned.as_ref().unwrap().clone(); // non-recoverable

        assert!(task.get().scheduled != Some(wref.clone()));

        //task.check_consistency_opt().unwrap(); // non-recoverable
        //wref.check_consistency_opt().unwrap(); // non-recoverable

        // Create request
        let mut req = wref.get().control.as_ref().unwrap().stop_tasks_request();
        {
            let mut tasks = req.get().init_tasks(1);
            let ct = &mut tasks.borrow().get(0);
            task.get_id().to_capnp(ct);
        }

        let task_id = task.get().id;
        wref.get_mut().ignored_tasks.insert(task_id);
        let wref2 = wref.clone();
        let duration = ::std::time::Duration::from_secs(IGNORE_ID_TIME_SECONDS);
        let clean_id_future = self.timer.sleep(duration).map(move |()| {
            wref2.get_mut().ignored_tasks.remove(&task_id);
        }).map_err(|e| panic!("Cleaning ignored id failed {:?}", e));

        self.handle.spawn(
            req.send()
                .promise
                .join(clean_id_future)
                .map(|_| ())
                .map_err(|e| panic!("[unassign_task] Send failed {:?}", e)),
        );

        task.get_mut().assigned = None;
        task.get_mut().state = TaskState::Ready;
        wref.get_mut().assigned_tasks.remove(task);
        self.update_task_assignment(task);

        for oref in task.get()
            .outputs
            .iter()
            .map(|x| x.clone())
            .collect::<Vec<_>>()
        {
            self.unassign_object(&oref, &wref);
        }

        task.check_consistency_opt().unwrap(); // non-recoverable
        wref.check_consistency_opt().unwrap(); // non-recoverable
    }

    /// Removes a keep flag from an object.
    pub fn unkeep_object(&mut self, object: &DataObjectRef) {
        object.check_consistency_opt().unwrap(); // non-recoverable
        object.get_mut().client_keep = false;
        let needed = object.get().is_needed();
        if !needed {
            object.unschedule();
        }
        self.update_object_assignments(object, None);
        object.check_consistency_opt().unwrap(); // non-recoverable
    }

    /// Update any assignments depending on the task state, and set to Ready on all inputs ready.
    ///
    /// * Check if all task inputs are ready, and switch state.
    /// * Check if a ready task is scheduled and queue it on the worker (`scheduled_ready`).
    /// * Check if a task is assigned and not scheduled or scheduled elsewhere,
    ///   then unassign and possibly enqueue as a ready task on scheduled worker.
    /// * Check if a task is finished, then unschedule and cleanup.
    /// * Failed task is an error here.
    pub fn update_task_assignment(&mut self, tref: &TaskRef) {
        assert!(tref.get().state != TaskState::Failed);

        if tref.get().state == TaskState::NotAssigned && tref.get().waiting_for.is_empty() {
            tref.get_mut().state = TaskState::Ready;
            self.updates.tasks.insert(tref.clone());
        }

        if tref.get().state == TaskState::Ready {
            if let Some(ref wref) = tref.get().scheduled {
                wref.get_mut().scheduled_ready_tasks.insert(tref.clone());
            }
        }

        if tref.get().state == TaskState::Assigned || tref.get().state == TaskState::Running {
            if tref.get().assigned != tref.get().scheduled {
                if let Some(ref wref) = tref.get().assigned {
                    // Unassign the task if assigned
                    self.unassign_task(tref);
                    // The state was assigned or running, now is ready
                    assert_eq!(tref.get().state, TaskState::Ready);
                }
                if let Some(ref wref) = tref.get().scheduled {
                    if tref.get().state == TaskState::Ready {
                        // If reported as updated by mistake, the task may be already in the set
                        wref.get_mut().scheduled_ready_tasks.insert(tref.clone());
                    }
                }
            }
        }

        if tref.get().state == TaskState::Finished {
            assert!(tref.get().assigned.is_none());
            tref.get_mut().scheduled = None;
        }

        tref.check_consistency_opt().unwrap(); // unrecoverable
    }

    /// Update finished object assignment to match the schedule on the given worker (optional) and
    /// needed-ness. NOP for Unfinished and Removed objects.
    ///
    /// If worker is given, updates the assignment on the worker to match the
    /// scheduling there. Object is unassigned only if located elsewhere or not needed.
    ///
    /// Then, if the object is not scheduled and not needed, it is unassigned and set to Removed.
    /// If the object is scheduled but located on more workers than scheduled on (this can happen
    /// e.g. when scheduled after the needed object was located but not scheduled), the located
    /// list is pruned to only match the scheduled list (possibly plus one remaining worker if no
    /// scheduled workers have it located).
    pub fn update_object_assignments(&mut self, oref: &DataObjectRef, worker: Option<&WorkerRef>) {
        let ostate = oref.get().state;
        match ostate {
            DataObjectState::Unfinished => (),
            DataObjectState::Removed => (),
            DataObjectState::Finished => {
                if let Some(ref wref) = worker {
                    if wref.get().scheduled_objects.contains(oref) {
                        if !wref.get().assigned_objects.contains(oref) &&
                            oref.get().state == DataObjectState::Finished
                        {
                            self.assign_object(oref, wref);
                        }
                    } else {
                        if wref.get().assigned_objects.contains(oref) &&
                            (oref.get().located.len() > 2 || !oref.get().located.contains(wref))
                        {
                            self.unassign_object(oref, wref);
                        }
                    }
                }

                // Note that the object may be already Removed here
                if oref.get().scheduled.is_empty() &&
                    oref.get().state == DataObjectState::Finished
                {
                    if !oref.get().is_needed() {
                        let assigned = oref.get().assigned.clone();
                        for wa in assigned {
                            self.unassign_object(oref, &wa);
                        }
                        oref.get_mut().state = DataObjectState::Removed;
                    }
                } else {
                    if oref.get().located.len() > oref.get().scheduled.len() {
                        for wa in oref.get().located.clone() {
                            if !oref.get().scheduled.contains(&wa) &&
                                oref.get().located.len() >= 2
                            {
                                self.unassign_object(oref, &wa);
                            }
                        }
                    }
                }
            }
        }
        oref.check_consistency_opt().unwrap(); // unrecoverable
    }

    /// Process state updates from one Worker.
    pub fn updates_from_worker(
        &mut self,
        worker: &WorkerRef,
        obj_updates: Vec<(DataObjectRef, DataObjectState, usize, Attributes)>,
        task_updates: Vec<(TaskRef, TaskState, Attributes)>,
    ) {
        debug!(
            "Update states for {:?}, objs: {}, tasks: {}",
            worker,
            obj_updates.len(),
            task_updates.len()
        );
        worker.check_consistency_opt().unwrap(); // non-recoverable

        for (tref, state, attributes) in task_updates {
            // inform the scheduler
            self.updates.tasks.insert(tref.clone());
            // set the state and possibly propagate
            match state {
                TaskState::Finished => {
                    {
                        let mut t = tref.get_mut();
                        t.session.get_mut().task_finished();
                        t.state = state;
                        t.attributes.update(attributes);
                        t.scheduled = None;
                        worker.get_mut().scheduled_tasks.remove(&tref);
                        t.assigned = None;
                        worker.get_mut().assigned_tasks.remove(&tref);
                        self.logger.add_task_finished_event(t.id);
                    }
                    tref.get_mut().trigger_finish_hooks();
                    self.update_task_assignment(&tref);

                    for input in &tref.get().inputs {
                        // We check that need_by was really decreased to protect against
                        // task that uses objects as more inputs
                        let not_needed = {
                            let mut o = input.object.get_mut();
                            o.need_by.remove(&tref) && !o.is_needed()
                        };
                        if not_needed {
                            self.purge_object(&input.object);
                        }
                    }

                    self.underload_workers.insert(worker.clone());
                }
                TaskState::Running => {
                    let mut t = tref.get_mut();
                    assert_eq!(t.state, TaskState::Assigned);
                    t.state = state;
                    t.attributes = attributes;
                    self.logger.add_task_started_event(t.id, worker.get_id());

                }
                TaskState::Failed => {
                    debug!(
                        "Task {:?} failed on {:?} with attributes {:?}",
                        *tref.get(),
                        worker,
                        attributes
                    );
                    let error_message : String = attributes.get("error").unwrap_or_else(|e| {
                        warn!("Cannot decode error message");
                        "Cannot decode error message".to_string()
                    });
                    self.underload_workers.insert(worker.clone());

                    tref.get_mut().state = state;
                    tref.get_mut().attributes = attributes;
                    let session = tref.get().session.clone();
                    let error_message = format!("Task {} failed: {}", tref.get().id, error_message);
                    self.fail_session(&session, error_message.clone()).unwrap();
                    self.logger.add_task_failed_event(
                        tref.get().id,
                        worker.get_id(),
                        error_message,
                    );
                }
                _ => {
                    panic!(
                        "Invalid worker {:?} task {:?} state update to {:?}",
                        worker,
                        *tref.get(),
                        state
                    )
                }
            }
        }

        for (oref, state, size, attributes) in obj_updates {
            // Inform the scheduler
            self.updates
                .objects
                .entry(oref.clone())
                .or_insert(Default::default())
                .insert(worker.clone());
            match state {
                DataObjectState::Finished => {
                    if !oref.get().assigned.contains(&worker) {
                        // We did not assign the object to the worker
                        // It means that it was an input of scheduled tasks, but object
                        // was not directly scheduled
                        continue;
                    }
                    oref.get_mut().located.insert(worker.clone());
                    worker.get_mut().located_objects.insert(oref.clone());
                    let cur_state = oref.get().state; // To satisfy the borrow checker
                    match cur_state {
                        DataObjectState::Unfinished => {
                            {
                                // capture `o`
                                let mut o = oref.get_mut();
                                // first completion
                                o.state = state;
                                o.size = Some(size);
                                o.attributes = attributes;
                                o.trigger_finish_hooks();
                            }
                            for cref in oref.get().consumers.clone() {
                                assert_eq!(cref.get().state, TaskState::NotAssigned);
                                cref.get_mut().waiting_for.remove(&oref);
                                self.update_task_assignment(&cref);
                            }
                            if oref.get().is_needed() {
                                self.update_object_assignments(&oref, Some(worker));
                            } else {
                                self.purge_object(&oref);
                            }
                        }
                        DataObjectState::Finished => {
                            // cloning to some other worker done
                            self.update_object_assignments(&oref, Some(worker));
                        }
                        _ => {
                            panic!(
                                "worker {:?} set object {:?} state to {:?}",
                                worker,
                                *oref.get(),
                                state
                            );
                        }
                    }
                }
                _ => {
                    panic!(
                        "worker {:?} set object {:?} state to {:?}",
                        worker,
                        *oref.get(),
                        state
                    );
                }
            }
        }
        worker.check_consistency_opt().unwrap(); // non-recoverable
    }

    /// For all workers, if the worker is not overbooked and has ready messages, distribute
    /// more scheduled ready tasks to workers.
    pub fn distribute_tasks(&mut self) {
        if self.underload_workers.is_empty() {
            return;
        }
        debug!("Distributing tasks");
        for wref in &::std::mem::replace(&mut self.underload_workers, Default::default()) {
            //let mut w = wref.get_mut();
            // TODO: Customize the overbook limit
            while wref.get().assigned_tasks.len() < 128 &&
                !wref.get().scheduled_ready_tasks.is_empty()
            {
                // TODO: Prioritize older members of w.scheduled_ready_tasks (order-preserving set)
                let tref = wref.get()
                    .scheduled_ready_tasks
                    .iter()
                    .next()
                    .unwrap()
                    .clone();
                assert!(tref.get().scheduled == Some(wref.clone()));
                self.assign_task(&tref);
            }
        }
    }

    /// Run the scheduler and do any immediate updates the assignments.
    pub fn run_scheduler(&mut self) {
        debug!("Running scheduler");

        // Run scheduler and reset updated objects.
        let changed = self.scheduler.schedule(&mut self.graph, &self.updates);
        self.updates = Default::default();

        // Update assignments of (possibly) changed objects.
        for (wref, os) in changed.objects.iter() {
            for oref in os.iter() {
                self.update_object_assignments(oref, Some(wref));
            }
        }

        for tref in changed.tasks.iter() {
            self.update_task_assignment(tref);
        }
        self.underload_workers = self.graph.workers.values().map(|w| w.clone()).collect();
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

impl ConsistencyCheck for State {
    /// Check consistency of all tasks, objects, workers, clients and sessions. Quite slow.
    fn check_consistency(&self) -> Result<()> {
        debug!("Checking State consistency");
        for tr in self.graph.tasks.values() {
            tr.check_consistency()?;
        }
        for or in self.graph.objects.values() {
            or.check_consistency()?;
        }
        for wr in self.graph.workers.values() {
            wr.check_consistency()?;
        }
        for sr in self.graph.sessions.values() {
            sr.check_consistency()?;
        }
        for cr in self.graph.clients.values() {
            cr.check_consistency()?;
        }
        Ok(())
    }
}

/// Note: No `Drop` impl as a `State` is assumed to live forever.
pub type StateRef = WrappedRcRefCell<State>;

impl StateRef {
    pub fn new(handle: Handle, listen_address: SocketAddr) -> Self {
        let s = Self::wrap(State {
            graph: Default::default(),
            need_scheduling: false,
            listen_address: listen_address,
            handle: handle,
            scheduler: Default::default(),
            underload_workers: Default::default(),
            updates: Default::default(),
            stop_server: false,
            self_ref: None,
            logger: Box::new(SQLiteLogger::new()),
            timer: tokio_timer::wheel()
                .tick_duration(Duration::from_millis(100))
                .num_slots(512)
                .build(),
        });
        s.get_mut().self_ref = Some(s.clone());
        s
    }

    pub fn start(&self) {
        let listen_address = self.get().listen_address;
        let handle = self.get().handle.clone();
        let listener = TcpListener::bind(&listen_address, &handle).unwrap();

        let state = self.clone();
        let future = listener
            .incoming()
            .for_each(move |(stream, addr)| {
                state.on_connection(stream, addr);
                Ok(())
            })
            .map_err(|e| {
                panic!("Listening failed {:?}", e);
            });
        handle.spawn(future);
        info!("Start listening on address={}", listen_address);

        // ---- Start HTTP server ----
        let http_address = "0.0.0.0:8080".parse().unwrap();
        let listener = TcpListener::bind(&http_address, &handle).unwrap();
        let http = Http::new();
        let handle1 = self.get().handle.clone();
        let http_server = listener
            .incoming()
            .for_each(move |(sock, http_address)| {
                http.bind_connection(&handle1, sock, http_address, RequestHandler);
                Ok(())
            })
            .map_err(|e| {
                panic!("HTTP server failed {:?}", e);
            });
        handle.spawn(http_server);
        info!("HTTP server running on address={}", http_address);

        // ---- Start logging ----
        let state = self.clone();
        let timer = state.get().timer.clone();
        let interval = timer.interval(Duration::from_secs(LOGGING_INTERVAL));

        let logging = interval
            .for_each(move |()| {
                state.get_mut().logger.flush_events();
                Ok(())
            })
            .map_err(|e| {
                error!("Logging error {}", e)
            });
        handle.spawn(logging);
    }

    /// Main loop State entry. Returns `false` when the server should stop.
    pub fn turn(&self) -> bool {
        // TODO: better conditional scheduling
        if !self.get().updates.is_empty() {
            self.get_mut().run_scheduler();
            self.get().check_consistency_opt().unwrap(); // unrecoverable
        }

        // Assign ready tasks to workers (up to overbook limit)
        self.get_mut().distribute_tasks();
        !self.get().stop_server
    }

    fn on_connection(&self, stream: TcpStream, address: SocketAddr) {
        // Handle an incoming connection; spawn gate object for it

        info!("New connection from {}", address);
        stream.set_nodelay(true).unwrap();
        let bootstrap = ::server_capnp::server_bootstrap::ToClient::new(
            ServerBootstrapImpl::new(self, address),
        ).from_server::<::capnp_rpc::Server>();

        let rpc_system = new_rpc_system(stream, Some(bootstrap.client));
        self.get().handle.spawn(rpc_system.map_err(
            |e| panic!("RPC error: {:?}", e),
        ));
    }

    #[inline]
    pub fn handle(&self) -> Handle {
        self.get().handle.clone()
    }
}
