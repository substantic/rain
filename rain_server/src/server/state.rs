use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use rain_core::logging::events;
use futures::{Future, Stream};
use hyper::server::Http;
use rain_core::{errors::*, sys::*, types::*, utils::*};
use tokio_core::net::{TcpListener, TcpStream};
use tokio_core::reactor::Handle;

use common::new_rpc_system;
use server::graph::{ClientRef, DataObjectRef, DataObjectState, GovernorRef, Graph, SessionRef,
                    TaskRef, TaskState};
use server::http::RequestHandler;
use server::logging::logger::Logger;
use server::logging::sqlite_logger::SQLiteLogger;
use server::rpc::ServerBootstrapImpl;
use server::scheduler::{ReactiveScheduler, UpdatedIn};
use server::testmode;
use wrapped::WrappedRcRefCell;

const LOGGING_INTERVAL: u64 = 1; // Logging interval in seconds

/// How long should be ID from governor ignored when it is task/object is unassigned
const IGNORE_ID_TIME_SECONDS: u64 = 30;

pub struct State {
    // Contained objects
    pub(super) graph: Graph,

    /// Id of recently closed sessions, that should be ignored for incoming messages
    /// from governor
    pub(in super::super) ignored_sessions: HashSet<SessionId>,

    /// Tokio core handle.
    handle: Handle,

    stop_server: bool,

    pub(super) updates: UpdatedIn,

    /// Governors that will checked by reactor in the next turn()
    underload_governors: RcSet<GovernorRef>,

    scheduler: ReactiveScheduler,

    // If testing_mode is true, then __test attributes are interpreted
    test_mode: bool,

    self_ref: Option<StateRef>,

    pub logger: Box<Logger>,

    /// Listening port and address.
    listen_address: SocketAddr,

    /// Listening port for HTTP interface
    http_listen_address: SocketAddr,
}

impl State {
    /// Add new governor, register it in the graph
    pub fn add_governor(
        &mut self,
        address: SocketAddr,
        control: Option<::rain_core::governor_capnp::governor_control::Client>,
        resources: Resources,
    ) -> Result<GovernorRef> {
        debug!("New governor {}", address);
        if self.graph.governors.contains_key(&address) {
            bail!("State already contains governor {}", address);
        }
        let w = GovernorRef::new(address, control, resources);
        self.graph.governors.insert(w.get_id(), w.clone());
        self.underload_governors.insert(w.clone());
        self.logger.add_new_governor_event(w.get_id());
        Ok(w)
    }

    /// Remove the governor from the graph, forcefully unassigning all tasks and objects.
    /// TODO: better specs and context of governor removal
    pub fn remove_governor(&mut self, _governor: &GovernorRef) -> Result<()> {
        unimplemented!() /*
            pub fn delete(self, graph: &mut Graph) {
        debug!("Deleting governor {}", self.get_id());
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
        graph.governors.remove(&self.get().id).unwrap();
        // assert that we hold the last reference, then drop it
        assert_eq!(self.get_num_refs(), 1);
        */
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
        self.logger
            .add_removed_client_event(client.get_id(), String::from("client disconnected"));
        Ok(())
    }

    /// Create a new session fr a client, register it in the graph.
    pub fn add_session(&mut self, client: &ClientRef, spec: SessionSpec) -> Result<SessionRef> {
        let s = SessionRef::new(self.graph.new_session_id(), client, spec.clone());
        self.graph.sessions.insert(s.get_id(), s.clone());
        self.logger
            .add_new_session_event(s.get_id(), client.get().id, spec);
        Ok(s)
    }

    /// Helper for .remove_session() and .fail_session(). Remove all session tasks,
    /// objects and cancel all finish hooks.
    fn clear_session(&mut self, s: &SessionRef) -> Result<()> {
        let session_id = s.get().id.clone();
        debug!("Clearing session {}", session_id);
        self.scheduler.clear_session(&s);

        let state_ref = self.self_ref.clone().unwrap();
        assert!(self.ignored_sessions.insert(session_id));
        let now = ::std::time::Instant::now();
        let duration = ::std::time::Duration::from_secs(IGNORE_ID_TIME_SECONDS);
        let clean_id_future = ::tokio_timer::Delay::new(now + duration)
            .map(move |()| {
                debug!("Cleaning ignored session id {}", session_id);
                state_ref.get_mut().ignored_sessions.remove(&session_id);
            })
            .map_err(|e| panic!("Cleaning ignored id failed {:?}", e));
        self.handle.spawn(clean_id_future);

        let tasks = s.get_mut().tasks.clone();
        for t in tasks {
            t.unschedule();
            self.updates.remove_task(&t);
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

    /// Remove a session and all the tasks and objects, both from the graph and from the governors,
    /// cancel all the finish hooks.
    pub fn remove_session(&mut self, session: &SessionRef) -> Result<()> {
        debug!(
            "Removing session {} of client {}",
            session.get_id(),
            session.get().client.get_id()
        );
        // remove children objects
        let has_error = session.get().error.is_some();
        if !has_error {
            self.clear_session(session)?;
            self.logger.add_closed_session_event(
                session.get_id(),
                events::SessionClosedReason::ClientClose,
                String::new());
        }
        // remove from graph
        self.graph.sessions.remove(&session.get_id()).unwrap();
        // unlink
        session.unlink();
        Ok(())
    }

    /// Put the session into a failed state, removing all tasks and objects,
    /// cancelling all finish_hooks.
    /// Debug message string is propagated together with error message
    /// it usually comes from task debug string
    pub fn fail_session(
        &mut self,
        session: &SessionRef,
        cause: String,
        debug: String,
        task_id: TaskId,
    ) -> Result<()> {
        debug!(
            "Failing session {} of client {} with cause {:?}",
            session.get_id(),
            session.get().client.get_id(),
            cause
        );
        assert!(session.get_mut().error.is_none());
        session.get_mut().error = Some(SessionError::new(cause.clone(), debug, task_id));
        // Remove all tasks + objects (with their finish hooks)
        self.clear_session(session)?;
        self.logger.add_closed_session_event(
            session.get_id(),
            events::SessionClosedReason::Error,
            cause);
        Ok(())
    }

    /// Add a new object, register it in the graph and the session.
    pub fn add_object(
        &mut self,
        session: &SessionRef,
        spec: ObjectSpec,
        client_keep: bool,
        data: Option<Vec<u8>>,
    ) -> Result<DataObjectRef> {
        if self.graph.objects.contains_key(&spec.id) {
            bail!("State already contains object with id {}", spec.id);
        }
        let oref = DataObjectRef::new(session, spec, client_keep, data);
        // add to graph
        self.graph.objects.insert(oref.get().id(), oref.clone());
        // add to updated objects
        self.updates.new_objects.insert(oref.clone());
        oref.check_consistency_opt().unwrap(); // non-recoverable
        Ok(oref)
    }

    /// Remove the object from the graph and governors (with RPC calls).
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
        self.graph.objects.remove(&oref.get().id()).unwrap();
        Ok(())
    }

    /// Add the task to the graph, checking consistency with adjacent objects.
    /// All the inputs+outputs must already be present.
    pub fn add_task(
        &mut self,
        session: &SessionRef,
        spec: TaskSpec,
        inputs: Vec<DataObjectRef>,
        outputs: Vec<DataObjectRef>,
    ) -> Result<TaskRef> {
        if self.graph.tasks.contains_key(&spec.id) {
            bail!("Task {} already in the graph", spec.id);
        }
        let tref = TaskRef::new(session, spec, inputs, outputs)?;
        // add to graph
        self.graph.tasks.insert(tref.get().id(), tref.clone());
        // add to scheduler updates
        self.updates.new_tasks.insert(tref.clone());
        tref.check_consistency_opt().unwrap(); // non-recoverable
        Ok(tref)
    }

    /// Remove task from the graph, from the governors and unlink from adjacent objects.
    /// WARNING: May leave objects without producers. You should check for them after removing all
    /// the tasks and objects in bulk.
    pub fn remove_task(&mut self, tref: &TaskRef) -> Result<()> {
        //tref.check_consistency_opt().unwrap(); // non-recoverable

        // unassign from governor
        if tref.get().assigned.is_some() {
            self.unassign_task(tref);
        }
        // Unlink from parent and objects.
        tref.unlink();
        // Remove from graph
        self.graph.tasks.remove(&tref.get().id()).unwrap();
        Ok(())
    }

    #[inline]
    pub fn is_task_ignored(&self, task_id: &TaskId) -> bool {
        self.ignored_sessions.contains(&task_id.get_session_id())
    }

    #[inline]
    pub fn is_object_ignored(&self, object_id: &DataObjectId) -> bool {
        self.ignored_sessions.contains(&object_id.get_session_id())
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
                    Err(session.get().get_error().clone().unwrap().into())
                } else {
                    Err(format!("Object {:?} not found", id).into())
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
                    Err(session.get().get_error().clone().unwrap().into())
                } else {
                    Err(format!("Task {:?} not found", id).into())
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
                    o.id(),
                    o.producer.as_ref().unwrap().get().id(),
                    o.data.as_ref().unwrap().len()
                );
            }
            if o.producer.is_none() && o.data.is_none() {
                bail!(
                    "Object {} submitted with neither producer nor data.",
                    o.id()
                );
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

    /// Assign a `Finished` object to a governor and send the object metadata.
    /// Panics if the object is already assigned on the governor or not Finished.
    pub fn assign_object(&mut self, object: &DataObjectRef, wref: &GovernorRef) {
        assert_eq!(object.get().state, DataObjectState::Finished);
        assert!(!object.get().assigned.contains(wref));
        object.check_consistency_opt().unwrap(); // non-recoverable
        wref.check_consistency_opt().unwrap(); // non-recoverable
        let empty_governor_id = ::rain_core::types::id::empty_governor_id();

        // Create request
        let mut req = wref.get().control.as_ref().unwrap().add_nodes_request();
        {
            let mut new_objects = req.get().init_new_objects(1);
            let mut co = &mut new_objects.reborrow().get(0);
            let o = object.get();
            o.to_governor_capnp(&mut co);
            let placement = o.located
                .iter()
                .next()
                .map(|w| w.get().id().clone())
                .unwrap_or_else(|| {
                    // If there is no placement, then server is the source of datobject
                    assert!(o.data.is_some());
                    empty_governor_id.clone()
                });
            placement.to_capnp(&mut co.reborrow().get_placement().unwrap());
            co.set_assigned(true);
        }

        self.handle.spawn(
            req.send()
                .promise
                .map(|_| ())
                .map_err(|e| panic!("[assign_object] Send failed {:?}", e)),
        );

        object.get_mut().assigned.insert(wref.clone());
        wref.get_mut().assigned_objects.insert(object.clone());
        object.check_consistency_opt().unwrap(); // non-recoverable
        wref.check_consistency_opt().unwrap(); // non-recoverable
    }

    // Remove object from governors (not server)
    pub fn purge_object(&mut self, object: &DataObjectRef) {
        object.unschedule();
        let assigned = object.get().assigned.clone();
        for governor in assigned {
            self.unassign_object(object, &governor);
        }
    }

    /// Unassign an object from a governor and send the unassign call.
    /// Panics if the object is not assigned on the governor.
    pub fn unassign_object(&mut self, object: &DataObjectRef, wref: &GovernorRef) {
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
            let co = &mut objects.reborrow().get(0);
            object.get().id().to_capnp(co);
        }

        {
            let o2 = object.clone();
            let w2 = wref.clone();
            self.handle
                .spawn(req.send().promise.map(|_| ()).map_err(move |e| {
                    panic!(
                        "Sending unassign_object {:?} to {:?} failed {:?}",
                        o2, w2, e
                    )
                }));
        }

        object.get_mut().assigned.remove(wref);
        wref.get_mut().assigned_objects.remove(object);
        object.get_mut().located.remove(wref); // may not be present
        wref.get_mut().located_objects.remove(object); // may not be present
        if object.get().assigned.is_empty() && object.get().state == DataObjectState::Finished {
            object.get_mut().state = DataObjectState::Removed;
            assert!(object.get().scheduled.is_empty());
            assert!(!object.get().client_keep);
        }

        object.check_consistency_opt().unwrap(); // non-recoverable
        wref.check_consistency_opt().unwrap(); // non-recoverable
    }

    /// Assign and send the task to the governor it is scheduled for.
    /// Panics when the task is not scheduled or not ready.
    /// Assigns output objects to the governor, input objects are not assigned.
    pub fn assign_task(&mut self, task: &TaskRef) {
        task.check_consistency_opt().unwrap(); // non-recoverable

        {
            // lexical scoping for `t`
            let mut t = task.get_mut();
            assert!(t.scheduled.is_some());
            assert!(t.assigned.is_none());

            // Collect input objects: pairs (object, governor_id) where governor_id is placement of object
            let mut objects: Vec<(DataObjectRef, GovernorId)> = Vec::new();

            let wref = t.scheduled.as_ref().unwrap().clone();
            t.assigned = Some(wref.clone());
            let governor_id = wref.get_id();
            let empty_governor_id = ::rain_core::types::id::empty_governor_id();
            debug!("Assiging task id={} to governor={}", t.id(), governor_id);

            for input in t.inputs.iter() {
                let o = input.get_mut();
                if !o.assigned.contains(&wref) {
                    // Just take first placement
                    let placement = o.located
                        .iter()
                        .next()
                        .map(|w| w.get().id().clone())
                        .unwrap_or_else(|| {
                            // If there is no placement, then server is the source of datobject
                            assert!(o.data.is_some());
                            empty_governor_id.clone()
                        });
                    objects.push((input.clone(), placement));
                }
            }

            for output in t.outputs.iter() {
                objects.push((output.clone(), governor_id.clone()));
                output.get_mut().assigned.insert(wref.clone());
                wref.get_mut().assigned_objects.insert(output.clone());
            }

            // Create request
            let mut req = wref.get().control.as_ref().unwrap().add_nodes_request();

            // Serialize objects
            {
                let mut new_objects = req.get().init_new_objects(objects.len() as u32);
                for (i, &(ref object, placement)) in objects.iter().enumerate() {
                    let mut co = &mut new_objects.reborrow().get(i as u32);
                    placement.to_capnp(&mut co.reborrow().get_placement().unwrap());
                    let obj = object.get();
                    obj.to_governor_capnp(&mut co);
                    // only assign output tasks - they are all assigned
                    co.set_assigned(obj.assigned.contains(&wref));
                }
            }

            // Serialize the task
            {
                let new_tasks = req.get().init_new_tasks(1);
                t.to_governor_capnp(&mut new_tasks.get(0));
            }

            self.handle.spawn(
                req.send()
                    .promise
                    .map(|_| ())
                    .map_err(|e| panic!("[assign_task] Send failed {:?}", e)),
            );

            {
                let mut w = wref.get_mut();
                w.assigned_tasks.insert(task.clone());
                w.scheduled_ready_tasks.remove(task);
            }
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

    /// Unassign task from the governor it is assigned to and send the unassign call.
    /// Panics when the task is not assigned to the given governor or scheduled there.
    pub fn unassign_task(&mut self, task: &TaskRef) {
        let wref = task.get().assigned.as_ref().unwrap().clone(); // non-recoverable

        assert!(task.get().scheduled != Some(wref.clone()));

        //task.check_consistency_opt().unwrap(); // non-recoverable
        //wref.check_consistency_opt().unwrap(); // non-recoverable

        // Create request
        let mut req = wref.get().control.as_ref().unwrap().stop_tasks_request();
        {
            let mut tasks = req.get().init_tasks(1);
            let ct = &mut tasks.reborrow().get(0);
            task.get().id().to_capnp(ct);
        }

        self.handle.spawn(
            req.send()
                .promise
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
    /// * Check if a ready task is scheduled and queue it on the governor (`scheduled_ready`).
    /// * Check if a task is assigned and not scheduled or scheduled elsewhere,
    ///   then unassign and possibly enqueue as a ready task on scheduled governor.
    /// * Check if a task is finished, then unschedule and cleanup.
    /// * Failed task is an error here.
    pub fn update_task_assignment(&mut self, tref: &TaskRef) {
        assert!(tref.get().state != TaskState::Failed);

        if tref.get().state == TaskState::NotAssigned && tref.get().waiting_for.is_empty() {
            tref.get_mut().state = TaskState::Ready;
            self.updates.tasks.insert(tref.clone());
            if let Some(ref wref) = tref.get().scheduled {
                let mut w = wref.get_mut();
                w.active_resources += tref.get().spec.resources.cpus();
            }
        }

        if tref.get().state == TaskState::Ready {
            if let Some(ref wref) = tref.get().scheduled {
                let mut w = wref.get_mut();
                w.scheduled_ready_tasks.insert(tref.clone());
            }
        }

        if tref.get().state == TaskState::Assigned
            || tref.get().state == TaskState::Running && tref.get().assigned != tref.get().scheduled
        {
            if let Some(_) = tref.get().assigned {
                // Unassign the task if assigned
                self.unassign_task(tref);
                // The state was assigned or running, now is ready
                assert_eq!(tref.get().state, TaskState::Ready);
            }
            if let Some(ref wref) = tref.get().scheduled {
                if tref.get().state == TaskState::Ready {
                    // If reported as updated by mistake, the task may be already in the set
                    let mut w = wref.get_mut();
                    w.scheduled_ready_tasks.insert(tref.clone());
                }
            }
        }

        if tref.get().state == TaskState::Finished {
            assert!(tref.get().assigned.is_none());
            tref.get_mut().scheduled = None;
        }

        tref.check_consistency_opt().unwrap(); // unrecoverable
    }

    /// Update finished object assignment to match the schedule on the given governor (optional) and
    /// needed-ness. NOP for Unfinished and Removed objects.
    ///
    /// If governor is given, updates the assignment on the governor to match the
    /// scheduling there. Object is unassigned only if located elsewhere or not needed.
    ///
    /// Then, if the object is not scheduled and not needed, it is unassigned and set to Removed.
    /// If the object is scheduled but located on more governors than scheduled on (this can happen
    /// e.g. when scheduled after the needed object was located but not scheduled), the located
    /// list is pruned to only match the scheduled list (possibly plus one remaining governor if no
    /// scheduled governors have it located).
    pub fn update_object_assignments(
        &mut self,
        oref: &DataObjectRef,
        governor: Option<&GovernorRef>,
    ) {
        let ostate = oref.get().state;
        match ostate {
            DataObjectState::Unfinished => (),
            DataObjectState::Removed => (),
            DataObjectState::Finished => {
                if let Some(ref wref) = governor {
                    if wref.get().scheduled_objects.contains(oref) {
                        if !wref.get().assigned_objects.contains(oref)
                            && oref.get().state == DataObjectState::Finished
                        {
                            self.assign_object(oref, wref);
                        }
                    } else if wref.get().assigned_objects.contains(oref)
                        && (oref.get().located.len() > 2 || !oref.get().located.contains(wref))
                    {
                        self.unassign_object(oref, wref);
                    }
                }

                // Note that the object may be already Removed here
                if oref.get().scheduled.is_empty() && oref.get().state == DataObjectState::Finished
                {
                    if !oref.get().is_needed() {
                        let assigned = oref.get().assigned.clone();
                        for wa in assigned {
                            self.unassign_object(oref, &wa);
                        }
                        oref.get_mut().state = DataObjectState::Removed;
                    }
                } else if oref.get().located.len() > oref.get().scheduled.len() {
                    for wa in oref.get().located.clone() {
                        if !oref.get().scheduled.contains(&wa) && oref.get().located.len() >= 2 {
                            self.unassign_object(oref, &wa);
                        }
                    }
                }
            }
        }
        oref.check_consistency_opt().unwrap(); // unrecoverable
    }

    /// Process state updates from one Governor.
    pub fn updates_from_governor(
        &mut self,
        governor: &GovernorRef,
        obj_updates: Vec<(DataObjectRef, DataObjectState, ObjectInfo)>,
        task_updates: Vec<(TaskRef, TaskState, TaskInfo)>,
    ) {
        debug!(
            "Update states for {:?}, objs: {}, tasks: {}",
            governor,
            obj_updates.len(),
            task_updates.len()
        );
        governor.check_consistency_opt().unwrap(); // non-recoverable

        let mut ignore_check_again = false;

        for (tref, state, info) in task_updates {
            if ignore_check_again && self.is_task_ignored(&tref.get().id()) {
                continue;
            }
            // inform the scheduler
            self.updates.tasks.insert(tref.clone());
            // set the state and possibly propagate
            match state {
                TaskState::Finished => {
                    {
                        let mut t = tref.get_mut();
                        t.session.get_mut().task_finished();
                        t.state = state;
                        t.info = info;
                        t.scheduled = None;
                        t.assigned = None;
                        let mut w = governor.get_mut();
                        w.scheduled_tasks.remove(&tref);
                        w.assigned_tasks.remove(&tref);
                        w.active_resources -= t.spec.resources.cpus();
                        self.logger.add_task_finished_event(t.id());
                    }
                    tref.get_mut().trigger_finish_hooks();
                    self.update_task_assignment(&tref);

                    for input in &tref.get().inputs {
                        // We check that need_by was really decreased to protect against
                        // task that uses objects as more inputs
                        let not_needed = {
                            let mut o = input.get_mut();
                            o.need_by.remove(&tref) && !o.is_needed()
                        };
                        if not_needed {
                            self.purge_object(&input);
                        }
                    }

                    self.underload_governors.insert(governor.clone());
                }
                TaskState::Running => {
                    let mut t = tref.get_mut();
                    assert_eq!(t.state, TaskState::Assigned);
                    t.state = state;
                    t.info = info;
                    self.logger
                        .add_task_started_event(t.id(), governor.get_id());
                }
                TaskState::Failed => {
                    debug!(
                        "Task {:?} failed on {:?} with info {:?}",
                        *tref.get(),
                        governor,
                        info
                    );
                    let error_message = if info.error.len() > 0 {
                        info.error.clone()
                    } else {
                        "Task failed, but no error attribute was set".to_string()
                    };
                    let debug_message = info.debug.clone();
                    ignore_check_again = true;
                    self.underload_governors.insert(governor.clone());
                    tref.get_mut().state = state;
                    tref.get_mut().info = info;
                    let session = tref.get().session.clone();
                    let task_id = tref.get().spec.id;
                    self.fail_session(&session, error_message.clone(), debug_message, task_id)
                        .unwrap();
                    self.logger.add_task_failed_event(
                        tref.get().id(),
                        governor.get_id(),
                        error_message,
                    );
                }
                _ => panic!(
                    "Invalid governor {:?} task {:?} state update to {:?}",
                    governor,
                    *tref.get(),
                    state
                ),
            }
        }

        for (oref, state, info) in obj_updates {
            // Inform the scheduler
            self.updates
                .objects
                .entry(oref.clone())
                .or_insert(Default::default())
                .insert(governor.clone());
            match state {
                DataObjectState::Finished => {
                    if !oref.get().assigned.contains(&governor) {
                        // We did not assign the object to the governor
                        // It means that it was an input of scheduled tasks, but object
                        // was not directly scheduled
                        continue;
                    }
                    oref.get_mut().located.insert(governor.clone());
                    governor.get_mut().located_objects.insert(oref.clone());
                    let cur_state = oref.get().state; // To satisfy the reborrow checker
                    match cur_state {
                        DataObjectState::Unfinished => {
                            {
                                // capture `o`
                                let mut o = oref.get_mut();
                                // first completion
                                o.state = state;
                                o.info = info;
                                o.trigger_finish_hooks();
                            }
                            for cref in oref.get().consumers.clone() {
                                assert_eq!(cref.get().state, TaskState::NotAssigned);
                                cref.get_mut().waiting_for.remove(&oref);
                                self.update_task_assignment(&cref);
                            }
                            if oref.get().is_needed() {
                                self.update_object_assignments(&oref, Some(governor));
                            } else {
                                self.purge_object(&oref);
                            }
                        }
                        DataObjectState::Finished => {
                            // cloning to some other governor done
                            self.update_object_assignments(&oref, Some(governor));
                        }
                        _ => {
                            panic!(
                                "governor {:?} set object {:?} state to {:?}",
                                governor,
                                *oref.get(),
                                state
                            );
                        }
                    }
                }
                _ => {
                    panic!(
                        "governor {:?} set object {:?} state to {:?}",
                        governor,
                        *oref.get(),
                        state
                    );
                }
            }
        }
        governor.check_consistency_opt().unwrap(); // non-recoverable
    }

    /// For all governors, if the governor is not overbooked and has ready messages, distribute
    /// more scheduled ready tasks to governors.
    pub fn distribute_tasks(&mut self) {
        if self.underload_governors.is_empty() {
            return;
        }
        debug!("Distributing tasks");
        for wref in &::std::mem::replace(&mut self.underload_governors, Default::default()) {
            //let mut w = wref.get_mut();
            // TODO: Customize the overbook limit
            while wref.get().assigned_tasks.len() < 128
                && !wref.get().scheduled_ready_tasks.is_empty()
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

        if self.test_mode {
            testmode::test_scheduler(self);
        }

        // Run scheduler and reset updated objects.
        let changed = self.scheduler.schedule(&mut self.graph, &self.updates);
        self.updates.clear();

        // Update assignments of (possibly) changed objects.
        for (wref, os) in changed.objects.iter() {
            for oref in os.iter() {
                self.update_object_assignments(oref, Some(wref));
            }
        }

        for tref in changed.tasks.iter() {
            self.update_task_assignment(tref);
        }
        self.underload_governors = self.graph.governors.values().map(|w| w.clone()).collect();
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

impl ConsistencyCheck for State {
    /// Check consistency of all tasks, objects, governors, clients and sessions. Quite slow.
    fn check_consistency(&self) -> Result<()> {
        debug!("Checking State consistency");
        for tr in self.graph.tasks.values() {
            tr.check_consistency()?;
        }
        for or in self.graph.objects.values() {
            or.check_consistency()?;
        }
        for wr in self.graph.governors.values() {
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
    pub fn new(
        handle: Handle,
        listen_address: SocketAddr,
        http_listen_address: SocketAddr,
        log_dir: PathBuf,
        test_mode: bool,
    ) -> Self {
        let (logger, last_session) = SQLiteLogger::new(&log_dir).unwrap();
        debug!("Session counter set to {}", last_session);
        let graph = Graph::new(last_session);

        let s = Self::wrap(State {
            graph,
            test_mode: test_mode,
            listen_address: listen_address,
            http_listen_address: http_listen_address,
            handle: handle,
            scheduler: Default::default(),
            underload_governors: Default::default(),
            updates: Default::default(),
            stop_server: false,
            self_ref: None,
            logger: Box::new(logger),
            ignored_sessions: Default::default(),
        });
        s.get_mut().self_ref = Some(s.clone());
        s
    }

    pub fn start(&self) {
        let listen_address = self.get().listen_address;
        let http_listen_address = self.get().http_listen_address;
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

        // ---- Start HTTP server ----
        //let listener = TcpListener::bind(&http_listen_address, &handle).unwrap();
        let handle1 = self.get().handle.clone();
        let state = self.clone();
        let http_server = Http::new()
            .serve_addr_handle(&http_listen_address, &handle1, move || {
                Ok(RequestHandler::new(state.clone()))
            })
            .unwrap();
        handle.spawn(
            http_server
                .for_each(move |conn| {
                    handle1.spawn(conn.map(|_| ()).map_err(|e| {
                        error!("Http connection error: {:?}", e);
                    }));
                    Ok(())
                })
                .map_err(|_| ()),
        );

        let hostname = get_hostname();
        info!(
            "Dashboard: http://{}:{}/",
            hostname,
            http_listen_address.port()
        );
        info!(
            "Lite dashboard: http://{}:{}/lite/",
            hostname,
            http_listen_address.port()
        );

        // ---- Start logging ----
        let state = self.clone();
        let interval =
            ::tokio_timer::Interval::new(Instant::now(), Duration::from_secs(LOGGING_INTERVAL));

        let logging = interval
            .for_each(move |_| {
                state.get_mut().logger.flush_events();
                Ok(())
            })
            .map_err(|e| error!("Logging error {}", e));
        handle.spawn(logging);
    }

    /// Main loop State entry. Returns `false` when the server should stop.
    pub fn turn(&self) -> bool {
        // TODO: better conditional scheduling
        if !self.get().updates.is_empty() {
            self.get_mut().run_scheduler();
            self.get().check_consistency_opt().unwrap(); // unrecoverable
        }

        // Assign ready tasks to governors (up to overbook limit)
        self.get_mut().distribute_tasks();
        !self.get().stop_server
    }

    fn on_connection(&self, stream: TcpStream, address: SocketAddr) {
        // Handle an incoming connection; spawn gate object for it

        info!("New connection from {}", address);
        stream.set_nodelay(true).unwrap();
        let bootstrap = ::rain_core::server_capnp::server_bootstrap::ToClient::new(
            ServerBootstrapImpl::new(self, address),
        ).from_server::<::capnp_rpc::Server>();

        let rpc_system = new_rpc_system(stream, Some(bootstrap.client));
        self.get()
            .handle
            .spawn(rpc_system.map_err(|e| panic!("RPC error: {:?}", e)));
    }
}
