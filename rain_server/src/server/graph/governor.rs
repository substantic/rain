use std::fmt;
use std::net::SocketAddr;
use std::rc::Rc;

use futures::Future;
use rain_core::{errors::*, types::*, utils::*};
use common::new_rpc_system;

use super::super::state::StateRef;
use super::{DataObjectRef, TaskRef};
use wrapped::WrappedRcRefCell;

pub struct Governor {
    /// Unique ID, here the registration socket address.
    id: GovernorId,

    /// Assigned tasks. The task state is stored in the `Task`.
    pub(in super::super) assigned_tasks: RcSet<TaskRef>,

    /// Scheduled tasks. Superset of `assigned_tasks`.
    pub(in super::super) scheduled_tasks: RcSet<TaskRef>,

    /// Scheduled tasks that are also ready but not yet assigned. Disjoint from
    /// `assigned_tasks`, subset of `scheduled_tasks`.
    pub(in super::super) scheduled_ready_tasks: RcSet<TaskRef>,

    // The sum of resources of scheduled tasks that may run (or are running)
    // (TODO: Generalize for Resource not only cpus)
    pub(in super::super) active_resources: u32,

    /// Obects fully located on the governor.
    pub(in super::super) located_objects: RcSet<DataObjectRef>,

    /// Objects located or assigned to appear on the governor. Superset of `located`.
    pub(in super::super) assigned_objects: RcSet<DataObjectRef>,

    /// Objects scheduled to appear here. Any objects in `located_objects` but not here
    /// are to be removed from the governor.
    pub(in super::super) scheduled_objects: RcSet<DataObjectRef>,

    /// Control interface. Optional for testing and modelling.
    pub(in super::super) control: Option<::rain_core::governor_capnp::governor_control::Client>,

    data_connection:
        Option<AsyncInitWrapper<::rain_core::governor_capnp::governor_bootstrap::Client>>,

    pub(in super::super) resources: Resources,
}

pub type GovernorRef = WrappedRcRefCell<Governor>;

impl Governor {
    #[inline]
    pub fn id(&self) -> &GovernorId {
        &self.id
    }

    /// Create a future that completes when datastore is available
    pub fn wait_for_data_connection(
        &mut self,
        governor_ref: &GovernorRef,
        state_ref: &StateRef,
    ) -> Box<
        Future<Item = Rc<::rain_core::governor_capnp::governor_bootstrap::Client>, Error = Error>,
    > {
        if let Some(ref mut store) = self.data_connection {
            return store.wait();
        }
        self.data_connection = Some(AsyncInitWrapper::new());
        let governor_ref = governor_ref.clone();
        let state_ref2 = state_ref.clone();
        let state = state_ref.get();
        Box::new(
            ::tokio_core::net::TcpStream::connect(&self.id, state.handle())
                .map(move |stream| {
                    stream.set_nodelay(true).unwrap();
                    let mut rpc_system = new_rpc_system(stream, None);
                    let bootstrap: ::rain_core::governor_capnp::governor_bootstrap::Client =
                        rpc_system.bootstrap(::capnp_rpc::rpc_twoparty_capnp::Side::Server);
                    let bootstrap_rc = Rc::new(bootstrap);
                    state_ref2
                        .get()
                        .handle()
                        .spawn(rpc_system.map_err(|e| panic!("Rpc system error: {:?}", e)));
                    governor_ref
                        .get_mut()
                        .data_connection
                        .as_mut()
                        .unwrap()
                        .set_value(bootstrap_rc.clone());
                    bootstrap_rc
                })
                .map_err(|e| e.into()),
        )
    }
}

impl GovernorRef {
    pub fn new(
        address: SocketAddr,
        control: Option<::rain_core::governor_capnp::governor_control::Client>,
        resources: Resources,
    ) -> Self {
        GovernorRef::wrap(Governor {
            id: address,
            assigned_tasks: Default::default(),
            scheduled_tasks: Default::default(),
            scheduled_ready_tasks: Default::default(),
            located_objects: Default::default(),
            assigned_objects: Default::default(),
            scheduled_objects: Default::default(),
            control: control,
            active_resources: 0,
            resources: resources,
            data_connection: None,
        })
    }

    /// Return the object ID in graph.
    pub fn get_id(&self) -> GovernorId {
        self.get().id
    }
}

impl ConsistencyCheck for GovernorRef {
    /// Check for state and relationships consistency. Only explores adjacent objects but still
    /// may be slow.
    fn check_consistency(&self) -> Result<()> {
        let s = self.get();

        if s.scheduled_tasks.is_empty() && s.active_resources != 0 {
            bail!(
                "Invalid active resources: active_resources = {}",
                s.active_resources
            );
        }

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
                bail!(
                    "scheduled_ready task ref {:?} inconsistency in {:?}",
                    tref,
                    s
                )
            }
        }
        Ok(())
    }
}

impl fmt::Debug for GovernorRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GovernorRef {}", self.get_id())
    }
}

impl fmt::Debug for Governor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Governor")
            .field("id", &self.id)
            .field("tasks", &self.assigned_tasks)
            .field("located", &self.located_objects)
            .field("assigned", &self.assigned_objects)
            .field("resources", &self.resources)
            .finish()
    }
}
