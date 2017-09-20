use futures::unsync::oneshot;
use std::fmt;

use errors::Error;

use common::convert::ToCapnp;
use common::wrapped::WrappedRcRefCell;
use common::id::{DataObjectId, SId};
use common::{RcSet, Additional};
use super::{TaskRef, WorkerRef, SessionRef, Graph, TaskState};
pub use common_capnp::{DataObjectState, DataObjectType};
use errors::Result;

#[derive(Debug)]
pub struct DataObject {
    /// Unique ID within a `Session`
    pub(in super::super) id: DataObjectId,

    /// Producer task, if any.
    pub(in super::super) producer: Option<TaskRef>,

    /// Label may be the role that the output has in the `producer`, or it may be
    /// the name of the initial uploaded object.
    pub(in super::super) label: String,

    /// Current state.
    pub(in super::super) state: DataObjectState,

    /// The type of this object.
    pub(in super::super) object_type: DataObjectType,

    /// Consumer set, e.g. to notify of completion.
    pub(in super::super) consumers: RcSet<TaskRef>,

    /// Workers scheduled to have a full copy of this object.
    pub(in super::super) scheduled: RcSet<WorkerRef>,

    /// Workers that have been instructed to pull this object or already have it.
    /// Superset of `located`.
    pub(in super::super) assigned: RcSet<WorkerRef>,

    /// Workers with full copy of this object.
    pub(in super::super) located: RcSet<WorkerRef>,

    /// Assigned session. Must match SessionId.
    pub(in super::super) session: SessionRef,

    /// The object is requested to be kept by the client.
    pub(in super::super) client_keep: bool,

    /// Hooks executed when the task is finished
    pub(in super::super) finish_hooks: Vec<oneshot::Sender<()>>,

    /// Final size if known. Must match `data` size when `data` present.
    pub(in super::super) size: Option<usize>,

    /// Optinal *final* data when submitted from client or downloaded
    /// by the server (for any reason thinkable).
    pub(in super::super) data: Option<Vec<u8>>,

    /// Additional attributes (WIP)
    pub(in super::super) additional: Additional,
}

impl DataObject {

    /// To capnp for worker message
    /// It does not fill "placement", it has to be done, byt callie
    pub fn to_worker_capnp(&self,
                           builder: &mut ::worker_capnp::data_object::Builder) {
        self.id.to_capnp(&mut builder.borrow().get_id().unwrap());
        builder.set_type(self.object_type);
        self.size.map(|s| builder.set_size(s as i64));
        builder.set_label(&self.label);

        // TODO: Additionals
        // TODO: Object state (or remove it)
    }

    /// Inform observers that task is finished
    fn trigger_finish_hooks(&mut self) {
        for sender in ::std::mem::replace(&mut self.finish_hooks, Vec::new()) {
            match sender.send(()) {
                Ok(()) => { /* Do nothing */}
                Err(e) => { /* Just log error, it is non fatal */
                               debug!("Failed to inform about finishing dataobject");
                }
            }
        }
    }

    pub fn set_state(&mut self, worker: &WorkerRef, new_state: DataObjectState, size: Option<usize>) {
        self.located.insert(worker.clone());
        self.state = new_state;
        self.size = size;
        match new_state {
            DataObjectState::Finished => self.trigger_finish_hooks(),
            _ => { /* do nothing */ }
        };
    }

    /// Wait until dataobject is finished
    pub fn wait(&mut self) -> oneshot::Receiver<()> {
        let (sender, receiver) = oneshot::channel();
        match self.state {
            DataObjectState::Finished => sender.send(()).unwrap(),
            _ => self.finish_hooks.push(sender)
        };
        receiver
    }

    #[inline]
    pub fn state(&self) -> DataObjectState {
        self.state
    }
}

pub type DataObjectRef = WrappedRcRefCell<DataObject>;

impl DataObjectRef {
    pub fn new(graph: &mut Graph,
               session: &SessionRef,
               id: DataObjectId,
               object_type: DataObjectType,
               client_keep: bool,
               label: String,
               data: Option<Vec<u8>>,
               additional: Additional) -> Result<Self> {
        assert_eq!(id.get_session_id(), session.get_id());
        if graph.objects.contains_key(&id) {
            bail!("Object {} was already in the graph", id);
        }
        let s = DataObjectRef::wrap(DataObject {
            id: id,
            producer: Default::default(),
            label: label,
            state: if data.is_none() {
                DataObjectState::Unfinished
            } else {
                DataObjectState::Finished
            } ,
            object_type: object_type,
            consumers: Default::default(),
            scheduled: Default::default(),
            located: Default::default(),
            assigned: Default::default(),
            session: session.clone(),
            client_keep: client_keep,
            finish_hooks: Vec::new(),
            size: data.as_ref().map(|v| v.len()),
            data: data,
            additional: additional,
        });
        // add to graph
        graph.objects.insert(s.get().id, s.clone());
        // add to session
        session.get_mut().objects.insert(s.clone());
        Ok(s)
    }

    /// Check for state and relationships consistency. Only explores adjacent objects but still
    /// may be slow.
    pub fn check_consistency(&self) -> Result<()> {
        let s = self.get();
        // ID consistency
        if s.id.get_session_id() != s.session.get_id() {
            bail!("ID and Session ID mismatch in {:?}", s);
        }
        // reference symmetries
        for wr in s.assigned.iter() {
            if !wr.get().assigned_objects.contains(self) {
                bail!("assigned asymmetry in {:?}", s);
            }
        }
        for wr in s.scheduled.iter() {
            if !wr.get().scheduled_objects.contains(self) {
                bail!("scheduled asymmetry in {:?}", s);
            }
        }
        for wr in s.located.iter() {
            if !wr.get().located_objects.contains(self) {
                bail!("located asymmetry in {:?}", s);
            }
            if !s.assigned.contains(wr) {
                bail!("located at not-assigned worker in {:?}", s);
            }
        }
        if !s.session.get().objects.contains(self) {
            bail!("session assymetry in {:?}", s);
        }
        if let Some(ref tr) = s.producer {
            if !tr.get().outputs.contains(self) {
                bail!("object missing in producer {:?} outputs in {:?}", tr, s);
            }
        }
        // producer consistency
        if let Some(ref pr) = s.producer {
            let p = pr.get();
            if s.state == DataObjectState::Unfinished && p.state == TaskState::Finished {
                bail!("producer finished state inconsistency in {:?}", s);
            }
            if s.state == DataObjectState::Finished && p.state != TaskState::Finished {
                bail!("producer not finished state inconsistency in {:?}", s);
            }
            if let Some(ref swr) = p.scheduled {
                if !s.scheduled.contains(swr) {
                    bail!("not scheduled to producer worker in {:?}");
                }
            }
            if let Some(ref swr) = p.assigned {
                if !s.assigned.contains(swr) {
                    bail!("not assigned to producer worker in {:?}");
                }
            }
        } else {
            if s.state == DataObjectState::Finished {
                if s.data.is_none() {
                    bail!("no data present for object without producer in {:?}", s);
                }
            }
        }
        // state consistency
        if !match s.state {
            DataObjectState::Unfinished =>
                s.scheduled.len() <= 1 && s.assigned.len() <= 1 && s.producer.is_some(),
            DataObjectState::Finished =>
                s.data.is_some() || (s.located.len() >= 1 && s.assigned.len() >= 1),
            DataObjectState::Removed =>
                s.located.is_empty() && s.scheduled.is_empty() && s.assigned.is_empty() &&
                s.finish_hooks.is_empty() && s.size.is_some() && s.data.is_none(),
        } {
            bail!("state inconsistency in {:?}", s);
        }
        // data consistency
        if let Some(ref dr) = s.data {
            if s.size.is_none() || dr.len() != s.size.unwrap() {
                bail!("size and uploaded data mismatch in {:?}", s);
            }
        }
        // finish hooks
        if !s.finish_hooks.is_empty() && s.state != DataObjectState::Unfinished {
            bail!("finish hooks for finished/removed object in {:?}", s);
        }
        // keepflag and empty assigned (via Removed state)
        // NOTE: Finished state already requires nonemplty locations
        if s.client_keep && s.state == DataObjectState::Removed {
            bail!("client_keep flag on removed object {:?}", s);
        }
        // used or kept objects must be assigned when their producers are
        if (s.client_keep || !s.consumers.is_empty()) && s.assigned.is_empty() &&
                s.state == DataObjectState::Unfinished {
            if let Some(ref prod) = s.producer {
                let p = prod.get();
                if p.state == TaskState::Assigned || p.state == TaskState::Running {
                    bail!(
                    "Unfinished object is not assigned when it's producer task is in {:?}", s);
                }
            }
        }
        Ok(())
    }


    pub fn delete(self, graph: &mut Graph) {
        let inner = self.get_mut();
        assert!(inner.consumers.is_empty(), "Can only remove objects without consumers.");
        assert!(inner.producer.is_none(), "Can only remove objects without a producer.");
        // remove from workers
        for w in inner.assigned.iter() {
            assert!(w.get_mut().assigned_objects.remove(&self));
        }
        for w in inner.located.iter() {
            assert!(w.get_mut().located_objects.remove(&self));
        }
        // remove from owner
        assert!(inner.session.get_mut().objects.remove(&self));
        // remove from graph
        graph.objects.remove(&inner.id).unwrap();
        // assert that we hold the last reference, , then drop it
        assert_eq!(self.get_num_refs(), 1);
    }
}

impl ::std::fmt::Debug for DataObjectRef {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "DataObjectRef {}", self.get().id)
    }
}

impl fmt::Debug for DataObjectState {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", match *self {
            DataObjectState::Unfinished => "Unfinished",
            DataObjectState::Finished => "Finished",
            DataObjectState::Removed => "Removed",
        })
    }
}

impl fmt::Debug for DataObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", match *self {
            DataObjectType::Blob => "Blob",
            DataObjectType::Directory => "Directory",
            DataObjectType::Stream => "Stream",
        })
    }
}
