use futures::unsync::oneshot::Sender;

use common::wrapped::WrappedRcRefCell;
use common::id::DataObjectId;
use common::RcSet;
use common::keeppolicy::KeepPolicy;
use server::task::Task;
use server::worker::Worker;
use server::session::Session;

pub enum DataObjectState {
    NotAssigned,
    Assigned,
    Finished(usize),
    Removed(usize),
}

pub struct Inner {
    /// Unique ID within a `Session`
    id: DataObjectId,

    /// Producer task, if any.
    producer: Option<Task>,

    /// Label may be the role that the output has in the `producer`, or it may be
    /// the name of the initial uploaded object.
    label: String,

    /// Current state.
    state: DataObjectState,

    /// Consumer set, e.g. to notify of completion.
    consumers: RcSet<Task>,

    /// Workers with full copy of this object.
    located: RcSet<Worker>,

    /// Workers that have been instructed to pull this object or already have it.
    /// Superset of `located`.
    assigned: RcSet<Worker>,

    /// Assigned session. Must match SessionId.
    session: Session,

    /// Reasons to keep the object alive
    keep: KeepPolicy,

    /// Hooks executed when the task is finished
    finish_hooks: Vec<Sender<()>>,
}

pub type DataObject = WrappedRcRefCell<Inner>;
