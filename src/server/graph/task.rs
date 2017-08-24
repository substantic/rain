use futures::unsync::oneshot::Sender;

use common::wrapped::WrappedRcRefCell;
use common::RcSet;
use common::id::TaskId;
use super::{DataObject, Worker, Session};

pub enum TaskState {
    NotAssigned,
    Ready,
    Assigned(Worker),
    AssignedReady(Worker),
    Running(Worker),
    Finished(Worker),
}

pub struct TaskInput {
    /// Input data object.
    object: DataObject,
    /// Label may indicate the role the object plays for this task.
    label: String,
    // TODO: add any input params or flags
}

pub struct Inner {
    /// Unique ID within a `Session`
    id: TaskId,

    /// Current state.
    state: TaskState,

    /// Ordered inputs for the task. Note that
    inputs: Vec<TaskInput>,
    outputs: RcSet<DataObject>,

    /// Unfinished objects that we wait for. These must be a subset of `inputs`,
    /// but multiplicities in `inputs` are here represented only once.
    waiting_for: RcSet<DataObject>,

    /// Worker with the scheduled task.
    assigned: Option<Worker>,

    /// Owning session. Must match `SessionId`.
    session: Session,

    /// Task type
    // TODO: specify task types or make a better type ID system
    procedure_key: String,

    /// Task configuration - task type dependent
    procedure_config: Vec<u8>,

    /// Hooks executed when the task is finished
    finish_hooks: Vec<Sender<()>>,
}

pub type Task = WrappedRcRefCell<Inner>;
