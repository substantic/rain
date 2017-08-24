use futures::unsync::oneshot::Sender;

use common::wrapped::WrappedRcRefCell;
use common::RcSet;
use common::id::SessionId;
use super::{Client, DataObject, Task};

pub struct Inner {
    /// Unique ID
    id: SessionId,

    /// Contained tasks.
    tasks: RcSet<Task>,

    /// Contained objects
    objects: RcSet<DataObject>,

    /// Client holding the session alive
    client: Client,

    /// Hooks executed when all tasks are finished
    finish_hooks: Vec<Sender<()>>,
}

pub type Session = WrappedRcRefCell<Inner>;