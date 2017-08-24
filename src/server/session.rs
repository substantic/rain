use futures::unsync::oneshot::Sender;

use common::id::SessionId;
use server::client::Client;
use server::dataobj::DataObject;
use server::task::Task;
use common::wrapped::WrappedRcRefCell;
use common::RcSet;

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