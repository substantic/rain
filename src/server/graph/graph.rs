use std::collections::HashMap;
use common::wrapped::WrappedRcRefCell;
use common::id::{SessionId, WorkerId, DataObjectId, TaskId, ClientId};
use super::{Worker, Task, DataObject, Session, Client};

#[derive(Clone, Default)]
pub struct Inner {
    /// Contained objects
    pub (in super::super) workers: HashMap<WorkerId, Worker>,
    pub (in super::super) tasks: HashMap<TaskId, Task>,
    pub (in super::super) objects: HashMap<DataObjectId, DataObject>,
    pub (in super::super) sessions: HashMap<SessionId, Session>,
    pub (in super::super) clients: HashMap<ClientId, Client>,

    /// The last SessionId assigned.
    session_id_counter: SessionId,
}

pub type Graph = WrappedRcRefCell<Inner>;

impl Graph {

    pub fn new() -> Self {
        Default::default()
    }

    pub fn new_session_id(&self) -> SessionId {
        let mut inner = self.get_mut();
        inner.session_id_counter += 1;
        inner.session_id_counter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::{Client, Session, Graph, Task, Worker, DataObject};
    use common::id::{SId, TaskId, SessionId, ClientId, DataObjectId, WorkerId};

    fn create_test_graph(workers: i32, clients: i32, sessions: i32, tasks: i32, objects: i32) ->
                                                                                           Graph {
        let mut g = Graph::new();
        for wi in 0..workers {
            let w = Worker::new(&g, format!("0.0.0.{}:67", wi + 1).parse().unwrap(), None, 8);
        }
        for ci in 0..clients {
            let c = Client::new(&g, format!("0.0.0.{}:42", ci + 1).parse().unwrap());
            for si in 0..sessions {
                let s = Session::new(&g, &c);
                for ti in 0..tasks {
                    let t = Task::new(&g, &s, TaskId::new(s.get_id(), ti));
                }
                for oi in 0..objects {
                    let o = DataObject::new(&g, &s, DataObjectId::new(s.get_id(), oi + tasks));
                }
            }
        }
        // TODO: add some links (objects, tasks, workers)
        g
    }

    #[test]
    fn graph_create_delete() {
        let mut g = create_test_graph(4, 2, 3, 10, 20);

        assert!(!g.get().objects.is_empty());
        assert!(!g.get().workers.is_empty());

        let client_rcs: Vec<_> = g.get().clients.values().map(|x| x.clone()).collect();
        let worker_rcs: Vec<_> = g.get().workers.values().map(|x| x.clone()).collect();
        for c in client_rcs { c.delete(&g); }
        for w in worker_rcs { w.delete(&g); }

        assert!(g.get().clients.is_empty());
        assert!(g.get().workers.is_empty());
        assert!(g.get().tasks.is_empty());
        assert!(g.get().objects.is_empty());
        assert!(g.get().sessions.is_empty());
    }
}