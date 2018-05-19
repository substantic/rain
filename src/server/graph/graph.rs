use super::{ClientRef, DataObjectRef, SessionRef, TaskRef, GovernorRef};
use common::id::{ClientId, DataObjectId, SessionId, TaskId, GovernorId};
use std::collections::HashMap;

#[derive(Clone, Default)]
pub struct Graph {
    /// Contained objects
    pub(in super::super) governors: HashMap<GovernorId, GovernorRef>,
    pub(in super::super) tasks: HashMap<TaskId, TaskRef>,
    pub(in super::super) objects: HashMap<DataObjectId, DataObjectRef>,
    pub(in super::super) sessions: HashMap<SessionId, SessionRef>,
    pub(in super::super) clients: HashMap<ClientId, ClientRef>,

    /// The last SessionId assigned.
    session_id_counter: SessionId,
}

impl Graph {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn new_session_id(&mut self) -> SessionId {
        self.session_id_counter += 1;
        self.session_id_counter
    }
}

#[cfg(test)]

mod tests {
    use super::super::{ClientRef, DataObjectRef, Graph, SessionRef, TaskInput, TaskRef, GovernorRef};
    use common::attributes::Attributes;
    use common::id::{DataObjectId, SId, TaskId};
    use common::resources::Resources;

    fn create_test_graph(
        governors: usize,
        clients: usize,
        sessions: usize,
        tasks: usize,
        objects: usize,
    ) -> Graph {
        use common::DataType;

        let g = Graph::new();
        for wi in 0..governors {
            GovernorRef::new(
                format!("0.0.0.{}:67", wi + 1).parse().unwrap(),
                None,
                Resources { cpus: 8 },
            );
        }
        for ci in 0..clients {
            let c = ClientRef::new(format!("0.0.0.{}:42", ci + 1).parse().unwrap());
            for si in 0..sessions {
                let s = SessionRef::new(si as i32, &c);
                let mut objs = Vec::new();
                for oi in 0..objects {
                    let o = DataObjectRef::new(
                        &s,
                        DataObjectId::new(s.get_id(), oi as i32),
                        Default::default(),
                        "label".to_string(),
                        DataType::Blob,
                        None,
                        Default::default(),
                    );
                    objs.push(o);
                }
                for ti in 0..tasks {
                    let mut inputs = Vec::new();
                    if ti >= 2 {
                        for i in 1..3 {
                            inputs.push(TaskInput {
                                object: objs[ti - i].clone(),
                                label: Default::default(),
                                path: Default::default(),
                            });
                        }
                    }
                    let outputs = vec![objs[ti].clone()];
                    TaskRef::new(
                        &s,
                        TaskId::new(s.get_id(), (ti + objects) as i32),
                        inputs,
                        outputs,
                        "TType".to_string(),
                        Attributes::new(),
                        Resources { cpus: 1 },
                    ).unwrap();
                }
            }
        }
        // TODO: add some governor links
        g
    }

    #[test]
    #[ignore]
    fn graph_create_delete() {
        let g = create_test_graph(4, 2, 3, 10, 20);

        assert!(!g.objects.is_empty());
        assert!(!g.governors.is_empty());

        //let client_rcs: Vec<_> = g.clients.values().map(|x| x.clone()).collect();
        //let governor_rcs: Vec<_> = g.governors.values().map(|x| x.clone()).collect();

        // FIXME!
        //for c in client_rcs { c.delete(&mut g); }
        //for w in governor_rcs { w.delete(&mut g); }

        assert!(g.clients.is_empty());
        assert!(g.governors.is_empty());
        assert!(g.tasks.is_empty());
        assert!(g.objects.is_empty());
        assert!(g.sessions.is_empty());
    }
}
