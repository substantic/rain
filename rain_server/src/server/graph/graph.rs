use rain_core::types::*;
use std::collections::HashMap;

use super::{ClientRef, DataObjectRef, GovernorRef, SessionRef, TaskRef};

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
    pub fn new(session_id_counter: SessionId) -> Self {
        let mut g: Graph = Default::default();
        g.session_id_counter = session_id_counter;
        g
    }

    pub fn new_session_id(&mut self) -> SessionId {
        self.session_id_counter += 1;
        self.session_id_counter
    }
}

#[cfg(test)]

mod tests {
    use super::super::{ClientRef, DataObjectRef, GovernorRef, Graph, SessionRef, TaskRef};
    use rain_core::types::{ObjectSpec, TaskSpec, TaskSpecInput};
    use rain_core::types::{DataObjectId, SId, TaskId};
    use rain_core::types::Resources;

    fn create_test_graph(
        governors: usize,
        clients: usize,
        sessions: usize,
        tasks: usize,
        objects: usize,
    ) -> Graph {
        use rain_core::types::DataType;

        let g = Graph::new(1);
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
                    let spec = ObjectSpec {
                        id: DataObjectId::new(s.get().id, oi as i32),
                        label: Default::default(),
                        user: Default::default(),
                        data_type: DataType::Blob,
                        content_type: "".into(),
                    };

                    let o = DataObjectRef::new(&s, spec, false, None);
                    objs.push(o);
                }
                for ti in 0..tasks {
                    let mut inputs = Vec::new();
                    let mut input_objs = Vec::new();

                    if ti >= 2 {
                        for i in 1..3 {
                            let obj = &objs[ti - i];
                            input_objs.push(obj.clone());
                            inputs.push(TaskSpecInput {
                                id: obj.get().id(),
                                label: Default::default(),
                            });
                        }
                    }
                    let output_objs = vec![objs[ti].clone()];
                    let outputs: Vec<_> = output_objs.iter().map(|o| o.get().id()).collect();

                    let spec = TaskSpec {
                        id: TaskId::new(s.get_id(), (ti + objects) as i32),
                        inputs: inputs,
                        outputs: outputs,
                        task_type: "TType".to_string(),
                        resources: Resources { cpus: 1 },
                        config: None,
                        user: Default::default(),
                    };

                    TaskRef::new(&s, spec, input_objs, output_objs).unwrap();
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
