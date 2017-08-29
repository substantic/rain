
use common::id::{Id, TaskId, DataObjectId, SubworkerId};
use super::{Task, DataObject, Subworker};
use std::collections::HashMap;


pub struct Graph {
    tasks: HashMap<TaskId, Task>,
    objects: HashMap<DataObjectId, DataObject>,
    subworkers: HashMap<SubworkerId, Subworker>,

    /// Last assigned id
    id_counter: Id
}

impl Graph {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            objects: HashMap::new(),
            subworkers: HashMap::new(),
            id_counter: 0
        }
    }

    pub fn add_subworker(&mut self, subworker: Subworker) {
        info!("Subworker registered subworker_id={}", subworker.id());
        let subworker_id = subworker.id();
        self.subworkers.insert(subworker_id,subworker);
        // TODO: Someone probably started subworker and he wants to be notified
    }

    pub fn make_subworker_id(&mut self) -> Id {
        self.make_id()
    }

    pub fn make_id(&mut self) -> Id {
        self.id_counter += 1;
        self.id_counter
    }
}