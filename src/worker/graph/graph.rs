
use common::id::{Id, TaskId, DataObjectId, SubworkerId};
use super::{TaskRef, DataObjectRef, SubworkerRef};
use std::collections::HashMap;


pub struct Graph {
    pub tasks: HashMap<TaskId, TaskRef>,
    pub objects: HashMap<DataObjectId, DataObjectRef>,
    pub subworkers: HashMap<SubworkerId, SubworkerRef>,

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

    pub fn make_id(&mut self) -> Id {
        self.id_counter += 1;
        self.id_counter
    }
}