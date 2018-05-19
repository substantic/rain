use super::{DataObjectRef, ExecutorRef, TaskRef};
use common::RcSet;
use common::id::{DataObjectId, Id, ExecutorId, TaskId};
use std::collections::HashMap;
use governor::tasks::TaskInstance;

pub struct Graph {
    pub ready_tasks: Vec<TaskRef>,
    pub running_tasks: HashMap<TaskId, TaskInstance>,
    pub tasks: HashMap<TaskId, TaskRef>,
    pub objects: HashMap<DataObjectId, DataObjectRef>,
    pub executors: HashMap<ExecutorId, ExecutorRef>,
    pub idle_executors: RcSet<ExecutorRef>,

    /// List of unsued objects, the value is time when it should be freed
    /// This is list is periodically checked
    pub delete_wait_list: HashMap<DataObjectRef, ::std::time::Instant>,

    /// Last assigned id
    id_counter: Id,
}

impl Graph {
    pub fn new() -> Self {
        Self {
            ready_tasks: Vec::new(),
            running_tasks: HashMap::new(),
            tasks: HashMap::new(),
            objects: HashMap::new(),
            executors: HashMap::new(),
            idle_executors: Default::default(),
            delete_wait_list: Default::default(),
            id_counter: 0,
        }
    }

    pub fn make_id(&mut self) -> Id {
        self.id_counter += 1;
        self.id_counter
    }
}
