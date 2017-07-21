
use graph::common::Sid;
use graph::task::Task;
use graph::dataobj::DataObject;
use graph::worker::{Worker, WorkerId};

use std::collections::{HashMap, HashSet};


pub struct Graph {
    tasks: HashMap<Sid, Task>,
    objects: HashMap<Sid, DataObject>,
    workers: HashMap<WorkerId, Worker>,

    ready_tasks: HashSet<Task>,
}
