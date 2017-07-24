
use common::id::Sid;
use server::task::Task;
use server::dataobj::DataObject;
use std::collections::HashMap;


pub struct Graph {
    tasks: HashMap<Sid, Task>,
    objects: HashMap<Sid, DataObject>,
}
