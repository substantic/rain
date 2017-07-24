
use common::id::Sid;
use worker::task::Task;
use worker::dataobj::DataObject;
use std::collections::HashMap;


pub struct Graph {
    tasks: HashMap<Sid, Task>,
    objects: HashMap<Sid, DataObject>,
}
