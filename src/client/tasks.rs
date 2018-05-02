use client::task::TaskInput;
use client::session::Session;
use common::wrapped::WrappedRcRefCell;
use client::dataobject::DataObject;
use client::task::Task;
use std::collections::HashMap;

pub trait CommonTasks {
    fn concat(&mut self, objects: &[WrappedRcRefCell<DataObject>]) -> WrappedRcRefCell<Task>;
    fn open(&mut self, filename: String) -> WrappedRcRefCell<Task>;
}

impl CommonTasks for Session {
    fn concat(&mut self, objects: &[WrappedRcRefCell<DataObject>]) -> WrappedRcRefCell<Task> {
        let inputs = objects
            .iter()
            .map(|o| TaskInput {
                label: None,
                data_object: o.clone(),
            })
            .collect();

        let outputs = vec![self.create_object("".to_owned(), None)];

        self.create_task("!concat".to_owned(), inputs, outputs, HashMap::new(), 1)
    }
    fn open(&mut self, filename: String) -> WrappedRcRefCell<Task> {
        let mut config = HashMap::new();
        config.insert("path".to_owned(), filename);

        let outputs = vec![self.create_object("".to_owned(), None)];

        self.create_task("!open".to_owned(), vec![], outputs, config, 1)
    }
}
