use std::collections::HashMap;

use super::task::TaskInput;
use super::session::{DataObjectPtr, Session, TaskPtr};

pub trait CommonTasks {
    fn concat(&mut self, objects: &[DataObjectPtr]) -> TaskPtr;
    fn open(&mut self, filename: String) -> TaskPtr;
    fn export(&mut self, object: DataObjectPtr, filename: String) -> TaskPtr;
}

impl CommonTasks for Session {
    fn concat(&mut self, objects: &[DataObjectPtr]) -> TaskPtr {
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
    fn open(&mut self, filename: String) -> TaskPtr {
        let mut config = HashMap::new();
        config.insert("path".to_owned(), filename);

        let outputs = vec![self.create_object("".to_owned(), None)];

        self.create_task("!open".to_owned(), vec![], outputs, config, 1)
    }
    fn export(&mut self, object: DataObjectPtr, filename: String) -> TaskPtr {
        let mut config = HashMap::new();
        config.insert("path".to_owned(), filename);

        let input = TaskInput {
            label: None,
            data_object: object.clone(),
        };

        self.create_task("!export".to_owned(), vec![input], vec![], config, 1)
    }
}
