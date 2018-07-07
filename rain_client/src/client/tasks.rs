use std::collections::HashMap;

use super::session::{DataObjectPtr, Session, TaskPtr};
use rain_core::types::TaskSpecInput;

pub trait CommonTasks {
    fn concat(&mut self, objects: &[DataObjectPtr]) -> TaskPtr;
    fn open(&mut self, filename: String) -> TaskPtr;
    fn export(&mut self, object: DataObjectPtr, filename: String) -> TaskPtr;
}

fn builtin(action: &str) -> String {
    return format!("buildin/{}", action);
}

impl CommonTasks for Session {
    fn concat(&mut self, objects: &[DataObjectPtr]) -> TaskPtr {
        let inputs = objects
            .iter()
            .map(|o| TaskSpecInput {
                label: "".to_owned(),
                id: o.id(),
            })
            .collect();

        let outputs = vec![self.create_object("".to_owned(), None)];

        self.create_task(builtin("concat"), inputs, outputs, HashMap::new(), 1)
    }
    fn open(&mut self, filename: String) -> TaskPtr {
        let mut config = HashMap::new();
        config.insert("path".to_owned(), filename);

        let outputs = vec![self.create_object("".to_owned(), None)];

        self.create_task(builtin("open"), vec![], outputs, config, 1)
    }
    fn export(&mut self, object: DataObjectPtr, filename: String) -> TaskPtr {
        let mut config = HashMap::new();
        config.insert("path".to_owned(), filename);

        let input = TaskSpecInput {
            label: "".to_owned(),
            id: object.id(),
        };

        self.create_task(builtin("export"), vec![input], vec![], config, 1)
    }
}
