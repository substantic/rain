use common::wrapped::WrappedRcRefCell;

use super::communicator::Communicator;
use client::data_object::DataObject;
use client::task::Task;
use client::task::TaskCommand;
use std::collections::HashMap;
use client::task::ConcatTaskParams;
use std::error::Error;
use client::task::TaskInput;
use common::Attributes;

#[derive(Copy, Clone, Debug)]
pub struct ObjectId {
    pub id: i32,
    pub session_id: i32,
}

impl ObjectId {
    pub fn new(id: i32, session_id: i32) -> Self {
        ObjectId { id, session_id }
    }
}

pub struct Session {
    pub id: i32,
    comm: WrappedRcRefCell<Communicator>,
    tasks: Vec<WrappedRcRefCell<Task>>,
    data_objects: Vec<WrappedRcRefCell<DataObject>>,
    id_counter: i32,
}

impl Session {
    pub fn new(id: i32, comm: WrappedRcRefCell<Communicator>) -> Self {
        debug!("Session {} created", id);

        Session {
            id,
            comm,
            tasks: vec![],
            data_objects: vec![],
            id_counter: 0,
        }
    }

    pub fn submit(&mut self) -> Result<(), Box<Error>> {
        self.comm.get_mut().submit(&self.tasks, &self.data_objects)?;
        self.tasks.clear();
        self.data_objects.clear();

        Ok(())
    }

    pub fn fetch(&mut self, object: &DataObject) -> Result<Vec<u8>, Box<Error>> {
        self.comm.get_mut().fetch(object.id)
    }

    pub fn concat(&mut self, objects: Vec<WrappedRcRefCell<DataObject>>) -> WrappedRcRefCell<Task> {
        let inputs = objects
            .iter()
            .map(|o| TaskInput {
                label: None,
                data_object: o.clone(),
            })
            .collect();

        let outputs = vec![self.create_object("".to_owned(), None)];

        self.create_task(
            TaskCommand::Concat(ConcatTaskParams { objects }),
            inputs,
            outputs,
        )
    }

    pub fn blob(&mut self, data: Vec<u8>) -> WrappedRcRefCell<DataObject> {
        self.create_object("".to_owned(), Some(data))
    }

    fn create_object(
        &mut self,
        label: String,
        data: Option<Vec<u8>>,
    ) -> WrappedRcRefCell<DataObject> {
        let object = DataObject {
            id: self.create_id(),
            keep: false,
            label,
            data,
            attributes: Attributes::new(),
        };
        let rc = WrappedRcRefCell::wrap(object);
        self.data_objects.push(rc.clone());

        rc
    }

    fn create_id(&mut self) -> ObjectId {
        let id = self.id_counter;
        self.id_counter += 1;

        ObjectId::new(id, self.id)
    }
    fn create_task(
        &mut self,
        command: TaskCommand,
        inputs: Vec<TaskInput>,
        outputs: Vec<WrappedRcRefCell<DataObject>>,
    ) -> WrappedRcRefCell<Task> {
        let mut task = Task {
            id: self.create_id(),
            command,
            inputs,
            outputs,
            attributes: Attributes::new(),
        };

        let mut resources: HashMap<String, i32> = HashMap::new();
        resources.insert("cpus".to_owned(), 1);
        task.attributes.set("resources", resources).unwrap();

        let rc = WrappedRcRefCell::wrap(task);
        self.tasks.push(rc.clone());

        rc
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.comm.get_mut().close_session(self.id).unwrap();
        debug!("Session {} destroyed", self.id);
    }
}
