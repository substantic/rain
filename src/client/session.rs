use common::wrapped::WrappedRcRefCell;

use super::communicator::Communicator;
use client::dataobject::DataObject;
use client::task::Task;
use std::collections::HashMap;
use std::error::Error;
use client::task::TaskInput;
use common::Attributes;
use common::id::TaskId;
use common::id::DataObjectId;
use common::id::SId;
use common::DataType;

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

    pub fn unkeep(&mut self, objects: &[WrappedRcRefCell<DataObject>]) -> Result<(), Box<Error>> {
        self.comm.get_mut().unkeep(objects)
    }
    pub fn wait(
        &mut self,
        tasks: &[WrappedRcRefCell<Task>],
        objects: &[WrappedRcRefCell<DataObject>],
    ) -> Result<(), Box<Error>> {
        self.comm.get_mut().wait(
            &tasks.iter().map(|t| t.get().id).collect::<Vec<TaskId>>(),
            &objects
                .iter()
                .map(|o| o.get().id)
                .collect::<Vec<DataObjectId>>(),
        )
    }
    pub fn wait_some(
        &mut self,
        tasks: &[WrappedRcRefCell<Task>],
        objects: &[WrappedRcRefCell<DataObject>],
    ) -> Result<
        (
            Vec<WrappedRcRefCell<Task>>,
            Vec<WrappedRcRefCell<DataObject>>,
        ),
        Box<Error>,
    > {
        let task_map: HashMap<TaskId, &WrappedRcRefCell<Task>> =
            tasks.iter().map(|t| (t.get().id, t)).collect();
        let object_map: HashMap<DataObjectId, &WrappedRcRefCell<DataObject>> =
            objects.iter().map(|o| (o.get().id, o)).collect();

        let (task_ids, object_ids) = self.comm.get_mut().wait_some(
            &tasks.iter().map(|t| t.get().id).collect::<Vec<TaskId>>(),
            &objects
                .iter()
                .map(|o| o.get().id)
                .collect::<Vec<DataObjectId>>(),
        )?;

        Ok((
            task_ids
                .iter()
                .filter_map(|id| task_map.get(id).map(|t| (*t).clone()))
                .collect(),
            object_ids
                .iter()
                .filter_map(|id| object_map.get(id).map(|o| (*o).clone()))
                .collect(),
        ))
    }
    pub fn wait_all(&mut self) -> Result<(), Box<Error>> {
        self.comm.get_mut().wait(
            &vec![TaskId::new(self.id, ::common_capnp::ALL_TASKS_ID)],
            &vec![],
        )
    }

    pub fn fetch(&mut self, object: &DataObject) -> Result<Vec<u8>, Box<Error>> {
        self.comm.get_mut().fetch(object.id)
    }

    pub fn blob(&mut self, data: Vec<u8>) -> WrappedRcRefCell<DataObject> {
        self.create_object("".to_owned(), Some(data))
    }

    pub(crate) fn create_object_id(&mut self) -> DataObjectId {
        let id = self.id_counter;
        self.id_counter += 1;

        DataObjectId::new(self.id, id)
    }
    pub(crate) fn create_object(
        &mut self,
        label: String,
        data: Option<Vec<u8>>,
    ) -> WrappedRcRefCell<DataObject> {
        let object = DataObject {
            id: self.create_object_id(),
            keep: false,
            label,
            data,
            attributes: Attributes::new(),
            data_type: DataType::Blob,
        };
        let rc = WrappedRcRefCell::wrap(object);
        self.data_objects.push(rc.clone());

        rc
    }

    pub(crate) fn create_task_id(&mut self) -> TaskId {
        let id = self.id_counter;
        self.id_counter += 1;

        TaskId::new(self.id, id)
    }
    pub(crate) fn create_task(
        &mut self,
        command: String,
        inputs: Vec<TaskInput>,
        outputs: Vec<WrappedRcRefCell<DataObject>>,
        config: HashMap<String, String>,
        cpus: i32,
    ) -> WrappedRcRefCell<Task> {
        let mut attributes = Attributes::new();
        attributes.set("config", config).unwrap();

        let mut resources: HashMap<String, i32> = HashMap::new();
        resources.insert("cpus".to_owned(), cpus);
        attributes.set("resources", resources).unwrap();

        let task = Task {
            id: self.create_task_id(),
            command,
            inputs,
            outputs,
            attributes,
        };

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
