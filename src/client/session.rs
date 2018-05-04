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
use std::cell::Cell;
use std::rc::Rc;

pub type DataObjectPtr = Rc<DataObject>;
pub type TaskPtr = Rc<Task>;

pub struct Session {
    pub id: i32,
    comm: Rc<Communicator>,
    tasks: Vec<TaskPtr>,
    data_objects: Vec<DataObjectPtr>,
    id_counter: i32,
}

impl Session {
    pub fn new(id: i32, comm: Rc<Communicator>) -> Self {
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
        self.comm.submit(&self.tasks, &self.data_objects)?;
        self.tasks.clear();
        self.data_objects.clear();

        Ok(())
    }

    pub fn unkeep(&mut self, objects: &[DataObjectPtr]) -> Result<(), Box<Error>> {
        self.comm
            .unkeep(&objects.iter().map(|o| o.id).collect::<Vec<DataObjectId>>())
    }

    pub fn wait(&mut self, tasks: &[TaskPtr], objects: &[DataObjectPtr]) -> Result<(), Box<Error>> {
        self.comm.wait(
            &tasks.iter().map(|t| t.id).collect::<Vec<TaskId>>(),
            &objects.iter().map(|o| o.id).collect::<Vec<DataObjectId>>(),
        )
    }
    pub fn wait_some(
        &mut self,
        tasks: &[TaskPtr],
        objects: &[DataObjectPtr],
    ) -> Result<(Vec<TaskPtr>, Vec<DataObjectPtr>), Box<Error>> {
        let task_map: HashMap<TaskId, &TaskPtr> = tasks.iter().map(|t| (t.id, t)).collect();
        let object_map: HashMap<DataObjectId, &DataObjectPtr> =
            objects.iter().map(|o| (o.id, o)).collect();

        let (task_ids, object_ids) = self.comm.wait_some(
            &tasks.iter().map(|t| t.id).collect::<Vec<TaskId>>(),
            &objects.iter().map(|o| o.id).collect::<Vec<DataObjectId>>(),
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
        self.comm.wait(
            &vec![TaskId::new(self.id, ::common_capnp::ALL_TASKS_ID)],
            &vec![],
        )
    }

    pub fn fetch(&mut self, object: &DataObject) -> Result<Vec<u8>, Box<Error>> {
        self.comm.fetch(object.id)
    }

    pub fn blob(&mut self, data: Vec<u8>) -> DataObjectPtr {
        self.create_object("".to_owned(), Some(data))
    }

    pub(crate) fn create_object_id(&mut self) -> DataObjectId {
        let id = self.id_counter;
        self.id_counter += 1;

        DataObjectId::new(self.id, id)
    }
    pub(crate) fn create_object(&mut self, label: String, data: Option<Vec<u8>>) -> DataObjectPtr {
        let object = DataObject {
            id: self.create_object_id(),
            keep: Cell::new(false),
            label,
            data,
            attributes: Attributes::new(),
            data_type: DataType::Blob,
        };
        let rc = Rc::new(object);
        self.data_objects.push(rc.clone());

        rc
    }

    pub(crate) fn create_task_id(&mut self) -> TaskId {
        let id = self.id_counter;
        self.id_counter += 1;

        TaskId::new(self.id, id)
    }
    pub fn create_task(
        &mut self,
        command: String,
        inputs: Vec<TaskInput>,
        outputs: Vec<DataObjectPtr>,
        config: HashMap<String, String>,
        cpus: i32,
    ) -> TaskPtr {
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

        let rc = Rc::new(task);
        self.tasks.push(rc.clone());

        rc
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.comm.close_session(self.id).unwrap();
        debug!("Session {} destroyed", self.id);
    }
}
