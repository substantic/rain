use client::dataobject::DataObject;
use std::error::Error;
use common::wrapped::WrappedRcRefCell;
use common::Attributes;
use common::id::TaskId;

pub struct TaskInput {
    pub label: Option<String>,
    pub data_object: WrappedRcRefCell<DataObject>,
}

pub struct Task {
    pub id: TaskId,
    pub command: String,
    pub inputs: Vec<TaskInput>,
    pub outputs: Vec<WrappedRcRefCell<DataObject>>,
    pub attributes: Attributes,
}

impl Task {
    pub fn output(&self) -> Result<WrappedRcRefCell<DataObject>, Box<Error>> {
        if self.outputs.len() == 1 {
            return Ok(self.outputs[0].clone());
        }

        bail!("There is not a single output")
    }
}
