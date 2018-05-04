use std::error::Error;
use common::Attributes;
use common::id::TaskId;
use super::session::DataObjectPtr;

pub struct TaskInput {
    pub label: Option<String>,
    pub data_object: DataObjectPtr,
}

pub struct Task {
    pub id: TaskId,
    pub command: String,
    pub inputs: Vec<TaskInput>,
    pub outputs: Vec<DataObjectPtr>,
    pub attributes: Attributes,
}

impl Task {
    pub fn output(&self) -> Result<DataObjectPtr, Box<Error>> {
        if self.outputs.len() == 1 {
            return Ok(self.outputs[0].clone());
        }

        bail!("There is not a single output")
    }
}
