use super::session::DataObjectPtr;
use rain_core::types::{TaskId, TaskSpec};

pub struct Task {
    pub spec: TaskSpec,
    pub outputs: Vec<DataObjectPtr>,
}

impl Task {
    pub fn output(&self) -> DataObjectPtr {
        assert_eq!(self.outputs.len(), 1, "Task has multiple outputs");

        self.outputs[0].clone()
    }
    pub fn id(&self) -> TaskId {
        self.spec.id
    }
}
