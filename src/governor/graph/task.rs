use super::{DataObjectRef, Graph};
use common::id::TaskId;
use common::{Attributes, RcSet};
use std::sync::Arc;

use common::resources::Resources;
use common::wrapped::WrappedRcRefCell;
use std::fmt;
use governor::data::Data;

use errors::Result;

#[derive(PartialEq, Eq, Debug)]
pub enum TaskState {
    Assigned,
    Running,
    Finished,
    Failed,
}

#[derive(Debug)]
pub struct TaskInput {
    /// Input data object.
    pub object: DataObjectRef,

    /// Label may indicate the role the object plays for this task.
    pub label: String,

    /// Path to subdirectory/subblob; if input is not directory it has to be empty
    pub path: String,
}

#[derive(Debug)]
pub struct Task {
    pub(in super::super) id: TaskId,

    /// Ordered inputs for the task. Note that one object can be present as multiple inputs!
    pub(in super::super) inputs: Vec<TaskInput>,

    /// Ordered outputs for the task. Every object in the list must be distinct.
    pub(in super::super) outputs: Vec<DataObjectRef>,

    /// Unfinished objects that we wait for. These must be a subset of `inputs`,
    /// but multiplicities in `inputs` are here represented only once.
    pub(in super::super) waiting_for: RcSet<DataObjectRef>,

    pub(in super::super) state: TaskState,

    pub(in super::super) task_type: String,

    pub(in super::super) resources: Resources,

    pub(in super::super) attributes: Attributes,

    pub(in super::super) new_attributes: Attributes,
}

impl Task {
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.waiting_for.is_empty()
    }

    /// Remove data object from waiting_for list,
    /// Returns true when task becomes ready
    pub fn input_finished(&mut self, object: &DataObjectRef) -> bool {
        let found = self.waiting_for.remove(object);
        assert!(found);
        let is_ready = self.waiting_for.is_empty();
        if is_ready {
            debug!("Task id={} is ready", self.id);
        }
        is_ready
    }

    /// Get input data of the task at given index
    pub fn input_data(&self, index: usize) -> Arc<Data> {
        let object = self.inputs.get(index).unwrap().object.get();
        object.data().clone()
    }

    /// Get all input data as vector
    pub fn inputs_data(&self) -> Vec<Arc<Data>> {
        self.inputs
            .iter()
            .map(|input| input.object.get().data().clone())
            .collect()
    }

    /// Returns an error if task has different number of arguments
    pub fn check_number_of_args(&self, n_args: usize) -> Result<()> {
        if self.inputs.len() != n_args {
            bail!("Invalid number of arguments, expected: {}", n_args);
        }
        Ok(())
    }

    pub fn output(&self, index: usize) -> DataObjectRef {
        self.outputs.get(index).unwrap().clone()
    }

    pub fn set_failed(&mut self, error_message: String) {
        warn!("Task {} failed: {}", self.id, error_message);
        assert_ne!(self.state, TaskState::Failed);
        self.state = TaskState::Failed;
        if !self.new_attributes.contains("error") {
            self.new_attributes.set("error", error_message).unwrap();
        }
    }
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl TaskRef {
    pub fn new(
        graph: &mut Graph,
        id: TaskId,
        inputs: Vec<TaskInput>,
        outputs: Vec<DataObjectRef>,
        resources: Resources,
        task_type: String,
        attributes: Attributes,
    ) -> Self {
        debug!("New task id={} type={}", id, task_type);

        let waiting_for: RcSet<_> = (&inputs)
            .iter()
            .map(|input| input.object.clone())
            .filter(|obj| !obj.get().is_finished())
            .collect();

        let task = Self::wrap(Task {
            id: id,
            inputs,
            outputs,
            waiting_for,
            task_type,
            state: TaskState::Assigned,
            resources: resources,
            attributes: attributes,
            new_attributes: Attributes::new(),
        });

        for input in &task.get().inputs {
            input.object.get_mut().consumers.insert(task.clone());
        }

        graph.tasks.insert(id, task.clone());

        task
    }
}

impl fmt::Debug for TaskRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TaskRef {}", self.get().id)
    }
}
