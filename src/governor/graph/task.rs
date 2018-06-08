use super::{DataObjectRef, Graph};
use common::{RcSet, TaskInfo, TaskSpec};
use std::sync::Arc;

use common::wrapped::WrappedRcRefCell;
use governor::data::Data;
use std::fmt;

use errors::Result;

#[derive(PartialEq, Eq, Debug)]
pub enum TaskState {
    Assigned,
    Running,
    Finished,
    Failed,
}

#[derive(Debug)]
pub struct Task {
    pub(in super::super) spec: TaskSpec,

    pub(in super::super) info: TaskInfo,

    pub(in super::super) state: TaskState,

    /// Ordered inputs for the task. Note that one object can be present as multiple inputs!
    pub(in super::super) inputs: Vec<DataObjectRef>,

    /// Ordered outputs for the task. Every object in the list must be distinct.
    pub(in super::super) outputs: Vec<DataObjectRef>,

    /// Unfinished objects that we wait for. These must be a subset of `inputs`,
    /// but multiplicities in `inputs` are here represented only once.
    pub(in super::super) waiting_for: RcSet<DataObjectRef>,
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
            debug!("Task id={} is ready", self.spec.id);
        }
        is_ready
    }

    /// Get input data of the task at given index
    pub fn input_data(&self, index: usize) -> Arc<Data> {
        let object = self.inputs.get(index).unwrap().get();
        object.data().clone()
    }

    /// Get all input data as vector
    pub fn inputs_data(&self) -> Vec<Arc<Data>> {
        self.inputs
            .iter()
            .map(|input| input.get().data().clone())
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
        warn!("Task {} failed: {}", self.spec.id, error_message);
        assert_ne!(self.state, TaskState::Failed);
        self.state = TaskState::Failed;
        if self.info.error != "" {
            self.info.error = format!("{}\n{}", self.info.error, error_message);
        } else {
            self.info.error = error_message;
        }
    }
}

pub type TaskRef = WrappedRcRefCell<Task>;

impl TaskRef {
    pub fn new(
        graph: &mut Graph,
        spec: TaskSpec,
        inputs: Vec<DataObjectRef>,
        outputs: Vec<DataObjectRef>,
    ) -> Self {
        let id = spec.id;
        debug!("New task id={} type={}", id, spec.task_type);

        let waiting_for: RcSet<_> = (&inputs)
            .iter()
            .map(|obj| obj.clone())
            .filter(|obj| !obj.get().is_finished())
            .collect();

        let task = Self::wrap(Task {
            inputs,
            outputs,
            waiting_for,
            spec,
            state: TaskState::Assigned,
            info: Default::default(),
        });

        for input in &task.get().inputs {
            input.get_mut().consumers.insert(task.clone());
        }

        graph.tasks.insert(id, task.clone());
        task
    }
}

impl fmt::Debug for TaskRef {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TaskRef {}", self.get().spec.id)
    }
}
