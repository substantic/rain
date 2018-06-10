use super::*;
use std::{env, fs, mem};

/// State of the processed Task instance and its specification.
#[derive(Debug)]
pub struct Context {
    /// The call message the Context was created for.
    pub spec: TaskSpec,
    /// The resulting task info
    pub(crate) info: TaskInfo,
    /// List of input objects. This is empty during task function call!
    pub(crate) inputs: Vec<DataInstance>,
    /// List of output objects. This is empty during task function call!
    pub(crate) outputs: Vec<Output>,
    /// Absolute path to task working dir
    pub(crate) work_dir: PathBuf,
    /// Absolute path to staging dir with input and output objects
    staging_dir: PathBuf,
    /// Success flag, initially true
    pub(crate) success: bool,
}

impl Context {
    pub(crate) fn for_call_msg(cm: CallMsg, staging_dir: &Path, work_dir: &Path) -> Self {
        assert!(work_dir.is_absolute());
        let inputs = cm
            .inputs
            .into_iter()
            .enumerate()
            .map(|(order, inp)| DataInstance::new(inp, work_dir, order))
            .collect();
        let outputs = cm
            .outputs
            .into_iter()
            .enumerate()
            .map(|(order, outp)| Output::new(outp, staging_dir, order))
            .collect();
        Context {
            spec: cm.spec,
            info: TaskInfo::default(),
            inputs: inputs,
            outputs: outputs,
            work_dir: work_dir.into(),
            staging_dir: staging_dir.into(),
            success: true,
        }
    }

    pub(crate) fn into_result_msg(self) -> ResultMsg {
        ResultMsg {
            task: self.spec.id,
            success: self.success,
            info: self.info,
            outputs: self
                .outputs
                .into_iter()
                .map(|o| {
                    let (os, _cached) = o.into_output_spec();
                    os
                })
                .collect(),
            cached_objects: Vec::new(),
        }
    }

    /// Call a task function within the context
    pub(crate) fn call_with_context<'f>(&mut self, f: &'f TaskFn) -> TaskResult<()> {
        env::set_current_dir(&self.work_dir).expect("error on chdir to task work dir");
        let mut outputs = Vec::new();
        let mut inputs = Vec::new();
        // Inputs and outputs are swapped out from the Context to hand over to the task.
        mem::swap(&mut outputs, &mut self.outputs);
        mem::swap(&mut inputs, &mut self.inputs);
        debug!("Calling {:?} in {:?}", self.spec.task_type, self.work_dir);
        let res = f(self, &inputs, &mut outputs);
        mem::swap(&mut outputs, &mut self.outputs);
        mem::swap(&mut inputs, &mut self.inputs);
        res
    }

    /// Sets the `info.user[key]` to value.
    ///
    /// Any old value is overwriten.
    pub fn set_user_info(&mut self, key: impl Into<String>, val: UserValue) {
        self.info.user.insert(key.into(), val);
    }

    /// Set the state of the task to failed with given message
    pub fn fail(&mut self, mut msg: String) {
        if msg.is_empty() {
            msg = "(unspecified error)".into();
        }
        debug!("Task {} failed: {}", self.spec.id, msg);
        self.success = false;
        self.info.error = msg;
    }

    // TODO: add inputs number checking, outputs number checking, attribute access, debug to attrs,
    // some reflection (e.g. access to spec)
}
