use super::*;
use std::{fs, env, mem};

#[derive(Debug)]
pub struct Context<'a> {
    /// The call message the Context was created for.
    spec: &'a CallMsg,
    /// List of input objects. This is empty during task function call!
    pub(crate) inputs: Vec<DataInstance<'a>>,
    /// List of output objects. This is empty during task function call!
    pub(crate) outputs: Vec<Output<'a>>,
    /// Task attributes
    pub(crate) attributes: Attributes,
    /// Absolute path to task working dir
    pub(crate) work_dir: PathBuf,
    /// Absolute path to staging dir with input and output objects
    staging_dir: PathBuf,
    /// Success flag, initially true
    pub(crate) success: bool,
}

impl<'a> Context<'a> {
    pub(crate) fn for_call_msg(cm: &'a CallMsg, staging_dir: &Path, work_dir: &Path) -> Self {
        assert!(work_dir.is_absolute());
        let inputs = cm.inputs.iter().enumerate().map(|(order, inp)| {
            DataInstance::new(inp, work_dir, order)
        }).collect();
        let outputs = cm.outputs.iter().enumerate().map(|(order, outp)| {
            Output::new(outp, staging_dir, order)
        }).collect();
        Context {
            spec: cm,
            inputs: inputs,
            outputs: outputs,
            attributes: Attributes::new(),
            work_dir: work_dir.into(),
            staging_dir: staging_dir.into(),
            success: true,
        }
    }

    pub(crate) fn into_result_msg(self) -> ResultMsg {
        ResultMsg {
            task: self.spec.task,
            success: self.success,
            attributes: self.attributes,
            outputs: self.outputs.into_iter().map(|o| {
                let (os, _cached) = o.into_output_spec();
                os
                }).collect(),
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
        debug!("Calling {:?} in {:?}", self.spec.method, self.work_dir);
        let res = f(self, &inputs, &mut outputs);
        mem::swap(&mut outputs, &mut self.outputs);
        mem::swap(&mut inputs, &mut self.inputs);
        res
    }

    // TODO: add inputs number checking, outputs number checking, attribute access, debug to attrs,
    // some reflection (e.g. access to spec)
}
