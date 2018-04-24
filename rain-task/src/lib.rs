#![feature(fn_traits)]
extern crate librain;

use std::collections::HashMap;

//#[derive(Debug)]
pub struct Subworker<'a> {
    tasks: HashMap<String, TaskFn<'a>>,
}

#[derive(Debug)]
pub struct Context {
//    task_id: TaskId,
//    &mut Subworker
}

#[derive(Debug)]
pub struct DataInstance {

}

#[derive(Debug)]
pub struct Output {

}

#[derive(Debug, Clone)]
pub enum Error {
    TaskNotFound(String),
}

pub type Result<T> = ::std::result::Result<T, Error>;

//pub type TaskFunction = fn(ctx: &mut Context, inputs: &[DataInstance], outputs: &mut [Output]) -> Result<()>;
//pub trait TaskFunction: 'static + Fn(&mut Context, &[DataInstance], &mut [Output]) {}

pub type TaskFn<'a> = &'a Fn(&mut Context, &[DataInstance], &mut [Output]) -> Result<()>;

pub trait TaskSimpleFunc {
    fn get_task_fn<'a>(&'a Self) -> TaskFn<'a>;
}

/*
impl<F> TaskSimpleFunc for F where F: Fn(&mut Context, &[DataInstance], &mut [Output]) -> Result<()> {
//    fn task_fn(ctx: &mut Context, ins: &[DataInstance], outs: &mut [Output]) -> Result<()> {
//        F::call(ctx, ins, outs)
//    }
    fn get_task_fn<'a>(&'a Self) -> TaskFn<'a> {

    }
}
*/
/*
impl<F> TaskSimpleFunc for F where F: Fn(&[u8]) -> Vec<u8> {
    fn get_task_fn<'a>(&'a Self) -> TaskFn<'a> {
        &|ctx, ins, outs| {
            
        }
    }
}
*/
impl<'a> Subworker<'a> {
    pub fn new() -> Self {
        Subworker { 
            tasks: HashMap::new()
        }
    }

    pub fn add_task<S: Into<String>>(&mut self, task_name: S, task_fun: TaskFn<'a>) -> Result<()> {
        let key: String = task_name.into();
        if self.tasks.contains_key(&key) {
            panic!("can't add task {:?}: already present", &key);
        }
        self.tasks.insert(key, task_fun);
        Ok(())
    }
/*
    pub fn add_task_simple<S: Into<String>, TF: 'a + TaskSimpleFunc>(&mut self, task_name: S, task_fun: TF) -> Result<()> {
        let key: String = task_name.into();
        if self.tasks.contains_key(&key) {
            panic!("can't add task {:?}: already present", &key);
        }
        self.tasks.insert(key, &TF::task_fn);
        Ok(())
    }
*/
    pub fn run() -> Result<()> {
        // Read env vars, connect, start event loop, register, ...
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn run_task_test<S: Into<String>>(&mut self, task_name: S) -> Result<()> {
        let key: String = task_name.into();
        match self.tasks.get(&key) {
            Some(f) => {
                let ins = vec![];
                let mut outs = vec![];
                f(&mut Context {}, &ins, &mut outs)
            },
            None => Err(Error::TaskNotFound(key))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task1(_ctx: &mut Context, _inputs: &[DataInstance], _outputs: &mut [Output]) -> Result<()>
    {
        Ok(())
    }

    #[test]
    fn it_works() {
        let mut s = Subworker::new();
        s.add_task("task1", &task1).unwrap();
        s.run_task_test("task1").unwrap();
        s.add_task("task2", &|_ctx, _ins, _outs| Ok(())).unwrap();
        s.run_task_test("task2").unwrap();
    }
}
