#[macro_use]
extern crate rain_task;
extern crate env_logger;

use rain_task::*;
use std::io::Write;

fn task_hello(_ctx: &mut Context, input: &DataInstance, output: &mut Output) -> TaskResult<()> {
    output.write_all(b"Hello ")?;
    output.write_all(input.get_bytes()?)?;
    output.write_all(b"!")?;
}

fn main() {
    env_logger::init();
    let mut s = Subworker::new("rusttester");
    register_task!(s, "hello", [I O], task_hello);
    s.run();
}
