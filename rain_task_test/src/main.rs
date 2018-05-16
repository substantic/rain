#[macro_use]
extern crate rain_task;
extern crate env_logger;

use rain_task::*;
use std::io::Write;

fn task_echo(_ctx: &mut Context, input: &DataInstance, output: &mut Output) -> TaskResult<()> {
    output.write_all(input.get_bytes()?)?;
    Ok(())
}

fn main() {
    env_logger::init();
    let mut s = Subworker::new("rust_task_test");
    register_task!(s, "echo", [I O], task_echo);
    s.run();
}
