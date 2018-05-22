#[macro_use]
extern crate rain_task;
extern crate env_logger;

use rain_task::*;
use std::io::Write;

fn task_hello(_ctx: &mut Context, input: &DataInstance, output: &mut Output) -> TaskResult<()> {
    output.write_all(b"Hello ")?;
    output.write_all(input.get_bytes()?)?;
    output.write_all(b"!")?;
    Ok(())
}

fn task_fail(_ctx: &mut Context, input: &DataInstance) -> TaskResult<()> {
    let message : &str = ::std::str::from_utf8(input.get_bytes()?).unwrap_or("Invalid message");
    Err(message.into())
}

fn task_panic(_ctx: &mut Context) -> TaskResult<()> {
    panic!("The task panicked on purpose, by calling task 'panic'");
}

fn main() {
    env_logger::init();
    let mut s = Executor::new("rusttester");
    register_task!(s, "hello", [I O], task_hello);
    register_task!(s, "fail", [I], task_fail);
    register_task!(s, "panic", [], task_panic);
    s.run();
}
