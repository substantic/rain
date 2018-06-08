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

fn task_meta(ctx: &mut Context, input: &DataInstance, output: &mut Output) -> TaskResult<()> {
    // Copy input user attr to output user attr
    let uo: UserValue = input.spec.user.get("test").ok_or_else(
        || Err("Expected input user attribute \"foo\"".to_owned()))?;
    output.set_user_info("test", uo);
    // Copy task spec.user attr to task info.user attr
    let ut: UserValue = ctx.spec.user.get("test").ok_or_else(
        || Err("Expected input user attribute \"foo\"".to_owned()))?;
    ctx.set_user_info("test", ut);
    output.set_content_type(input.get_content_type())?;
    output.write_all(input.get_bytes()?)?;
    Ok(())
}

fn main() {
    env_logger::init();
    let mut s = Executor::new("rusttester");
    register_task!(s, "meta", [I O], task_meta);
    register_task!(s, "hello", [I O], task_hello);
    register_task!(s, "fail", [I], task_fail);
    register_task!(s, "panic", [], task_panic);
    s.run();
}
