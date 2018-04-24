extern crate rain_task;

use rain_task::{Context, DataInstance, Output, Result, Subworker};

/*
// One possibility: macro magic
#[rain_task("sometask")]
pub fn sometask(ctx: &mut Context, in1: &DataInstance, in2: &DataInstance, out1: &mut Output) -> Result<()> {
    write!(out1, "Length of in1 is {}, type of in2 is {}", in1.len(), in2.content_type())?;
    out1.set_context_type("text")?;
    // Set user attributes to anything serializable
    ctx.set_attribute("my-attr", [42, 43])?;
}
*/

pub fn task1(ctx: &mut Context, inputs: &[DataInstance], outputs: &mut [Output]) -> Result<()> {
    Ok(())
}

pub fn main() {
    let s = Subworker::new();
    /*
    // macro to add fixed arity function
    add_task!(s, 2, 1, sometask).unwrap();
    // add variadic task
    */
    s.add_task("task1", &task1).unwrap();
    s.run().unwrap();
}