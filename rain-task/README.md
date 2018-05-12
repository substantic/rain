# Rust subworker library for Rain

This library allows you to easily write efficient custom Rain tasks in Rust.

## Implementing your tasks

Implement your tasks as:

```rust
fn task_hello(ctx: &Context, inputs: &[DataInstance], outputs: &[Output]) -> Result<()> { ... }
```

Then create a binary with the following main:

```rust
use rain_task::*;
fn main() {
    let mut s = Subworker::new("greeter"); // The subworker type name
    s.add_task("hello", task_hello);
    s.add_task("world", task_world);
    s.run().unwrap(); // Runs the subworker event loop
}
```

## Running

The subworker is run by a worker in a dedicated working directory.
Worker parameters are passed via environment variables, so you are free to use any argument parsing.

You may setup a rust logger before `Subworker::run()` but be warned that the logs are not as conveniently accessible as the debug/error messages returned with the tasks (both failed and successful).

The task function is ran with the working dir set to its dedicated empty directory which is cleaned
for you afterwards.

## Error handling

The subworker library has a simplified error handling: Usage errors of the `rain-task` library (e.g. writing data to an output that was already set to an external file) lead to panics. Any situation that could lead to an inconsistent state (or would be very hard to recover) leads to panic. This includes most I/O errors (with the exception of TODO).

The task functions should only return `Err` in cases where the subworker is in a consistent state. This includes *task usage errors*, i.e. the user of the task supplied the wrong type or number of inputs, wrong input values etc. But panicking in that situation is also acceptable.

The rationale is that the subworker crash is properly reported to the user and is a clean and safe way to handle arbitrary errors within the subworker. While it may be an expensive operation to restart the subworker, it is expected to be very infrequent. With this logic, even task-usage errors may lead to panics without any significant loss. (Only some meta-data may be lost while panicking).


 