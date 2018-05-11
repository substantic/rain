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