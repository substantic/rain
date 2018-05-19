# Rust executor library for Rain

This library allows you to easily write efficient custom Rain tasks in Rust.

## Implementing your tasks

Implement your tasks as:

```rust
#[macro_use] // For register_task! if you want to use it
use rain_task::*;

// Generic task tages arrays of inputs and outputs
fn task_hello(ctx: &mut Context, inputs: &[DataInstance], outputs: &mut [Output]) -> TaskResult<()> { ... }

// Or you can have a funtion with the individual parameters
fn task_world(ctx: &mut Context, in1: &DataInstance, in2: &DataInstance, out: &mut Output) -> TaskResult<()> { ... }
```

Then create a binary target with the following `main` function:

```rust
fn main() {
    let mut s = Executor::new("greeter"); // The executor type name
    // Generic tasks are registered with a function
    s.register_task("hello", task_hello);
    // For individual parameters, use a macro. `[I I O]` specifies the type and order of parameters (after Context)
    register_task!(s, "world", [I I O], task_world);
    s.run(); // Runs the executor event loop
}
```

## Running

The executor is run by a governor in a dedicated working directory.
Governor parameters are passed via environment variables, so you are free to use any argument parsing.

You may setup a rust logger before `Executor::run()` but remember that the logs are not as conveniently accessible as the debug/error messages returned with the tasks (both failed and successful).

The task function is ran with the working dir set to its dedicated empty directory which is cleaned
for you afterwards.

## Error handling

The executor library has a simplified error handling: Usage errors of the `rain-task` library itself (e.g. writing data to an output that was already set to an external file) lead to panics. Any situation that could lead to an inconsistent state (or would be very fragile to recover) leads to panic. This includes all the I/O errors within library code (with the exception of `impl Write for Output` I/O errors).

The task functions themselves should generally return `Err` in cases where the executor is in a consistent state. This includes mostly *task usage errors*, i.e. the user of the task supplied the wrong type or number of inputs in the graph, wrong input values or content-types etc. But panicking in that situation is also acceptable.

The rationale is that the executor crash is properly reported to the user and is a clean and safe way to handle arbitrary errors within the executor. While it may be an expensive operation to restart the executor, it is expected to be very infrequent. With this logic, even task-usage errors may panic without any significant loss, but it is preferred to return errors as this may retain more meta-data.


