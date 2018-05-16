/// Local macro to match variants.
/// Use as: `matchvar!(var, OutputState::MemBacked(_))`
macro_rules! matchvar {
    ($ex:expr, $pat:pat) => {{
        if let $pat = $ex {
            true
        } else {
            false
        }
    }};
}

/// Internal macro used in `register_task!`.
#[macro_export]
macro_rules! register_task_make_call {
    ($f: expr, $ins: expr, $outs: expr, (), ($($args: tt)*)) => {{
        if $ins.len() != 0 {
            Err("too many inputs")?;
        }
        if $outs.len() != 0 {
            Err("too many outputs")?;
        }
        $f($($args)*)
    }};
    ($f: expr, $ins: expr, $outs: expr, (I $($params: tt)*), ($($args: tt)*)) => {{
        let (first, more) = $ins.split_first().ok_or("not enough inputs")?;
        register_task_make_call!($f, more, $outs, ($($params)*), ($($args)*, first))
    }};
    ($f: expr, $ins: expr, $outs: expr, (Is $($params: tt)*), ($($args: tt)*)) => {{
        let no_more: &[DataInstance] = &[];
        register_task_make_call!($f, no_more, $outs, ($($params)*), ($($args)*, $ins))
    }};
    /*
    ($f: expr, $ins: expr, $outs: expr, (ID $($params: tt)*), ($($args: tt)*)) => {{
        let (first, more) = $ins.split_first().ok_or("not enough inputs")?;
        first.expect_directory()?;
        register_task_make_call!($f, more, $outs, ($($params)*), ($($args)*, first))
    }};
    ($f: expr, $ins: expr, $outs: expr, (IF $($params: tt)*), ($($args: tt)*)) => {{
        let (first, more) = $ins.split_first().ok_or("not enough inputs")?;
        first.expect_file()?;
        register_task_make_call!($f, more, $outs, ($($params)*), ($($args)*, first))
    }};
    ($f: expr, $ins: expr, $outs: expr, (IF($expr) $($params: tt)*), ($($args: tt)*)) => {{
        let (first, more) = $ins.split_first().ok_or("not enough inputs")?;
        first.expect_file()?;
        first.expect_content_type($expr)?;
        register_task_make_call!($f, more, $outs, ($($params)*), ($($args)*, first))
    }};*/
    ($f: expr, $ins: expr, $outs: expr, (O $($params: tt)*), ($($args: tt)*)) => {{
        let (first, more) = $outs.split_first_mut().ok_or("not enough outputs")?;
        register_task_make_call!($f, $ins, more, ($($params)*), ($($args)*, first))
    }};
    ($f: expr, $ins: expr, $outs: expr, (Os $($params: tt)*), ($($args: tt)*)) => {{
        let no_more: &[Output] = &[];
        register_task_make_call!($f, $ins, no_more, ($($params)*), ($($args)*, $outs))
    }};
}

/// Register a given task function with complex arguments with the subworker.
///
/// The task function should take individual input and output parameters as specified by `$params`.
/// On call, the supplied input and output lists are unpacked, both not enough and too many of each
/// parameter kinds is raised as a task error.
///
/// `$params` are surrounded with `[]` and may contain:
/// * `I` - a single `&Datainstance`.
/// * `Is` - all the remaining inputs as `&[Datainstance]`.
/// * `O` - a single `&mut Output`.
/// * `Os` - all the remaining outputs as `&mut [Output]`.
///
/// # Examples
///
/// ```rust,ignore
/// // Simple task with 2 inputs and 1 output (notice the freedom in arg order).
/// fn task_hello(_ctx: &mut Context, in1: &DataInstance, out: &mut Output, in2: &DataInstance) -> TaskResult<()> { ... }
/// // Register with:
/// register_task!(s, "hello", [I O I], task_hello);
/// ```
///
/// ```rust,ignore
/// // Task with one input, then arbitrarily many remaining inputs and one output
/// fn task_concat(_ctx: &mut Context, separator: &DataInstance, ins: &[DataInstance], out: &mut Output) -> TaskResult<()> { ... }
/// // Register with:
/// register_task!(s, "hello", [I Is O], task_hello);
/// ```

#[macro_export]
macro_rules! register_task {
    ($subworker: expr, $name: expr, [$($params: tt)*], $taskfn: expr) => ({
        $subworker.register_task($name, |ctx: &mut Context, ins: &[DataInstance], outs: &mut [Output]| -> TaskResult<()> {
            register_task_make_call!($taskfn, ins, outs, ($($params)*), (ctx))
        })
    });
}
