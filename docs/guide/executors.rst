
Writing Own Executors
*********************

.. figure:: imgs/arch.svg
   :alt: Connection between basic components of Rain

This section covers how to write a new executor, i.e. how to create a program
that introduces new tasks type to Rain. A governor spawns and stops executors
as needed according tasks that are assigned to it. Each tasks always specifies
what kind of executor it needs.

There are generally two types of executors: **Universal executors** and
**Specialized executors**. The universal one allows to execute an arbitrary code
and specialized offers a fix of tasks that they provide.

The current version of Rain supports universal executor for Python. This is how
`remove()` decorator works. It serializes a decorated function into a data
object and creates a task that needs Python executor that executes it.

For languages where code cannot be simply transferred in portable way, Rain
offers **tasklibs**, a libraries for writing specialized executors. The current
version provides tasklibs for C++ and Rust. A tasklib allows to create a
stand-alone program that know how to communicate with governor and provides a
set of functions.

This sections covers how to write new tasks using tasklibs for C++ and Rust and
how to create run this tasks from client.

Note: Governor itself also provides some of basic task types, that are provided
through a virtual executor called **buildin**. You may see this "executor" in
dashboard.


Rust tasklib
============

TODO


C++ tasklib
===========

.. note::
  C++ tasklib is not fully finished. It allows to write basic task types, but
  some of more advanced features (e.g. working with attributes) are not
  implemented yet.


Installation of C++ tasklib
---------------------------

TODO


Getting started
---------------

The following code shows how to create an executor named "example1" that
provides one task type "hello". This task takes one blob as the input,
and returns one blob as the output.

.. highlight:: c++

::

  #include <tasklib/executor.h>

  int main()
  {
    // Create executor, the argument is the name of the executor
    tasklib::Executor executor("example1");

    // Register task "hello"
    executor.add_task("hello", [](tasklib::Context &ctx, auto &inputs, auto &outputs) {

        // Check that we been called exactly with 1 argument.
        // If not, the error message is set to context
        if (!ctx.check_n_args(1)) {
            return;
        }

        // This is body of our task, in our case, it reads the input data object
        // inserts "Hello" before the input and appends "!"
        auto& input1 = inputs[0];
        std::string str = "Hello " + input1->read_as_string() + "!";

        // Create new data instance and set it as one (and only) result
        // of the task
        outputs.push_back(std::make_unique<tasklib::MemDataInstance>(str));
    });

    // Connect to governor and serve registered tasks
    // This function is never finished.
    executor.start();
  }


Registration in governor
====================================

TODO


Client API
==========

TODO
