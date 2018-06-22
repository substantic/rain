
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


Building
--------

To compile the example we need to creating following file structure:

* myexecutor

  * myexecutor.cpp  -- Source code of our example

  * CMakeFile.txt -- CMake configuration file. The content is below.

  * tasklib -- Copy of tasklib from Rain repository (located in ``rain/cpp/tasklib``)


Content of ``CMakeFile.txt`` is following::

  cmake_minimum_required(VERSION 3.1)
  project(myexecutor)

  add_subdirectory(tasklib)

  add_executable(myexecutor
                myexecutor.cpp)

  target_include_directories(myexecutor PUBLIC ${CBOR_INCLUDE_DIRS} ${CMAKE_CURRENT_SOURCE_DIR}/src)
  target_link_libraries (myexecutor tasklib ${CBOR_LIBRARIES} pthread)


Now, we can build the executor as follows::

  $ cd myexecutor
  $ mkdir _build
  $ cd _build
  $ cmake ..
  $ make


Registration in governor
========================

When you write your own executors, you have to registrate them in the governor.
For this purpose, you have to create a configuration file for governor.

As an example, let us assume that we want to register called "example1".

::

   [executors.example1]
       command = "/path/to/executor/binary"

The configuration is in TOML format. If we save it as ``/path/to/config.toml``
we can provide the path to the governor by starting as follows::

  rain governor <SERVER_ADDRESS> --config=/path/to/config.toml

or if you are using "rain start"::

  rain start --simple --governor-config=/path/to/config

More about starting Rain can be found at :ref:`start-rain`.


Client API
==========

.. highlight:: py

This section describes how to call own tasks from Python API.

Each task contains a string value called ``task_type`` that specifies executor
and function. It has format ``<EXECUTOR>/<FUNCTION>``.
So far we have created (and registered) own executor called ``example1``
that provides task ``hello``. The task type is ``example1/hello`.

The followig code creates a class ``Hello`` that serves for calling our task::


   from rain.client import Task


   class Hello(Task):
       """ Task takes one blob as input and puts b"Hello " before
           and "!" after the input. """

       TASK_TYPE = "example1/hello"

       def __init__(self, obj):
            # Define task with one input and one output,
            # Outputs may be a (labelled) list of data objects or a number.
            # If a number is used than it creates the specified number of blob outputs
            super().__init__(inputs=(obj,), outputs=1)


This class can be used to create task in task graph in the same way as tasks
from module ``rain.client.tasks``, e.g.::

  with client.new_session() as session:
      a = blob("Hello world")
      t = Hello(a)
      session.submit()
      print(t.output.fetch().get_bytes())  # prints b"Hello WORLD!"
