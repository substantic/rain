Getting Started
***************

Introducing Rain Applications
=============================

Rain Applications are programs **defined on client side** and **executed on
Rain infrastructure** using the Rain API. Rain automaticaly distributes
execution of the applications in a distributed environment.

Rain Applications follows the paradigm of task oriented programming. Basic
building blocks for every Rain App are tasks - generic abstraction units
representing various kinds of computations ranging from native Python tasks to
3rd party software.

Rain tasks may be arbitrarily* chained together and so provide complex
high-level functionality.


Writing your first Rain Application
===================================

This section demonstrate how to start Rain DEE locally and execute simple
"Hello world" App.

- **Start Rain infrastructue** Although, the components of Rain (server and
  worker(s)) can be started manually, in order to simplify this process, we
  provide "rain start" command to do it for you automatically. The following
  command starts server and one local worker. (Starting Rain infrastructure on
  distributed systems is described in :ref:`start-rain`.)::

  $ rain start --simple

- **Running "Hello World" example.** The following Python program creates a task
  that joins two strings (This example is more explained in Section
  :ref:`tasks-and-objs`.)::

    from rain.client import Client, tasks, blob

    # Connect to server
    client = Client("localhost")

    # Create a new session
    with client.new_session() as session:

        # Create task (and two data objects)
        task = tasks.concat(blob("Hello "), blob("world!"))

        # Mark that the output should be kept after submit
        task.output.keep()

        # Submit all crated tasks to server
        session.submit()

        # Wait for completion of task and fetch results
        result = task.outout.fetch()

        # Prints 'Hello world!'
        print(result)

