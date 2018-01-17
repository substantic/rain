
Python tasks
************

Among build-in tasks, Rain allows to run additional tasks via subworkers. Rain
is shipped with Python subworker, that allows to execute arbirary Python code.

Decorator ``@remote``
=====================

Decorator :func:`rain.client.remote` serves to turing a python function into a
Rain task. Let us consider the following example::

  from rain.client import Client, remote

  @remote()
  def hello(ctx):
      return "Hello world!"

  client = Client("localhost", 7210)

  with client.new_session() as session:
      t = hello()                # Create a task
      t.output.keep()
      session.submit()
      result = t.output.fetch()
      print(result)              # Prints b'Hello world!'

The decorator changes the behavior of the decorated function in way that calling
it no longer executes it but creates task that executes the function in a python
subworker. Worker starts and manages subworkers as necessary, there is no need
of any action from the user perspective.

The decorated function has to take at least one argument. As the first argument,
the context of the execution is passed to the function. Context enables some
actions within the task. It is a convetion to named this argument as ``ctx``.

The task defined has one output by default. The decorated function may return an
instance of ``bytes`` or ``str`` that will be used the value for the output. How
to define inputs or more outputs is elaborated in the following sections.


Inputs
======


Outputs
=======


Resources
=========
