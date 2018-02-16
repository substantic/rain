
User's Guide
************

Basic terms
===========

**Task** is a basic unit of work in Rain, it reads inputs and produces outputs.
Tasks are executed on computational nodes (computers where Rain workers are
running). Tasks can be external programs, python functions, and other basic
build-in tasks.

**Data objects** are objects that are read and created by tasks. Data objects
are immutable, once they are created thay cannot be changed. They are generic
data blobs with accompanying metadata. It is upto tasks to interpret the data
object content.


Task defintion and submission
=============================

Rain represents your computation as a graph of tasks and data objects. Tasks are
not eagerly executed during the graph construction. Instead, the actual
execution is managed by Rain infrastructure after explicit submission. This
leads to a programming model in which you first only **define** a graph and then
**execute** it.

Let us consider the following example, where two constant objects are created
and merged together::

  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)  # Create a connection to the server
                                      # running at localhost:7210

  with client.new_session() as session:  # Creates a session

      a = blob("Hello ")    # Create a definition of data object in the current session
      b = blob("world!")    # Create a definition of data object in the current session
      tasks.concat((a, b))  # Create a task definition in the current session
                            # that concatenates input data objects

      session.submit()      # Send the created graph into the server, where the computation
                            # is performed.
      session.wait_all()    # Wait until all submitted tasks are completed

The graph composed in the session looks as follows:

.. figure:: imgs/helloworld.svg
   :alt: Example of task graph

When the graph is constructed, all created objects and tasks are put into the
active session. For many cases, it sufficient just create one session for whole
program life time, with one submit at the end. However, it is possible to create more
sessions or built a graph gradually with more submits. More details are
covered in Section :ref:`sessions`.


Fetching data objects
=====================

Data objects produced by tasks are not transferred back to client
automatically. If needed, this can be done using the ``fetch()`` function. In
the following example, we download the result back to the Python client code.
Expression ``t.output`` refers to the data object that is the output of task
``t``::


  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)

  with client.new_session() as session:
      a = blob("Hello ")
      b = blob("world!")
      t = tasks.concat((a, b))
      t.output.keep()            # Tell server to keep result of task

      session.submit()           # Submit task graph

      result = t.output.fetch()  # Download result from the server
      print(result)              # Prints b'Hello world!'


By default, Rain automatically removes data objects that are no longer needed
for further computation. Method ``keep()`` sets a flag to a given data object
that instructs the server to keep the object until the client does not
explicitly frees it. An object is freed when the session is closed or when
``unkeep()`` method is called. Method ``keep()`` may be called only before the
submit. Method ``unkeep()`` may be called on any "kept" object any time.

Kept objects can be fetched to the client by method ``fetch()``. If the object
is not finished yet, the method blocks until the object is not finished. Note
that because of that, we did not use ``wait_all()`` in this example.


Inter-task dependencies
=======================

Naturally, an output of a task may be used as an input for another task. This
is demonstrated by the following example. In the example, we use
``tasks.sleep(T, O)`` that creates a task taking an arbitrary data object ``O``
and waits for ``T`` seconds and then returns ``O`` as its output. Being aware
that such task is not very useful in practice, we find it useful as an
intuitive example to demostrate the concept of task chaining::

  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)

  with client.new_session() as session:
      a = blob("Hello ")
      b = blob("world!")
      t1 = tasks.sleep(1.0, b)   # Wait for one second and then returns 'b'
      t2 = tasks.concat((a, t1.output))
      t2.output.keep()

      session.submit()           # Submit task graph

      result = t2.output.fetch()  #  It will wait around 1 second
                                  #  and then returns b'Hello world'

If a task produces only a single output, we can ommit ``.output`` and directly use the task
as an input for another task. In our example, we can define ``t2`` as follows::

  t2 = tasks.concat((a, t1))

This shortened notation is used in the rest of the text.


More outputs
============

A tasks may generally creates zero, one, or more outputs. All outputs are
accessible via attribute ``outputs``. That contains an instance of
:class:`rain.client.LabeledList`. It is an extension of a standard list (indexed
from zero), that also allows to access via string labels.

::

   # The following task creates two outputs labeled "output1" and "output2"
   # Details of this task is explained later
   t = tasks.execute(["tee", Output("output1")], stdout="output2", stdin=data)

   t.outputs["output1"]  # Access to output "output1"
   t.outputs["output2"]  # Access to output "output2"

   # There is also some helper functions:
   # Keep all outputs (equivalent to: for o in t.outputs: o.keep())
   t.keep_outputs()

   # After submit
   # Fetch all outputs (equivalent to: [o.fetch() for o in t.outputs])
   t.fetch_outputs()



Constant data objects
=====================


Build-in tasks
==============


Task *concat*
-------------


Task *export*
-------------


Task *open*
-----------


Task *sleep*
------------


Running external programs
=========================

Task ``task.execute``
---------------------

The whole functionality is built around build-in task ``task.execute``. When a
program is executed through ``tasks.execute``, then a new temporary directory is
created. This directory will be removed at the end of program execution. The
current working directory of the program is set to this directory.

The idea is that this directory is program's playground where input data objects
are mapped and files created in this directory may be moved out at the end as
new data objects. Unlike (as in many workflow systems), program should not work
with absolute paths but to use relative path to its working directory. Input
data objects (as files) are served by worker as needed. Worker tries to avoids
unnecessary copying of data objects when computation of following tasks in the
same worker.

If the executed program terminates with a non-zero code, then tasks fails and
standard error output is put into the error message.

The simple example looks as follow::

  tasks.execute("sleep 1")

This creates a task with no inputs and no outputs executing program "sleep" with
argument "1". Arguments are parsed in shell-like manner.
Arguments can be also explicitly as a list::

  tasks.execute(("sleep",  "1"))

Command may be also interpreted by shell, if the argument ``shell=True`` is
provided::

  tasks.execute("sleep 1 && sleep 1", shell=True)


Outputs
-------

Created files and standard output can be used as output of
:func:``task.execute``. The following example calls program ``wget`` that
downloads web page at https://github.com/ at saves it as `index.html`. The
created file is used as output of the task.

::

  from rain.client import Client, task, Output

  client = Client("localhost", 7210)

  with client.new_session() as session:
      t = tasks.execute("wget https://github.com/",
                         outputs=[Output("index", path="index.html")])
      t.output.keep()

      session.submit()
      result = t.output.fetch()

The class :class:`rain.client.Output` serves for configuring output. The first
argument is the label of the output. The argument ``path`` sets the path to
output file. It is a relative path w.r.t. the working directory of task. If the
path is not defined, then label is used as path; e.g. ``Output("my_output")`` is
equivalent to ``Output("my_output", path="my_output")``. The Output instance may
also serve for additional attributes, like its content type or size hint. Please
see the class documentation for more details.

If we do not want to configure the output, it is possible to use just string
instead of instance of ``Output``. It creates the output with the same label and
path as the given string. Therefore we can create the execute task as follows::

  t = tasks.execute("wget https://github.com/", outputs=["index.html"])

The only difference is that label of the output is now "index.html" (not
"index").

Of course, more than one output may be specified. Program ``wget`` allows
redirect its log to a file through ``--output-file`` option::

  t = tasks.execute("wget https://github.com/ --output-file log",
                    outputs=["index.html", "log"])

This creates tasks with two outputs with labels "index.html" and "log". Outputs
is available through normal multiple outputs API, e.g. ``t.outputs["log"]``.

Outputs can be also passed directly as program arguments. This is a shortcut for
passing the output path as an argument. The example above can be written as
follows::

  t = tasks.execute(["wget", "https://github.com/", "--output-file", Output("log")],
                    outputs=["index.html"])


Standard output / error output
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Inputs
------


Argument ``inputs``
~~~~~~~~~~~~~~~~~~~


Inputs in program arguments
~~~~~~~~~~~~~~~~~~~~~~~~~~~


Standard input
~~~~~~~~~~~~~~


Factory ``Program``
-------------------


Python tasks
============

Among build-in tasks, Rain allows to run additional types of tasks via
subworkers. Rain is shipped with Python subworker, that allows to execute
arbirary Python code.

Decorator ``@remote``
---------------------

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

The decorator changes the behavior of the decorated function in a way that
calling it no longer executes it in the client but creates a task that executes
the function in a python subworker. Worker starts and manages subworkers as
necessary, there is no need of any action from the user perspective.

The decorated function accepts at least one argument. As the first argument,
the context of the execution is passed to the function. Context enables some
actions within the task. It is a convetion to named this argument as ``ctx``.

The task defined has one output by default. The decorated function may return an
instance of ``bytes`` or ``str`` that will be used the value for the output. How
to define inputs or more outputs is elaborated in the following sections.


Inputs
------


Outputs
-------


Resources
=========



Attributes
==========


Content type
============


Resources
=========


Waiting for object(s) and task(s)
=================================

For waiting on completion of a single task/object there is ``wait()`` method.
For waiting on multiple objects at once, session implementes also ``wait``
method that takes a list of objects/tasks::


  with client.new_session() as session:
      a = blob("Hello world")
      t1 = tasks.sleep(1.0, a)
      t2 = tasks.sleep(2.0, a)
      session.submit()

      t1.wait()  # This blocks until t1 is finished, independently of t2
      t2.output.wait()  # Waits until a data object is not finished

      session.wait([t1, t2.output])  # This is slightly more efficient equivalent of two lines above

 
Since the object is created in the same time as task is finished, it behaves
exactly as the previous example.

.. note::

  Note that in the case of ``wait()`` (in contrast with ``fetch()``), object
  does not have to be marked as "kept".


.. _sessions:

Sessions
========

Overview
--------

The client allows to create one or more sessions. Sessions are the environment
where application may create task graphs and submit them into the server.
Sessions follows the following rules:

  * Each client may manage multiple sessions. Tasks and data object in different
    sessions are independent and they may be executed simultaneously.

  * If a client disconnects, all sessions created by the client are terminated,
    i.e. running tasks are stopped and data objects are removed.
    (Persistent sessions are not supported in the current version)

  * If any task in a session fails, the session is labeled as failed, and all
    running tasks in the session are stopped. Any access to tasks/objects in the
    session will throw an exception containing error that caused the problem.
    Destroying the session is the only operation that do not throw the exception.
    Other sessions are not affected.


Active session
--------------

Rain client maintains a global stack of sessions and ``with`` block puts a
session on the top of the stack at the beginning and removes it from the stack
when the block ends. The session on the top of the stack is called *active*. The
following example demonstrates when a session is active::

  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)

  # no session is active
  with client.new_session() as a:

      # 'a' is active

      with client.new_session() as b:
          # 'b' is active
          pass

      # 'b' is closed and 'a' is active again

  # 'a' is closed and no session is active

Tasks and data objects are always created within the scope of active session.
Note, that in the data object concatenation example above, the same active
session is used for all of the created tasks and data objects.

.. note::

   Which session is active is always local information that only influences
   tasks and data objects creation. This information is not propagated to the
   server. Submitted tasks are running regardless the session is active or not.


Closing session
---------------

Session may be closed manually by calling method ``close()``, dropping the
client connection or leaving ``with`` block. To suppress the last named
behavior you can use the ``bind_only()`` method as follows::

  session = client.new_session()

  with session.bind_only():
      # 'session' is active
      pass

  # 'session' is not active here; however it is NOT closed

Once a session is closed, it is pernamently removed from the session stack and
cannot be reused again.

.. note::

  The server holds tasks and object metadata (e.g. performance information) as
  long as a session is live. If you use a long living client with many sessions,
  sessions should be closed as soon as they are not needed.


Multiple submits
----------------

The task graph does not have to be submmited at once; multiple submmits may
occur during in during lifetime of a session. Data object from previous submits
may be used in during the construction of new submit, the only condition is that
they have to be "kept" explicitly.

::

   with client.new_session() as session:
      a = blob("Hello world")
      t1 = tasks.sleep(1.0, a)
      t1.output.keep()

      session.submit()  # First submit

      t2 = tasks.sleep(1.0, t1.output)

      session.submit()  # Second submit
      session.wait_all()  # Wait until everything is finished

      t3 = tasks.sleep(1.0, t1.output)

      session.submit()  # This submit
      session.wait_all()  # Wait again until everything is finished

Let us note that method ``wait_all()`` waits until all currently running task
are finished, regardless in which submit they arrived to the server.


Directories
-----------

TODO: Not implemented yet
