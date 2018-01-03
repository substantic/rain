.. _tasks-and-objs:

Tasks & Data Objects
********************

Tasks and data objects are core concepts that allows to define computation
submited into server.


Basic terms
===========

**Task** is a basic unit of work in Rain, it reads some inputs and produces
some outputs. Tasks are executed on computational nodes (computers where Rain
workers are running). Tasks are defined as external programs, python functions,
and there are build-in tasks.

**Data objects** are objects that are read and created by tasks. Data objects
are immutable, once they are created thay cannot be changed. They are generic
data blobs with accompanying metadata. It is upto tasks to interprep data object
contents.


Submission
==========

The client composes graph of tasks and objects within the currently active
session. We call this graph as **task graph**. The composed graph is sent to the
server via ``submit()`` method on session. Let us repeat the example from the
previous section::

  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)

  with client.new_session() as session:
      a = blob("Hello ")    # Create a data object
      b = blob("world!")    # Create a data object
      tasks.concat((a, b))  # Create task that concatenates objects

      session.submit()      # Send the created graph into the server
      session.wait_all()    # Wait until all running tasks are not finished      

Call :func:`rain.client.blob` creates a data object with a content defined by
the client, that is takes as the first argument; allowed types are ``bytes`` or
``str``. Data are uploaded together during the submission as a part of task
graph. Task created by :func:`rain.client.tasks.concat` takes arbitrary blobs
and creates a new data object by concatenating the inputs. Therefore the graph
contains three data objects and one task. The graph submitted to the server
looks as follows:

.. figure:: imgs/helloworld.svg
   :alt: Example of task graph

The call of method ``submit()`` is finished as far as the task graph submitted
to the server. The method ``wait_all()`` waits until all submitted tasks are not
finished.


Fetching data objects
=====================

In the example above, we wait until the task is not finished, but the result of
the task is thrown away. In the following example, we download result to our
Python code. Expression ``t.output`` refrers to the data object that is the
output of task ``t``::


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


By default, Rain automatically removes data objects when there is no unfinished
tasks that needs it as an input. Method ``keep()`` sets a flag to a given object
that tells the server to keep the object until the client does not explicitly
frees it. An object is freed when the session is closed or when ``unkeep()``
method is called. Method ``keep()`` may be called only before the submit. Method
``unkeep()`` may be called on any "kept" object any time.

Kept objects can be fetched to the client by method ``fetch()``. If the object
is not finished yet, the method blocks until the object is not finished. Note
that because of that, we did not use ``wait_all()`` in this example.


More complex plans
==================

Naturally, an output of a task may be used as an input for another task. This is
demonstrated by the following example. In the example, we use ``tasks.sleep(T,
O)`` that creates a task taking an arbitrary data object ``O`` and waits for
``T`` seconds and then returns ``O`` as its output. This task is usually not
much useful, but it good for testing purposes::

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

If a task has exactly one output, we can ommit ``.output`` and directly use the task
as an input for another task. In our example, we can define ``t2`` as follows::

  t2 = tasks.concat((a, t1))

This shorten way is used in the rest of the text.


Labels
======


More outputs
============


Attributes
==========


Content type
============


Resources
=========


Waiting on tasks and objects
============================

So far, we have use waiting for all tasks in a session via ``wait_all()`` or we
fetch (and potentially wait for) a kept object. Rain offers additional options
for waiting on tasks and objects.


Waiting on single object and tasks
----------------------------------

The simplest one is waiting on a single task, if ``t`` is a submitted task, we
can wait for it by calling method ``wait()``::


  with client.new_session() as session:
      a = blob("Hello world")
      t1 = tasks.sleep(1.0, a)
      t2 = tasks.sleep(2.0, a)
      session.submit()

      t1.wait()  # This blocks until t1 is not finished, independantly on t2
 
This call blocks the client until the task is not finished. In the same way,
we can wait for a single data object::

   with client.new_session() as session:
      a = blob("Hello world")
      t1 = tasks.sleep(1.0, a)
      t2 = tasks.sleep(2.0, a)
      session.submit()

      t1.output.wait()

Since the object is created in the same time as task is finished, it behaves
exactly as example above. Note that in the case of ``wait()`` (unlike
``fetch()``), object does not have to be marked as "kept".


Waiting for more tasks and objects
----------------------------------

For waiting on more objects, there is call session method
``wait(tasks_and_objects)``. It takes a collection of tasks and objects and
blocks until all of them are not finished. The example shows waiting for two
tasks explicitly::

   with client.new_session() as session:
      a = blob("Hello world")
      t1 = tasks.sleep(1.0, a)
      t2 = tasks.sleep(2.0, a)
      session.submit()

      session.wait([t1, t2])


Wait until some of task/data object are not finished
------------------------------------------------------

TODO: session.wait_some(...)



More submits
============

The task graph does not have to be submmited at once, but more submmits may
occur during in during lifetime of a session. Data object from previous submits
may be used in during the construction of new submit, the only condition is that
they have to be "kept".

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

Note: Method ``wait_all()`` waits until all currently running task are not finished,
regardless in which submit they arrived to the server.


More terminology
================

Task, Task instances, Task types

Data object, Data instance


Debugging task graph
====================

Exporting graph to .dot
