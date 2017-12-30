
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

**Data objects** are objects that are read and created by tasks. They are
generic data blobs with accompanying metadata. It is upto tasks to interprep
content of data objects.


Submission
==========

Client composes graph of tasks and objects within the currenlty active session.
We call this graph as **task graph**. The composed graph is sent to server via
``submit()`` method on session. Let us repeat the example from the previous
section::

  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)

  with client.new_session() as session:
      a = blob("Hello ")    # Create a data object
      b = blob("world!")    # Create a data object
      tasks.concat((a, b))  # Create task that concatenates objects

      session.submit()      # Send the created graph into the server
      session.wait_all()    # Wait until all running tasks are not finished      


The graph submitted to graph looks as follows:

.. figure:: imgs/helloworld.svg
   :alt: Example of task graph

The example contains one task and three data objects. The used task creates a
new data object that is created by concatenation of inputs. Two input data
objets are uploaded into the server when task graph is submitted. The third data
object is the product of the task.

The call of method ``submit()`` is terminated as far as the task graph submitted
to the server. The method ``wait_all()`` waits until all submitted tasks is not
finished. Server starts to schedule tasks as soon as there are free resources.


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
frees it. Object is freed when the session is closed or when ``unkeep()`` method
called.

Kept objects can be fetched to the client by method ``fetch()``. If the object
is not finished yet, the method blocks until the object is not finished. Note
that we did not use ``wait_all()`` this time.


More complex plans
==================

Naturally, an output of a task may be used as an input for other. This is
demonstrated by the following example. In the example, we use ``tasks.sleep(T,
O)`` that createsa a task that takes an arbitrary data object ``O`` and waits
for ``T`` seconds and then returns ``O`` as its output. The task is usually not
much usefull, but it good for testing purposes::


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


More submits
============


More terminology
================

Task, Task instances, Task types

Data object, Data instance


Debugging task graph
====================

Exporting graph to .dot
