.. _tasks-and-objs:

Tasks & Data Objects
********************

Tasks and data objects are the key concepts used for to define computational
workflows.


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


Waiting for object / task
=========================

The simplest one is waiting on a single task, if ``t`` is a submitted task, we
can wait for it by calling method ``wait()``::


  with client.new_session() as session:
      a = blob("Hello world")
      t1 = tasks.sleep(1.0, a)
      t2 = tasks.sleep(2.0, a)
      session.submit()

      t1.wait()  # This blocks until t1 is finished, independently of t2
 
This call blocks the client until the task is finished. Similarly, we can wait
for a single data object::

   with client.new_session() as session:
      a = blob("Hello world")
      t1 = tasks.sleep(1.0, a)
      t2 = tasks.sleep(2.0, a)
      session.submit()

      t1.output.wait()

Since the object is created in the same time as task is finished, it behaves
exactly as the previous example. Note that in the case of ``wait()`` (in
contrast with ``fetch()``), object does not have to be marked as "kept".


.. note::

  Rain also alows to wait for multiple tasks or objects. These methods are
  described in the session section.

More terminology
================

Task, Task instances, Task types

Data object, Data instance


Debugging task graph
====================

Exporting graph to .dot
