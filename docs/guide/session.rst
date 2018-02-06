
Connecting to server
********************

Client
======

Client is an user interface into Rain Distributed Execution Engine that
maintains a connection to the server. In the following example, we assume that
the server is running on localhost and default port 7210. The following code
creates a client object that connects to the server::

  from rain.client import Client

  client = Client("localhost", 7210)


Sessions
========

The client allows to create sessions. Sessions are the environment where
application may create task graphs and submit them into the server. Sessions
follows the following rules:

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

The following example creates a session with a single task that concatenates
two data objects, submits it to the server and then waits until everything is
completed::

  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)

  with client.new_session() as session:
      a = blob("Hello ")    # Create a data object
      b = blob("world!")    # Create a data object
      tasks.concat((a, b))  # Create task that concatenates objects

      session.submit()      # Send the created graph into the server
      session.wait_all()    # Wait until all running tasks are not finished      

Wrapping ``client.new_session()`` into ``with`` block does two things: it sets
the session as *active* within the block and it automatically closes the
session at the end of block.


Submission
----------

The client composes graph of tasks and objects within the currently active
session. We call this graph as **task graph**. The composed graph is sent to
the server using the ``submit()`` method on session. Let us repeat the example
from the previous section::

  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)

  with client.new_session() as session:
      a = blob("Hello ")    # Create a data object
      b = blob("world!")    # Create a data object
      tasks.concat((a, b))  # Create task that concatenates objects

      session.submit()      # Send the created graph into the server
      session.wait_all()    # Wait until all running tasks are completed      

Call :func:`rain.client.blob` creates a data object with a content defined by
its first argument; allowed types are ``bytes`` or ``str``. Data are uploaded
together during the submission as a part of task graph. Task created by
:func:`rain.client.tasks.concat` takes an arbitrary set of blobs and creates a
new data object by concatenating the inputs. Therefore, the graph in this
example contains three data objects and one task. The graph submitted to the
server looks as follows:

.. figure:: imgs/helloworld.svg
   :alt: Example of task graph

The call of method ``submit()`` is finished as far as the task graph submitted
to the server. The method ``wait_all()`` waits until all submitted tasks are
finished.


Waiting for multiple tasks and objects
--------------------------------------

So far, we have only use waiting for all tasks in a session via ``wait_all()``
or we fetch (and potentially wait for) a kept data object. Rain also allows to
wait for multiple objects at the same time. This can be done using session
method ``wait(tasks_and_objects)``.

Method ``wait(tasks_and_objects)`` takes a collection of tasks and objects and
blocks until **all** of them are completed. The example shows how to wait for
the completion of two specific tasks::

   with client.new_session() as session:
      a = blob("Hello world")
      t1 = tasks.sleep(1.0, a)
      t2 = tasks.sleep(2.0, a)
      session.submit()

      session.wait([t1, t2])

.. note::

  Method ``wait_some(tasks_and_objects)`` is yet to be implemented.


Active session
--------------

Rain maintains a global stack of sessions and ``with`` block puts a session on
the top of the stack at the beginning and removes it from the stack when the
block ends. The session on the top of the stack is called *active*. The
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

Tasks and data objects are always created within the scope of
active session. Note, that in the data object concatenation example above,
the same active session is used for all of the created tasks and data objects.

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

  The server holds metadata avout tasks and data objects (e.g. performance
  information) as long as a session is live. If you use a long living client
  with many sessions, sessions should be closed as soon as they are not needed.


Multiple submits
----------------

The task graph does not have to be submmited at once; multiple submmits may
occur during in during lifetime of a session. Data object from previous submits
may be used in during the construction of new submit, the only condition is that
they have to be "kept" explicitely.

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

.. note::

  Method ``wait_all()`` waits until all currently running task are finished,
  regardless in which submit they arrived to the server.
