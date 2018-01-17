
Connecting to server
********************

Client
======

Client is user's gate into Rain's infrastructure. It maintains a connection to
the server. In the following example, we assume that server running on localhost
and default port 7210. The code creates a client that connects to the server::

  from rain.client import Client

  client = Client("localhost", 7210)


Sessions
========

The client allows to create sessions. Sessions are the environment where
application may create task graphs and submit them into the server. Sessions
follows the following rules:

  * Each client may have more sessions. Tasks and data object in different
    sessions are independent and they may be executed simultaneously.

  * If a client disconnects, all sessions created by the client is terminated,
    i.e. running tasks are stopped and data objects are removed.
    (Persistent sessions are not supported in the current version)

  * If any task in a session fails, session is marked as failed, and all running
    tasks in the session is stopped. Any access to tasks/objects in the session
    will throw an exception containing error that caused the problem. Destroying
    the session is the only operation that do not throw the exception. Other
    sessions are not affected.

The following example creates a session with a single tasks that concatenates
two data objects, submits it to the server and then waits until everything is
not finished::

  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)

  with client.new_session() as session:
      a = blob("Hello ")    # Create a data object
      b = blob("world!")    # Create a data object
      tasks.concat((a, b))  # Create task that concatenates objects

      session.submit()      # Send the created graph into the server
      session.wait_all()    # Wait until all running tasks are not finished      

Wrapping ``client.new_session`` into ``with`` block does two things: it sets the
session as *active* within the block and it automatically closes the session at
then end of block.


Active session
--------------

Rain maintains a global stack of sessions and ``with`` block puts
a session on the top of the stack at the beginning and pops it back
when block ends. The session on the top of the stack is called *active*.
The following example demonstrates when a session is active::

  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)

  # no session is active
  with client.new_session() as a:

      # 'a' is active

      with client.new_session() as b:
          # 'b' is active
          pass

      # 'a' is active again

  # no session is active

When you creates a tasks and data objects, there are always created in the
active session. Note, that the example with the concatenation does not
explicitly addresses session when data objects and the task is created.

.. note::

   Which session is active is always local information that only influences
   creating tasks and data objects. The information is not propagated to the
   server. Submitted tasks are running regardless if a session is active or not.


Auto-closing session
--------------------

Session may be closed manually by calling method ``close()``, dropping the
client connection or leaving ``with`` block. To suppress the last named behavior
you can use the following ``bind_only()`` method::

  session = client.new_session()

  with session.bind_only():
      # 'session' is active
      pass

  # 'session' is not active here; however it is NOT closed


.. note::

  The server holds metadata of tasks and data objects (e.g. performance
  information) as long as a session is live. If you use a long living client
  with many sessions, you should close sessions as long as you do not need them.
