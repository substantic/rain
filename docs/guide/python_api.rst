**********
Python API
**********

The Rain Pyton API consists of two domains that observe the 
workflow graph differently:

* Code run at the client, creating sessions and task graphs,
  executing and querying sessions. There, the tasks are only
  created and declared, never actually executed. 

* Python code that runs inside Pyhton tasks on the remote workers.
  This code has access to the actual input data, but only sees the adjacent
  data objects.

Client API
==========

.. py:currentmodule:: rain.client

.. autoclass:: RainException

.. autoclass:: RainWarning

Client
------

.. autoclass:: Client
   :members:

Session
-------

.. autoclass:: Session
   :members:

DataObject
----------

.. autofunction:: blob

.. autoclass:: DataObject
   :members:

Tasks
-----

.. autoclass:: Task
   :members:


Input and Output
----------------

.. autoclass:: Input
   :members:

.. autoclass:: Output
   :members:

Builtin tasks and external programs
-----------------------------------

Native Rain tasks to be run at the workers.

.. automodule:: rain.client.tasks
   :members:
   :undoc-members:

.. py:currentmodule:: rain.client

.. autoclass:: Program
   :members:

Remote Python tasks
===================

.. autofunction:: remote