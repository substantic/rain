**********
Python API
**********

The Rain Pyton API consists of two domains that observe the
workflow graph differently, although the concepts are similar
and some classes are used in both contexts.

* Code run at *the client*, creating sessions and task graphs,
  executing and querying sessions. There, the tasks are only
  created and declared, never actually executed.

* Python code that runs inside remote Pyhton tasks on the governors.
  This code has access to the actual input data, but only sees the adjacent
  data objects (input and output).

Client API
==========

.. py:currentmodule:: rain.client

.. autoclass:: RainException

.. autoclass:: RainWarning

.. autoclass:: rain.common.ID

.. py:currentmodule:: rain.client

Client
------

One instance per connection to a server.

.. autoclass:: Client
   :members:

Session
-------

One instance per a constructed graph (possibly with multiple submits).
Tied to one `Client`.

.. autoclass:: Session
   :members:

Data objects
------------

Tied to a `Session`.

.. autofunction:: blob

.. autofunction:: pickled

.. autoclass:: DataObject
   :members:

Tasks
-----

Tied to a `Session`.

.. autoclass:: Task
   :members:

Attributes
----------



Input and Output
----------------

These are helper objects are used to specify task
input and output attributes. In particular, specifying
an `Output` is the preferred way to set properties of the
output `DataObject`.

.. autoclass:: InputBase
   :members:

.. autoclass:: Input
   :members:

.. autoclass:: InputDir
   :members:

.. autoclass:: OutputBase
   :members:

.. autoclass:: Output
   :members:

.. autoclass:: OutputDir
   :members:



Builtin tasks and external programs
-----------------------------------

Native Rain tasks to be run at the governors.

.. automodule:: rain.client.tasks
   :members:
   :undoc-members:

.. py:currentmodule:: rain.client

.. autoclass:: Program
   :members:

Data instance objects
---------------------

Tied to a session and a `DataObject`. Also used in :ref:`sec-remote`.

.. autoclass:: rain.common.DataInstance
   :members:

.. py:currentmodule:: rain.client

Resources
---------

.. note:: TODO: Describe and document task resources.

.. autoclass:: rain.common.LabeledList
   :members:

Labeled list
------------

.. autoclass:: rain.common.LabeledList
   :members:

.. _sec-remote:

Remote Python tasks
===================

.. py:currentmodule:: rain.client

API for creating routines to be run at the governors.
Created by the decorating with `remote` (preferred) or
by `Remote`.

Whe **specifying** the remote task in the client code, the relevant classes are
`Remote`, `Input`, `Output`, `RainException`, `RainWarning`, `LabeledList`
and the decorateor `remote`.

**Inside** the running remote task, only
`RainException`, `RainWarning`, `LabeledList`, `DataInstance` and `Context`
are relevant.




The inputs of a `Remote` task are arbitrary python objects
containing a `DataInstance` in place of every `DataObject`,
or loaded data object if `autoload=True` or `load=True` is
set on the `Input`.

The remote should return a list, tuple or `LabeledList` of
`DataInstance` (created by `Context.blob()`), `bytes` or `string`.

.. autofunction:: remote

.. autoclass:: Remote
   :members:

