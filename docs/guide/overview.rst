Overview
********

**Rain** is an open-source distributed computational framework for large-scale
 task-based pipelines.

Rain aims to lower the entry barrier to the world of distributed computing and
to do so efficiently and within any scale. Our intention is to develop a light
yet robust distributed framework that features an intuitive Python_ API,
straightforward installation and deployment with insightful monitoring on top.

.. _Python: https://www.python.org/

.. note::

  This is an early release of Rain that is already usable and quite efficient,
  with a polished Python API, but there is still a lot that can be improved.

  Most importantly, we are looking for external users and collaborators to drive
  our future work, both enthusiasts, from the industry and the scientific
  community. Talk to us online at Gitter_ or via email and let us know what your
  project needs and use-cases, submit bugs or feature requests at GitHub_ or
  even contribute with pull requests.


* **Dataflow programming.** Computation in Rain is defined as a flow graph of
  tasks. Tasks may be build-in functions, Python code, or an external
  applications, short and light or long-running and heavy. The system is
  designed to integrate any code into a pipeline, respecting its resource
  requirements, and to handle very large task graphs (hundreds thousands tasks).

* **Easy to use.** Rain was designed to be easy to deployed anywhere, ranging
  from a single node deployments to large-scale distributed systems and clouds
  ranging thousands of cores.

* **Rust core, Python API.** Rain is written in Rust_ for safety and efficiency
  and has a high-level Python API to Rain core infrastructure, and even supports
  Python tasks out-of-the-box. Nevertheless, Rain core infrastructure provides
  language-indepedent inteface that does not prevent adding support for other
  languages in the future.

* **Monitoring** Rain is designed to support both online and postmortem
  monitoring.

.. _Rust: https://www.rust-lang.org/en-US/
.. _GitHub: https://github.com/substantic/rain
.. _Gitter: https://gitter.im/substantic/rain

** :doc:`Get started now. </quickstart>` **


What is in the box
==================

Rain infrastructure composes of a central **server** component and **worker**
components, that may run on different machines. A worker may spawn one or more
**subworkers** that are local processes that provides execution of an external
code. Rain is distributed with Python subworker. Workers communicate via
direct connections to exchange data.

Users interacts with server via
**client** applications. Rain is distributed with Python client API.


.. figure:: imgs/arch.svg
   :alt: Connection between basic compoenents of Rain


Python Client
-------------

   * Task-based programming model.
   * High-level interface to Rain core infrastructure.
   * Easy definition of various types of tasks and their inter-dependencies.
   * Python3 module.

Rain Core Infrastructure
------------------------

   * Basic schedulling heuristic respecting inter-task dependencies.
   * Basic dashboard for execution monitoring.
   * Rust implementation enabling easy build, deployment, and reliable run.
   * Distributed as all-in-one binary.
   * Direct worker-to-worker communication.
   * Basic dashboard for execution monitoring.


Future directions
=================

There are many things to improve, and even more new things to add. To work
efficiently, we need to prioritize and for that we need your feedback and use
cases. Which features would you like to see and put to good use? What kind of
pipelines do you run?


Better dashboard
----------------

Better interactive view on the current and past computation status, including
post-mortem analysis. Which stats and views give you the most insight?


Better scheduler
----------------

While surprisingly efficient, the current scheduler is currently mostly based on
heuristics and rules. We plan to replace it with an incremental global scheduler
based on belief propagation.


Resiliency
----------

The current version supports and propagates some failures (remote python task
exceptions, external program errors) but other errors still cause server panic
(e.g. worker node failure). The near-term goal is to have better failure modes
for introspection and possibly recovery. The system is designed to allow
building resiliency against task or worker failures via checkpoints in the task
graph (keeping file copies). It is not clear how useful to our users this would
be but it is on our radar.

Resources
---------

Currently, the only resources supported are CPU cores. We are working on also
supporting memory requirements, but other resources (GPUs, TPUs, disk space,
...) should be possible with enough work and interest.


Directory and stream objects support
------------------------------------

Currently, only plain file objects are supported (with optional content type
hints). We are working on also supporting arbitrary directories, picking just a
subset of files for transport and "lazy" remote access. This will also allow for
simple map/array data types for large volumes. Some tasks work in a streaming
fashion and it would be inefficient to wait for their entire output before
starting a consumer task. We plan to include streaming data objects but there
are semantic and usage issues about resources, scheduling, multiple consumers
and resiliency.


Plain C/C++ tasks and subworkers
--------------------------------

Right now, the availabe tasks are either built in, external programs or python
routines. It should be possible and straightforward to turn your C or C++ (or
other language) function to a custom task by creating a new subworker. We plan a
simple C library subworker scaffold that will allow easy gray-box subworkers.
You do not have to link agains Rain, which should make deployment easier.


REST client interface
---------------------

The capnp API is a bit heavy-handed for a client API. We plan to create a REST
API for the client applications, simplifying API creation in new languages, and
to unify it with the dashboard/status query API. External REST apis are
convenient for many users and they do not seem to be a performance bottleneck.


Easier Deployment in cloud settings
-----------------------------------

The Rust binary is already one staically linked file and one python-only
library, making distribution easy and running on PBS is already supported. We
would like to add better support for cloud settings, e.g. AWS and Kubernetes.


What we do *NOT* want to do
===========================

There are also some directions we do NOT intend to focus on in the scope of Rain.

Visual editor
-------------

We do not plan to support visual creation and editing of pipelines. The scale of
reasonably editable workflows is usually very small. We focus on clean and easy
client APIs and great visualization.

User isolation and task sandboxing
----------------------------------

We do not plan to limit malicious users or tasks from doing any harm. Use
existing tools for task isolation. The system is lightweight enough to have one
instance per user if necessary.

Fair user scheduling, accounting and quotas
-------------------------------------------

When running multiple sessions, there is no intention to fairly schedule or
prioritize them. The objective is only overally efficient resource usage.


Comparison with similar tools
=============================

TODO

Roadmap
=======

v0.2
----

* Worker/Subworker crash resilience
* More clever scheduler
* Directories as blobs
