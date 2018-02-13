*****************
Goals and roadmap
*****************

Rain Goals
++++++++++

There are many things to improve, and even more new things to add. To work efficiently, we need to prioritize and for that we need your feedback and use cases. Which features would you like to see and put to good use? What kind of pipelines do you run?

Better dashboard
----------------

Better interactive view on the current and past computation status, including post-mortem analysis. Which stats and views give you the most insight?

Better scheduler
----------------

While surprisingly efficient, the current scheduler is currently mostly based on heuristics and rules. We plan to replace it with an incremental global scheduler based on belief propagation.

Resiliency
----------

The current version supports and propagates some failures (remote python task exceptions, external program errors) but other errors still cause server panic (e.g. worker node failure). The near-term goal is to have better failure modes for introspection and possibly recovery.
The system is designed to allow building resiliency against task or worker failures via checkpoints in the task graph (keeping file copies). It is not clear how useful to our users this would be but it is on our radar.

Resources
---------

Currently, the only resources supported are CPU cores. We are working on also supporting memory requirements, but other resources (GPUs, TPUs, disk space, ...) should be possible with enough work and interest. 

Directory and stream objects support
------------------------------------

Currently, only plain file objects are supported (with optional content type hints). We are working on also supporting arbitrary directories, picking just a subset of files for transport and "lazy" remote access. This will also allow for simple map/array data types for large volumes.
Some tasks work in a streaming fashion and it would be inefficient to wait for their entire output before starting a consumer task. We plan to include streaming data objects but there are semantic and usage issues about resources, scheduling, multiple consumers and resiliency.

Plain C/C++ tasks and subworkers
--------------------------------

Right now, the availabe tasks are either built in, external programs or python routines. It should be possible and straightforward to turn your C or C++ (or other language) function to a custom task by creating a new subworker. We plan a simple C library subworker scaffold that will allow easy gray-box subworkers. You do not have to link agains Rain, which should make deployment easier.

REST client interface
---------------------

The capnp API is a bit heavy-handed for a client API. We plan to create a REST API for the client applications, simplifying API creation in new languages, and to unify it with the dashboard/status query API. External REST apis are convenient for many users and they do not seem to be a performance bottleneck.

Data objects on a shared filesystem
-----------------------------------

In some cases it makes sense to have the intermediate or output files on some shared storage filesystem so we want to also support this. 

Easier Deployment in cloud settings
-----------------------------------

The Rust binary is already one staically linked file and one python-only library, making distribution easy and running on PBS is already supported. We would like to add better support for cloud settings, e.g. AWS and Kubernetes.

Non-goals
+++++++++

Visual editor
-------------

We do not plan to support visual creation and editing of pipelines. The scale of reasonably editable workflows is usually very small. We focus on clean and easy client APIs and great visualisation. 

User isolation and task sandboxing
----------------------------------

We do not plan to limit malicious users or tasks from doing any harm. Use existing tools for task isolation. The system is lightweight enough to have one instance per user if necessary.

Fair user scheduling, accounting and quotas
-------------------------------------------

When running multiple sessions, there is no intention to fairly schedule or prioritize them. The objective is only overally efficient resource usage.

Roadmap
+++++++

v0.2
----

* Worker/Subworker crash resilience
* More clever scheduler
* Directories as blobs


v0.1
----

* Basic functionality - all components are working, basic build-in tasks,
  external programs and Python tasks may be used.
* Simple (but not stupid) scheduler
