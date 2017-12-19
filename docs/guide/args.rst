
Program *rain*
**************

Program *rain* contains core infrastructure of rain: server and worker. It also
known how to start infrastructure for common usages.

Examples
========

Starting local workers
----------------------

TODO

Starting remote workers
-----------------------

TODO

Starting worker manually
------------------------

TODO


Arguments
=========

Synopsis
--------

::

  rain run [--autoconf=CONF] [--listen=LISTEN_ADDRESS]
           [--logdir=DIR] [--workdir=DIR]
  rain server [--listen=LISTEN_ADDRESS] [--logdir=DIR]
              [--ready-file=<FILE>]
  rain worker [--cpus=N] [--workdir=DIR] [--logdir=DIR]
              [--ready-file=FILE] SERVER_ADDRESS[:PORT]
  rain --version | -v
  rain --help | -h
 

Command: run
------------

Starts Rain infrastructure (server & workers), makes sure that everything is
ready and terminates.

**--autoconf=CONF**
  Automatic configuration from the environment. Possible options are:

  - *pbs* - If executed in an PBS job, it starts worker on each node.

**--listen=(<PORT>|<ADDRESS>|<ADDRESS>:<PORT>)**
  Set listening address of server. Default is 0.0.0.0:7210.

**--logdir=<DIR>**
  The option is unchanged propagated into the server and workers.

**--workdir=<DIR>**
  The option is unchanged propagated into workers.


Command: server
---------------

Runs Rain server.

**--listen=(<PORT>|<ADDRESS>|<ADDRESS>:<PORT>)**
  Set listening address of server. Default is 0.0.0.0:7210.

**--logdir=<DIR>**
  Set logging directory of server. The program creates directory
  ``<DIR>/rain/server-<HOSTNAME>-<PID>/logs`` where logs of server are stored.

**--ready-file=<FILE>**
  Create file containing a single line "ready", when the server is fully initialized
  and ready to accept connections.


Command: worker
---------------

Runs Rain worker.

**<SERVER_ADDRESS>[:<PORT>]**
  An address where a server listens. If the port is omitted than port 7210 is
  used.

**--cpus=N**
  Set a number of cpus available to the worker.

  * If not specified then the all cores in the machine is used.
  * If a positive number is used then value is used as the number of available
    cpus.
  * If a negative number -X is used then X subtracted from all available cores,
    and resulting number is used as the number of available cpus.

**--listen=(<PORT>|<ADDRESS>|<ADDRESS>:<PORT>)**
  Set listening address of worker for worker-to-worker connections. When port is
  0 then a open random port is assigned. The default is 0.0.0.0:0.

**--logdir=<DIR>**
  Set the logging directory for the worker. The program creates directory
  ``<DIR>/rain/worker-<HOSTNAME>-<PID>/logs`` where logs of server are stored.

**--workdir=<DIR>**
  Set the working directory where the worker stores intermediate results.
  The program creates directory ``<DIR>/rain/worker-<HOSTNAME>-<PID>/logs``
  where logs of server are stored.

  .. warning::
     Rain assumes that working directory is placed on a fast device (ideally
     ramdisk). Avoid placing workdir on a network file system.

**--ready-file=<FILE>**
  Creates the file containing a single line "ready", when the worker is
  connected to server and ready to accept worker-to-worker connections.
