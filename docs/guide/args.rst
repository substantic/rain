
Program *rain*
**************

Program *rain* contains core infrastructure of rain: server and worker. It also
known how to start infrastructure for common usages.

Examples
========

Starting local workers
----------------------

The most simple case is running starting server and one worker with all
resources of the local machine. The following command do all work for you::

  $ rain run --simple


If you want to start more local workers, you can use the following command.
It starts two workers with 4 assigned cpus and one with 2 assigned cpus::

  $ rain run --local-wokers=[4,4,2]


Starting remote workers
-----------------------

If you have more machines that are reachable via SSH you can use the following
command. We assume that file ``my_hosts`` contains addresses of hosts, one per
line::

  $ rain run --worker-host-file=my_hosts

Let us note, that current version assumes that assumes for each host that Rain
is placed in the same directory as on machine from which command is invoked.

If you are running Rain inside PBS scheduler (probably if you are using an HPC
machine), then you can simple run::

  $ rain run --autoconf=pbs

It executes worker on each node allocated by PBS scheduler.


Starting worker manually
------------------------

If you need a special setup that is not covered by ``rain run`` you can
simply start server and workers manually::

  $ rain server                    # Start server
  $ rain worker <SERVER-ADDRESS>   # Start worker


Arguments
=========

Synopsis
--------

::

  rain run [--simple | --autoconf=CONF | --local-workers | --worker-host-file=FILE]
           [--listen=LISTEN_ADDRESS] [--logdir=DIR] [--workdir=DIR]
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

**--simple**
  Starts server and one local worker that gains all resources of the local
  machine.

**--autoconf=CONF**
  Automatic configuration from the environment. Possible options are:

  - *pbs* - If executed in an PBS job, it starts server on current node and
    worker on each node.

**--local-workers=RESOURCES**
  Start local with a given number of cpus. E.g. --local-workers=[4,4,2]
  starts three workers: two with 4 cpus and one with 2 cpus.

**--worker-host-file=FILE**
  Starts local server and remote workers. FILE should be file containing
  name of hosts, one per line.

  The current version assumes the following of each host:

  * SSH server is running.
  * Rain is installed in the same directory as on the machine
    from which that ``rain run`` is executed.

**--listen=(PORT|ADDRESS|ADDRESS:PORT)**
  Set listening address of server. Default is 0.0.0.0:7210.

**--logdir=DIR**
  The option is unchanged propagated into the server and workers.

**--workdir=DIR**
  The option is unchanged propagated into workers.


Command: server
---------------

Runs Rain server.

**--listen=(PORT|ADDRESS|ADDRESS:PORT)**
  Set listening address of server. Default is 0.0.0.0:7210.

**--logdir=DIR**
  Set logging directory of server. The program creates directory
  ``<DIR>/rain/server-<HOSTNAME>-<PID>/logs`` where logs of server are stored.

**--ready-file=FILE**
  Create file containing a single line "ready", when the server is fully initialized
  and ready to accept connections.


Command: worker
---------------

Runs Rain worker.

**SERVER_ADDRESS[:PORT]**
  An address where a server listens. If the port is omitted than port 7210 is
  used.

**--cpus=N**
  Set a number of cpus available to the worker.

  * If not specified then the all cores in the machine is used.
  * If a positive number is used then value is used as the number of available
    cpus.
  * If a negative number -X is used then X subtracted from all available cores,
    and resulting number is used as the number of available cpus.

**--listen=(PORT|ADDRESS|ADDRESS:PORT)**
  Set listening address of worker for worker-to-worker connections. When port is
  0 then a open random port is assigned. The default is 0.0.0.0:0.

**--logdir=DIR**
  Set the logging directory for the worker. The program creates directory
  ``<DIR>/rain/worker-<HOSTNAME>-<PID>/logs`` where logs of server are stored.

**--workdir=DIR**
  Set the working directory where the worker stores intermediate results.
  The program creates directory ``<DIR>/rain/worker-<HOSTNAME>-<PID>/work``
  where logs of server are stored.

  .. warning::
     Rain assumes that working directory is placed on a fast device (ideally
     ramdisk). Avoid placing workdir on a network file system.

**--ready-file=FILE**
  Creates the file containing a single line "ready", when the worker is
  connected to server and ready to accept worker-to-worker connections.
