Installation, Running & Deployment
**********************************


Rain Distributed Execution Framework consists is an all-in-one binary.
Rain API is a pure Python package with a set of dependencies installable via pip.


Binaries
========

Rain provides a binary distribution for Linux/x64. The binary is almost fully
statically linked. The only dynamic dependancies are libc and sqlite3 (for logging
purpose).

Latest release can be found at https://github.com/substantic/rain/releases.
It can be downloaded and unpacked as follows:

::

   $ wget https://github.com/substantic/rain/releases/download/v0.4.0/rain-v0.4.0-linux-x64.tar.xz
   $ tar xvf rain-v0.4.0-linux-x64.tar.xz

Installation of Python API::

  $ pip3 install rain-python


Build via cargo
===============

If you have installed Rust toolchain, you can use cargo to build
Rust binaries and skip manual download::

   $ cargo install rain_server

Note that you still have to install Python API through pip::

  $ pip3 install rain-python


Build from sources
==================

For building from sources, you need Rust and SQLite3 (for logging) and Capnp
compiler (for compiling protocol files) installed on your system.

::

  # Example for installation of dependencies on Ubuntu

  # Installation of latest Rust
  $ curl https://sh.rustup.rs -sSf | sh

  # Other dependencies
  $ sudo apt-get install capnproto libsqlite3-dev

For building Rain, run the following commands::

  $ git clone https://github.com/substantic/rain
  $ cd rain
  $ cargo build --release

After the installation, the final binary can be found ``rain/target/relase/rain``.

Installation of Python API::

  $ cd python
  $ python setup.py install


.. _start-rain:

Starting infrastructure
=======================


Starting local governors
------------------------

The most simple case is running starting server and one governor with all
resources of the local machine. The following command do all work for you::

  $ rain start --simple


If you want to start more local governors, you can use the following command.
It starts two governors with 4 assigned cpus and one with 2 assigned cpus::

  $ rain start --local-wokers=[4,4,2]


Starting remote governors
-------------------------

If you have more machines that are reachable via SSH you can use the following
command. We assume that file ``my_hosts`` contains addresses of hosts, one per
line::

  $ rain start --governor-host-file=my_hosts

Let us note, that current version assumes that assumes for each host that Rain
is placed in the same directory as on machine from which command is invoked.

If you are running Rain inside PBS scheduler (probably if you are using an HPC
machine), then you can simple run::

  $ rain start --autoconf=pbs

It executes governor on each node allocated by PBS scheduler.

.. note::

   We recommended to reserve one CPU for server unless you have long runnig
   tasks. This reservation can be done through cgroups, or CPU pinning.

   Another option (with less isolation) is to use option ``-S``::

     $ rain start -S --governor-host-file=my_hosts

   If a remote machine is actually localhost (and therefore runs Rain server)
   then ``--cpus=-1`` argument is used for the governor on that machine, i.e. the
   governor will consider one cpu less on that machine.


Starting governors manually
---------------------------

If you need a special setup that is not covered by ``rain start`` you can
simply start server and governors manually::

  $ rain server                    # Start server
  $ rain governor <SERVER-ADDRESS>   # Start governor


Arguments for program *rain*
============================


Synopsis
--------

::

  rain start --simple [--listen=ADDRESS] [--http-listen=ADDRESS]
           [-S] [--runprefix=CMD] [--logdir=DIR] [--workdir=DIR]
           [--governor-config=PATH]
  rain start --autoconf=CONF [--listen=ADDRESS] [--http-listen=ADDRESS]
           [-S] [--runprefix=CMD] [--logdir=DIR] [--workdir=DIR]
           [--governor-config=PATH] [--remote-init=COMMANDS]
  rain start --local-governors [--listen=ADDRESS] [--http-listen=ADDRESS]
           [-S] [--runprefix=CMD] [--logdir=DIR] [--workdir=DIR]
           [--governor-config=PATH]
  rain start --governor-host-file=FILE [-S] [--listen=ADDRESS]
           [--http-listen=ADDRESS]
           [-S] [--runprefix=CMD] [--logdir=DIR] [--workdir=DIR]
           [--governor-config=PATH] [--remote-init=COMMANDS]

  rain server [--listen=LISTEN_ADDRESS] [--http-listen=LISTEN_ADDRESS]
              [--logdir=DIR] [--ready-file=<FILE>]
  rain governor [--cpus=N] [--workdir=DIR] [--logdir=DIR]
              [--ready-file=FILE] [--config=PATH] SERVER_ADDRESS[:PORT]
  rain --version | -v
  rain --help | -h


Command: start
--------------

Starts Rain infrastructure (server & governors), makes sure that everything is
ready and terminates.

**--simple**
  Starts server and one local governor that gains all resources of the local
  machine.

**--autoconf=CONF**
  Automatic configuration from the environment. Possible options are:

  - *pbs* - If executed in an PBS job, it starts server on current node and
    governor on each node.

**--local-governors=RESOURCES**
  Start local with a given number of cpus. E.g. --local-governors=[4,4,2]
  starts three governors: two with 4 cpus and one with 2 cpus.

**--governor-config=PATH**
  Path to governor config. It is passed as --config argument for all governors.

**--governor-host-file=FILE**
  Starts local server and remote governors. FILE should be file containing
  name of hosts, one per line.

  The current version assumes the following of each host:

  * SSH server is running.
  * Rain is installed in the same directory as on the machine
    from which that ``rain start`` is executed.

**-S**
  Serves for reserving a CPU on server node. If remote governor
  detects that it is running on the same machine as server then it
  is executed with ``--cpus=-1``.

  The detection is based on checking if the server PID exists on the remote
  machine and program name is "rain".

**--listen=(PORT|ADDRESS|ADDRESS:PORT)**
  Set listening address of server. Default is 0.0.0.0:7210.

**--http-listen=(PORT|ADDRESS|ADDRESS:PORT)**
  Set listening address of server for HTTP (dashboard). Default is 0.0.0.0:7222.

**--runprefix**
  Set a command before rain programs. It is designed to used to run
  analytical tools (e.g. --runprefix="valgrind --tool=callgrind")

**--logdir=DIR**
  The option is unchanged propagated into the server and governors.

**--workdir=DIR**
  The option is unchanged propagated into governors.

**--remote-init=COMMAND**
  Commands executed on each remote connection. For example:
  ``--remote-init="export PATH=$PATH:/path/bin"``.


Command: server
---------------

Runs Rain server.

**--listen=(PORT|ADDRESS|ADDRESS:PORT)**
  Set listening address of server. Default is 0.0.0.0:7210.

**--logdir=DIR**
  Set logging directory of server. Default is /tmp/rain/logs/server-<HOSTNAME>-PID.

**--ready-file=FILE**
  Create file containing a single line "ready", when the server is fully initialized
  and ready to accept connections.


Command: governor
-----------------

Runs Rain governor.

**SERVER_ADDRESS[:PORT]**
  An address where a server listens. If the port is omitted than port 7210 is
  used.

**--config=PATH**
  Set a path for a governor config.

**--cpus=N**
  Set a number of cpus available to the governor (default: 'detect')

  * If 'detect' is used then the all cores in the machine is used.
  * If a positive number is used then value is used as the number of available
    cpus.
  * If a negative number -X is used then the number of cores is detected and X
    is subtracted from this number, the resulting number is used as the number
    of available cpus.

**--listen=(PORT|ADDRESS|ADDRESS:PORT)**
  Set listening address of governor for governor-to-governor connections. When port is
  0 then a open random port is assigned. The default is 0.0.0.0:0.

**--logdir=DIR**
  Set the logging directory for the governor. Default is
  ``/tmp/rain/logs/governor-<HOSTNAME>-<PID>/logs``.

**--ready-file=FILE**
  Creates the file containing a single line "ready", when the governor is
  connected to server and ready to accept governor-to-governor connections.

**--workdir=DIR**
  Set the working directory where the governor stores intermediate results.
  The defautl is ``/tmp/rain/work/governor-<HOSTNAME>-<PID>``

  .. warning::
     Rain assumes that working directory is placed on a fast device (ideally
     ramdisk). Avoid placing workdir on a network file system.
