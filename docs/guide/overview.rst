Overview
********

**Rain** is a framework for running distributed computations.

   * Rain provides Python interface, where tasks and their interconnections is
     defined.
   * Rain has low-overhead. You can have many (>10^6) short running (<1s) tasks.
   * Rain workers communicates directly; there is no central bottle neck (e.g.
     shared file system) for data transfers.
   * Rain supports smooth integration of 3rd party programs and Python codes
     into pipeline. It avoid unncessary copying of data when external "blackbox"
     programs are used.
   * Rain is focused on easy to use, deploy and run. Our goal is to built a nice
     API, straightforward installation, and straightforward deployment on a
     cluster.


Install and requirements
========================


Installation binaries
---------------------

TODO


Built from sources
------------------

TODO


Hello world
===========

This section shows how to start server and one local worker, and execute simple
"Hello world" program.

- Start infrastructure. You can do start and workers manually, but for standard
  scenarios there is "rain run" to do it for you. The following command stars
  server and one local worker::

  $ rain run --local_worker=1

- Running "Hello World" example. The following program creates a task that joins
  two strings.::

    from rain.client import Client, tasks, blob

    # Connect to server
    client = Client("localhost")  

    # Create a new session
    with client.new_session() as session:  

        # Create task
        task = tasks.concat(blob("Hello "), blob("world!"))

        # Mark that the output should be kept after submit
        task.output.keep()

        # Submit all crated tasks to server
        session.submit()

        # Wait for completion of task and fetch results
        result = task.outout.fetch()

        # Prints 'Hello world!'
        print(result)  



Highlighted features
====================


*Tasks can form any acyclic graph*
::

    TODO


*Integrating external programs*
::

    # Open file
    data = tasks.open("data")

    # Run "grep" with on opened file and use stdout as result
    pruned = tasks.execute(["grep", "something", data], stdout=True)



*Python functions as task*
::

    @remote()
    def my_function(ctx, param_a):
        return param_a.to_str() + " world!"

    with client.new_session as session:

        task = my_function(blob("Hello"))
        task.output.keep()

        session.submit()
        print(task.output.fetch())


*Preserving structres in Python tasks*
::

    TODO


*Resource management*
::

    TODO


*Computaiton can be formed dynamically*
::

    TODO - more submits
