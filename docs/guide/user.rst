
User's Guide
************


Basic terms
===========

**Task** is a basic unit of work in Rain, it reads inputs and produces outputs.
Tasks are executed on computational nodes (computers where Rain governors are
running). Tasks can be external programs, python functions, and basic built-in
operations.

**Data objects** are objects that are read and created by tasks. Data objects
are immutable, once they are created they cannot be modified. They are generic
data blobs or directories with accompanying metadata. It is upto tasks to
interpret the data object content.


Task definition and submission
==============================

Rain represents your computation as a graph of tasks and data objects. Tasks are
not eagerly executed during the graph construction. Instead, the actual
execution is managed by Rain infrastructure after an explicit submission. This
leads to a programming model in which you first only **define** a graph and then
**execute** it.

Let us consider the following example, where two constant objects are created
and merged together::

  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)  # Create a connection to the server
                                      # running at localhost:7210

  with client.new_session() as session:  # Creates a session

      a = blob("Hello ")    # Create a definition of data object in the current session
      b = blob("world!")    # Create a definition of data object in the current session
      tasks.Concat([a, b])  # Create a task definition in the current session
                            # that concatenates input data objects

      session.submit()      # Send the created graph into the server, where the computation
                            # is performed.
      session.wait_all()    # Wait until all submitted tasks are completed

The graph composed in the session looks as follows:

.. figure:: imgs/helloworld.svg
   :alt: Example of task graph

When the graph is constructed, all created objects and tasks are put into the
active session. In many cases, it is sufficient just to create one session for
whole program lifetime, with one submit at the end. However, it is possible to
create more sessions or built a graph gradually with more submits. More details
are covered in Section :ref:`sessions`.


Fetching data objects
=====================

Data objects produced by tasks are not transferred back to the client
automatically. If needed, this can be done using the ``fetch()`` method. It
returns :class:`rain.common.DataInstance` that wraps data together with some
additional information. To get raw bytes from :class:`rain.common.DataInstance`
you can call method ``get_bytes()``.

In the following example, we download the result back to the Python client
code. Expression ``t.output`` refers to the data object that is the output
of task ``t``::


  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)

  with client.new_session() as session:
      a = blob("Hello ")
      b = blob("world!")
      t = tasks.Concat((a, b))
      t.output.keep()            # Tell server to keep result of task

      session.submit()           # Submit task graph

      result = t.output.fetch()  # Download result from the server
      print(result.get_bytes())  # Prints b'Hello world!'


By default, Rain automatically removes data objects that are no longer needed
for further computation. Method ``keep()`` sets a flag to a given data object
that instructs the server to keep the object until the client does not
explicitly frees it. An object can be freed when the session is closed or when
``unkeep()`` method is called. Method ``keep()`` may be called only before the
submit. Method ``unkeep()`` may be called on any "kept" object any time.

If method ``fetch()`` is called and the object has not been finished yet, the
method blocks until the object is not finished. Note that this is the reason,
why we did not use ``wait_all()`` in this example.


Inter-task dependencies
=======================

Naturally, an output of a task may be used as an input for another task. This
is demonstrated by the following example. In the example, we use
``tasks.Sleep(O, T)`` that creates a task taking an arbitrary data object ``O``
and waits for ``T`` seconds and then returns ``O`` as its output. Being aware
that such task is not very useful in practice, we find it useful as an
intuitive example to demostrate the concept of task chaining::

  from rain.client import Client, tasks, blob

  client = Client("localhost", 7210)

  with client.new_session() as session:
      a = blob("Hello ")
      b = blob("world!")
      t1 = tasks.Sleep(b, 1.0)   # Wait for one second and then returns 'b'
      t2 = tasks.Concat((a, t1.output))
      t2.output.keep()

      session.submit()           # Submit task graph

      result = t2.output.fetch()  #  It will wait around 1 second
                                  #  and then returns b'Hello world'

If a task produces only a single output, we can ommit ``.output`` and directly
use the task as an input for another task. In our example, we can define ``t2``
as follows::

  t2 = tasks.Concat((a, t1))

This shortened notation is used in the rest of the text.


More outputs
============

A task may generally create zero, one, or more outputs. All outputs are
accessible via attribute ``outputs``. That contains an instance of
:class:`rain.common.LabeledList`. It is an extension of a standard list
(indexed from zero), that also allows to be accessed via string labels.

::

   # The following task creates two outputs labeled "output1" and "output2" with
   # an equivalent of 'cat data | tee output1 > output2'.
   t = tasks.Execute(["tee", Output("output1")], stdout="output2", stdin=data)

   t.outputs["output1"]  # Access to output "output1"
   t.outputs["output2"]  # Access to output "output2"

   # There is also some helper functions:
   # Keep all outputs (equivalent to: for o in t.outputs: o.keep())
   t.keep_outputs()

   # After submit
   # Fetch all outputs (equivalent to: [o.fetch() for o in t.outputs])
   t.fetch_outputs()

If a task has more than one output or zero outputs, then accessing attribute
``.output`` throws an exception. Attribute ``.outputs`` is always availble
independently on the number of outputs.


Object data types
=================

Every data object represents either a single binary data blob or a directory.
Since these two object *data types* behave very differently, they are
distinguished and checked already when constructing the computation graph.
The *data type* may be one of:

* 'blob' - Binary data block. May have a :ref:`content type` specified.
* 'dir' - Directory structure, see section :ref:`directories`.

We consider developing other data object "modes", e.g. streams.


.. _`content type`:

Object content types
====================

Binary data objecs represent different type of data in different formats.
The Rain infrastructure treats all data objects as raw binary blobs,
and it is up to tasks to interpret them. Content type is a string identifier
of the format of the data in tasks and clients. Python code also recognize
some of content types and allows to deserialize them directly.

Currently recognized content types are:

  * '' - Raw binary data, unknown or unspecified content type
  * 'pickle' - Serialized Python object
  * 'cloudpickle' - Serialized Python object via Cloudpickle
  * 'json' - Object serialized into JSON
  * 'cbor' - Object serialized into CBOR
  * 'arrow' - Object serialized with Apache Arrow
  * 'text' - UTF-8 string.
  * 'text-<ENCODING>' - Text with specified encoding
  * 'mime/<MIME>' - Content type defined as MIME type
  * 'user/<TYPE>' - User defined type, <TYPE> may be arbitrary string

An object may have two different content-types: First, a type is specified
when constructing the task graph. Second, the type may be set by the task
executor dynamically (e.g. depending on some input data).
If present, the latter is taken to be the actual content type and must
be a sub-type of the former.
Any type is considered a subtype of the unspecified type.


Constant data objects
=====================

Function :func:`rain.client.blob` serves for a creation of a constant data
object. The content of the data object is uploaded to the server together with
the task graph.

::

   from rain.client import blob, pickled
   blob(b"Raw data")  # Creates a data object with a defined content
   blob(b"Raw data", label="input data")  # Data with a non-default label
                                          # (Default label is 'const')
   blob("String data")  # Creates a data object from a string, the content type
                        # is set to 'text'
   blob("[1, 2, 3, 4]", content_type="json")  # Data with a specified content type
   blob([1, 2, 3, 4], encode="json")  # Serialize python object to JSON and set
                                      # content type to "json"
   blob([1, 2, 3, 4], encode="pickle")  # Serialize python object by pickle
                                        # content type to "pickle"
   pickled([1, 2, 3, 4])  # Short-cut for blob(..., encode="pickle")


Built-in tasks
==============

The following tasks are supported directly by the Rain governor:

*Concat* (:class:`rain.client.tasks.Concat`)
  Concatencates inputs into one resulting blob.

*Load*, *LoadDir* (:class:`rain.client.tasks.Load`, :class:`rain.client.tasks.LoadDir`)
  Creates data object from an external file or direftory.
  (Note: The current version does not support tracking external resources;
  therefore, this operation "internalizes" the file, i.e. it makes a copy
  of it into the working directory.)

*Store* (:class:`rain.client.tasks.Store`)
  Saves data object to a filesystem.
  The data are saved into local file system of the governor on which the
  task is executed. This task is usually used for saving files to
  a distributed file system, hence it does not matter which governor
  performs the task.

*Sleep* (:class:`rain.client.tasks.Sleep`)
  Task that forwards its input as its
  output after a specified delay. Mostly for testing and benchmarking.

*Execute* (:class:`rain.client.tasks.SliceDirectory`)
  Run an external program with given inputs, parameters and resources.
  See :class:`rain.client.Program` if you execute a program repeatedly
  with different data.

*MakeDirectory* (:class:`rain.client.tasks.MakeDirectory`)
  Tasks that creates a directory combining the inputs under given paths.

*SliceDirectory* (:class:`rain.client.tasks.SliceDirectory`)
  Tasks that extracts a file or subdirectory from a directory object.

::

  # This example demonstrates usage of four built-in tasks
  from rain.client import tasks, Client, blob

  client = Client("localhost", 7210)

  with client.new_session() as session:

      # Create tasks opening an external file
      data1 = tasks.Load("/path/to/data")

      # Create a constant object
      data2 = blob("constant data")

      # Merge two objects
      merge = tasks.Concat((data1, data2))

      # Sleep for 1s
      result = tasks.Sleep(merge, 1.0)

      # Write result into file
      tasks.Store(result, "/path/to/result")

      session.submit()
      session.wait_all()

(Examples for the directory-related tasks are in section :ref:`directories`)


Running external programs
=========================


Task ``tasks.Execute``
----------------------

The whole functionality is built around built-in task
:class:`rain.client.tasks.Execute`. When a program is executed through
:class:`rain.client.tasks.Execute`, then a new temporary directory is created.
This directory will be removed at the end of program execution. The current
working directory of the program is set to this directory.

The idea is that this directory is program's sandbox where input data objects
are mapped and files created in this directory may be moved out as new data
objects when computation completes. Therefore, in contrast with many other
workflow systems, programs in rain should not be called with absolute paths in
arguments but use relative paths (to stay in its working directory).
Governors try to avoid unnecessary data object replication in the cases when
a data object is used by multiple tasks that run on the same governor.

If the executed program terminates with a non-zero code, then tasks fails and
content of standard error output is written into the error message.

The simple example looks as follow::

  tasks.Execute("sleep 1")

This creates a task with no inputs and no outputs executing program "sleep"
with argument "1". Arguments are parsed in shell-like manner.
Arguments can be also specified explicitly as a list::

  tasks.Execute(("sleep",  "1"))

Command may be also interpreted by shell, if the argument ``shell=True`` is
provided::

  tasks.Execute("sleep 1 && sleep 1", shell=True)


Outputs
-------

Files created during task execution or task standard output can be used as the
output of :class:`rain.client.tasks.Execute`. The following example calls program
``wget`` that downloads web page at https://github.com/ and saves it as
`index.html`. The created file is forwarded as the output of the task.

::

  from rain.client import Client, task, Output

  client = Client("localhost", 7210)

  with client.new_session() as session:
      t = tasks.Execute("wget https://github.com/",
                         output_paths=[Output("index", path="index.html")])
      t.output.keep()

      session.submit()
      result = t.output.fetch().get_bytes()

The class :class:`rain.client.Output` allows to configure the outputs.
The first argument is the label of the output. The argument ``path`` sets the
path to the file used as output.
It is a relative path w.r.t. the working directory of the
task. If the path is not defined, then label is used as path; e.g.
``Output("my_output")`` is equivalent to ``Output("my_output",
path="my_output")``. The Output instance can be also used for specification of
additional attributes such content type or size hint. Please see the class
documentation for more details.

If we do not want to configure the output, it is possible to use just a string
instead of instance of ``Output``. It creates the output with the same label
and path as the given string.
Therefore we can create the previous task as follows::

  t = tasks.Execute("wget https://github.com/", output_paths=["index.html"])

The only difference is that label of the output is now "index.html" (not
"index").

Of course, more than one output may be specified. Program ``wget`` allows to
redirect its log to a file through ``--output-file`` option::

  t = tasks.Execute("wget https://github.com/ --output-file log",
                    outputs_paths=["index.html", "log"])

This creates a task with two outputs with labels "index.html" and "log".
The outputs are available using standard syntax, e.g. ``t.outputs["log"]``.

Outputs can be also passed directly as program arguments.
This is a shortcut for two actions: passing the output path as an argument
and putting output into ``output_paths``.
The example above can be written as follows::

  t = tasks.Execute(["wget", "https://github.com/", "--output-file", Output("log")],
                    output_paths=["index.html"])

The argument ``stdout`` allows to use program's standard output::

   # Creates output from stdout labelled "stdout"
   tasks.Execute("ls /", stdout=True)

   # Creates output from stdout with label "my_label"
   tasks.Execute("ls /", stdout="my_label")

   # Creates output through Output object, argument 'path' is not allowed
   tasks.Execute("ls /", stdout=Output("my_label"))


Inputs
------

Data objects can be mapped into the working directory of
:func:`rain.client.tasks`. The simplest case is to use a data object directly
as arguments for a program. In such case, the data object is mapped into
randomly named file and the name is placed into program arguments.
Note that files are by default mapped only for reading (and proctected by
setting file permissions). More options of mapping is described in
:ref:`fs_mappings`.

::

  from rain.client import Client, task, blob

  client = Client("localhost", 7210)

  with client.new_session() as session:
      data = blob(b"It is\nrainy day\n")

      # Maps 'data' into file XXX where is a random name and executes
      # "grep rain XXX"
      task = tasks.Execute(["grep", "rain", data], stdout=True)
      task.output.keep()

      session.submit()
      print(task.output.fetch().get_bytes())  # Prints b"rainy day"

For additional settings and file name control, there is
:class:`rain.client.Input`, that is a counter-part for
:class:`rain.client.Output`. It can be used as follows::

    from rain.client import Client, task, Input

    ...

    # It executes a program "a-program" with arguments "argument1" and "myfile"
    # and while it maps dataobject in variable 'data' into file 'myfile'
    my_data = ... # A data object
    task = tasks.Execute(["a-program", "argument1",
                          Input("my_label", path="myfile", dataobj=my_data)])

The argument ``input_paths`` of :class:`rain.client.tasks.Execute` serves to map
a data object into file without putting its filename into the program
arguments::

  # It executes a program "a-program" with arguments "argument1"
  # and while it maps dataobject in variable 'data' into file 'myfile'
  tasks.Execute(["a-program", "argument1"],
                input_paths=[Input("my_label", path="myfile", dataobj=my_data)])

The argument ``stdin`` serves to map a data object on the standard input of the
program::

  # Executes a program "a-program" with argument "argument1" while mapping
  # a data object on the standard input
  tasks.Execute(["a-program", "argument1"], stdin=my_data)


Factory ``Program``
-------------------

In many cases, we need to call the same program with the same argument set.
Class :class:`rain.client.Program` serves as a factory for
:class:`rain.client.tasks.Execute` for this purpose. An instance of ``Program``
can be called as a function that takes data objects; the call creates a task in
the active session.

::

  from rain.client import Client, blob, Program, Input

  rain_grep = Program(["grep", "rain", Input("my_input", path="my_file")], stdout=True)

  client = Client("localhost", 7210)

  with client.new_session() as session:
      data = blob(b"It is\nrainy day\n")

      # Creates a task that executes "grep rain my_file" where dataobject in variable
      # 'data' is mapped into <FILE>
      task = rain_grep(my_input=data)

``Program`` accepts the same arguments as ``execute``, including
``input_paths``, ``output_paths``, ``stdin``, and ``stdout``. The only
difference is that in all places where data object could be used, ``Input``
instance (without ``dataobj`` argument) has to be used, since ``Program``
defines "pattern" indepedently on a particular session.


Python tasks
============

In addition to built-in tasks, Rain allows to run additional types of tasks via
executors. Rain is shipped with Python executor, that allows to execute
arbitrary Python code.

Decorator ``@remote``
---------------------

Decorator :func:`rain.client.remote` turns a python function into a
Rain task. Let us consider the following example::

  from rain.client import Client, remote

  @remote()
  def hello(ctx):
      return "Hello world!"

  client = Client("localhost", 7210)

  with client.new_session() as session:
      t = hello()                # Create a task
      t.output.keep()
      session.submit()
      result = t.output.fetch()
      print(result)              # Prints b'Hello world!'

The decorator changes the behavior of the decorated function in a way that
calling it no longer executes it in the client but creates a task that executes
the function in a python executor. Governor starts and manages executors as
necessary, there is no need of any action from the user.

The decorated function should accept at least one argument. As the first
argument, the context of the execution is passed to the function. Context
enables some actions within the task. It is a convention to name this argument
as ``ctx``.


Inputs
------

Decorated function may take more parameters than ``ctx``; these parameters
define inputs of the task. By default, they can be arbitrary Python objects and
they are serialized via ``cloudpickle``. If the decorated function is called
with a data object, it is invokend with :class:`rain.common.DataInstance` that
contains data defined by the object::

  from rain.client import Client, remote, blob

  @remote()
  def hello(ctx, data1, data2):
      return data1 + data2.get_bytes()

  client = Client("localhost", 7210)
  with client.new_session() as s:

      # Create data object
      data = blob("Rain!")

      # Creates a task calling function 'hello' in governor
      t = hello(b"Hello ", data)

      t.output.keep()
      s.submit()
      s.wait_all()

      # Prints b'Hello Rain!"
      print(t.output.fetch().get_bytes())

In remotely executed Python code, Rain data objects are replaced with actual
data instances. All occurences of data objects are replaced, even those
encapsulated in own data structures::

  class MyClass:

      def __init__(self, my_data):
          self.my_data = my_data


   @remote()
   def my_call(ctx, input):
       # If we assume a call of this function as below,
       # we obtain an instance of MyClass where attribute 'my_data'
       # is list of instances of DataInstance
       return b""

   ...

   my_instance = MyClass([blob(b"data1"), blob(b"data2"), blob(b"data3")])
   task = my_call(my_instance)

.. note::
   It is possible to pass also generators as arguments to remote functions, and
   it works as expected. However, Rain has to include all data objects occuring
   in related expressions as task dependencies. Therefore, you may create more
   dependencies then expected. To avoid this problems, we recommend to evaluate
   generators before passing to remote functions, especiialy if it is a
   filtering kind of generator.

All metadata of data objects (including content type) are passed to the data
instances occuring in remote functions. Therefore, it is possible to call
method ``load()`` on data instances to deserialize objects according to
their content types::

   @remote()
   def fn1(ctx, data):
       # Load according content type. Throws an error if content type is not provided
       loaded_data = data.load()
       ...

   # Automatically call load() on specific argument
   @remote(inputs={"data": Input(load=True)})
   def fn2(ctx, data):
       ....

   # Automatically call load() on all arguments
   @remote(auto_load=True)
   def fn3(ctx, data):
       ....

   # Example of calling:
   data = blob([1,2,3,4], encode="json")
   fn1(data)

The second case uses :class:`rain.client.Input` to configure individual
parameters. It can be also used for additional configurations, like data-object
size hints for Rain scheduler, or content type specification::

   # The following function asks for a dataobject with content type "json" as
   # its argument. If the function is called the following happens:
   # 1) If the input dataobject has content type "json", it is passed as it is
   # 2) If the input dataobject has no content type (None), then content type "json"
        is set as the object content type
   # 3) If the input dataobject has content type different from "json", the task fails

   @remote(inputs={"data": Input(content_type="json")})
   def fn1(ctx, data):
       pass


Outputs
-------

By default, it is expected that a remote function returns one data object. It
may return an instance of ``bytes`` or ``str`` that will be used as content of
the resulting data object. If an instance of bytes is returned then the content
type of resulting object is ``None``, if a string is returned then the content
type is set to "text". A remote function may also return a data instance, when
you want to set additional attributes of data object. More outputs may be
configured via ``outputs`` attribute of remote::

    @remote()
    def fn1(ctx):
        return b"Returning bytes"

    @remote()
    def fn2(ctx):
        return "Returning string"

    # Configuring more unlabaled outputs
    @remote(outputs=3)
    def fn3(ctx):
        (b"data1", b"data2", b"data3")

    # No output
    @remote(outputs=0)
    def fn4(ctx):
         pass

    # Configuring labeled outputs
    @remote(outputs=("label1", "label2"))
    def fn5(ctx):
         return {"label1": b"data1", "label2": b"data2"}

    # Set content types of resulting objects
    @remote(outputs=(Output(content_type="json"), Output(content_type="json"))
    def fn6(ctx):
        return ("[1, 2, 3]", "{'x': 123}")

    # Automatically encode resulting objects
    @remote(outputs=(Output(encode="pickle"), Output(encode="json"))
    def fn7(ctx):
        return ([1, 2, 3], {"x": 123})


Debug stream
------------

Method ``debug`` on the context allows to write messages into debug stream that
can be found in task attribute "debug" and it is also part of an error message
when the task fails.

::

    @remote()
    def remote_fn(ctx):
        a = 11
        b = a + 10
        ctx.debug("Variable a = {}", a)
        ctx.debug("Variable b = {}", b)
        raise Exception("Error occured!")

    # When this task is executed, you get the following error message:
    #
    # Exception: Error occured!
    #
    # Debug:
    # Variable a = 11
    # Variable b = 21


Type hints
----------

If you are using sufficiently new Python (>=3.5), you can use type hints
to define outputs and inputs, e.g.::

    @remote
    def test1(ctx, a : Input(content_type="json")) -> Output(encode='pickle', label='test_pickle');
        pass


Resources
=========

In the current version, the only resource that can be configured is the number
of cpus. This following example shows how to request a a specific number of
cpus for a task::

  # Reserve 4 CPUs for execution of a program
  tasks.Execute("a-parallel-program", cpus=4)

  # Resere 4 CPUs for a Python task
  @remote(cpus=4)
  def myfunction(ctx):
      pass


Attributes 'spec' and 'info'
============================

Most of the information about the tasks and data objects falls into
two categories:

* The user-created specification data (*spec*).
* The information about the task execution and object computation (*info*).

These are stored and transmitted separately. Once the objects and tasks
are submitted, the spec is immutable. The info is initially empty
and is set by the governor (and in part by the task executor). When
a task or object is finished, info is also immutable.

The data is transmitted as JSON, attributes with values ``None``,
empty strings and empty lists may be omitted when encoding.

A client may ask for info attributes of any task/object as long as session
is open; "keep" flag is not necessary. Attributes are not updated
automatically, ``fetch()`` or ``update()`` has to be called to update
attributes.


Error, debugn and user
----------------------

The task info and object info share ``error`` attribute. When non-empty,
the task is assumed to have failed. You may specify ``error``
of an object to indicate the error more precisely, but it usually
indicates a failure of the generating task.
Note that empty ``error`` is assumedto mean success even if explicitly present.

The ``debug`` attribute is intended for any log messages from Rain or the user,
especially for internal and external debugging. General node progress is
normally not logged here as it is contained in the Rain event log.
This is the only attribute that is not immutable once set and may be appended
to.

Both task and object info and spec have a ``user`` dictionary intended
for any JSON-serializable data for any purpose. The keys prefixed with ``_``
are used internally in testing and development.


Task spec and info
------------------

Task spec ( ::`rain.common.attributes.TaskSpec` in Python)
has the following attributes:

* ``id`` - Task ID tuple, type :class:`rain.common.ID`.
* ``task_type`` - The task-type identificator (e.g. "executor/method").
* ``config`` - Any task-type specific configuration data, JSON-serializable.
* ``inputs`` - A list of input object IDs and labels as
  ::`rain.common.attributes.TaskSpecInput`
  * ``id`` - Input object ID.
  * ``label`` - Optional label.
* ``outputs`` - List of output object IDs.
* ``resources`` - Dictionary with resource specification.
* ``user`` - Arbitrary user json-serializable attributes.

Task info (::`rain.common.attributes.TaskInfo` in Python)
has the following attributes:

* ``error`` - Error message. Non-empty error indicates failure.
* ``start_time`` - Time the task was started.
* ``duration`` - Real-time duration in seconds (floating-point number).
* ``governor`` - The ID of the governor that executed this task.
* ``debug`` - Debugging log, usually empty.
* ``user`` - Arbitrary json-serializable objects.


Data object spec and info
-------------------------

Data object spec (::`rain.common.attributes.ObjectSpec` in Python)
has the following attributes:

* ``id`` - Object ID tuple, type :class:`rain.common.ID`.
* ``label`` - Label (role) of this output at the generating task.
* ``content_type`` - Specified content type name, see `content type`_.
* ``data_type`` - Object data type, ``"blob"`` or ``"dir"``.
* ``user`` - Arbitrary user json-serializable attributes.

Data object info (::`rain.common.attributes.ObjectInfo` in Python)
has the following attributes:

* ``error`` - Error message. Non-empty error indicates failure.
* ``size`` - Final size in bytes (approximate for directories).
* ``content_type`` - Content type after execution. Note that this must
  be a sub-type of ``spec.content_type``.
* ``debug`` - Debugging log, usually empty.
* ``user`` - Arbitrary json-serializable objects.


Python API
----------

In the client, the attributes are available as ``spec`` and ``info`` on
:class:`rain.client.Task` and :class:`rain.client.DataObject`.

An example of fetching and querying the attributes at the client::

    with client.new_session() as s:
        task = tasks.Execute("sleep 1")
        s.submit()

        s.wait_all()

        # Download recent attributes
        task.update()

        # Print name of governor where task was executed
        print(task.info.governor)

In the python executor and remote tasks, the object attributes are
available on the input :class:`rain.common.DataInstance`, the
task attributes on the execution context (::`rain.executor.context.Context`).

An example of remote attribute manipulation::

    @remote()
    def attr_demo(ctx):
       # read user defined attributes
       foo = ctx.spec.user["foo"]

       # setup new "user_info" attribute
       ctx.info.user["bar"] = [1, 2, foo]

       # Write some debug log
       ctx.debug("Running at governor", ctx.info.governor)
       return b"Result"

    with client.new_session() as session:
        task = attr_demo()
        task.spec.user["foo"] = 42
        session.submit()
        session.wait_all()
        task.update()

        # prints: [1, 2, 42]
        print(task.info.user["bar"])

        # prints the debug log
        print(task.info.debug)


Waiting for object(s) and task(s)
=================================

Waiting for a completion of a single task/object is done using the ``wait()``
method directly on awaited task or data object. Multiple tasks/objects can be
awaited at once using the ``wait`` method with a set of tasks/obejcts on
session::


  with client.new_session() as session:
      a = blob("Hello world")
      t1 = tasks.Sleep(a, 1.0)
      t2 = tasks.Sleep(a, 2.0)
      session.submit()

      t1.wait()  # This blocks until t1 is finished, independently of t2
      t2.output.wait()  # Waits until a data object is not finished

      # The same as two lines above, but since we are doing it at once, it is
      # slightly more efficient
      session.wait([t1, t2.output])


.. note::

  Note that in the case of ``wait()`` (in contrast with ``fetch()``), object
  does not have to be marked as "kept".


.. _directories:

Directories
===========

Rain allows to use directories in the similar way to blobs. Rain allows to
create directory data objects that can be passed to ``tasks.Execute()``, remote
python code, and other places without any differences. There are only two
specific features:

  - If a directory dataobject is mapped to a file system it is mapped as directory
    (not as a file as in the case of blobs).
  - If a directory is viewed as raw bytes (e.g. method ``get_bytes`` on data
    instance), tar file is returned.

A data type of an object (blob/directory) is a part of the
task graph and has to be determinated during its construction. To specify it in
places where ``Input`` and ``Output`` classes are used, there are classes
:class:`rain.client.InputDir` and :class:`rain.client.OutputDir`.

::

   from rain import

   from rain.client import Client, tasks, blob, OutputDir, directory

   client = Client("localhost", 7210)

   with client.new_session() as session:

       # Creates a directory object from client's local file system
       # Recursively collects all files and directories in /path/to/dir
       d = directory("/path/to/dir")

       # Create blob data objects
       data1 = blob(b"12345")
       data2 = blob(b"67890")

       # Task that creates a directory from data objects
       d2 = tasks.MakeDirectory(tasks.make_directory([
            ("myfile.txt", data1),  # Map 'data1' as file 'myfile.txt' into directory
            ("adir", d),  # Map directory 'd' as subdir named 'adir'
            ("a/deep/path/x", data2),  # Map 'data2' as a file 'x'; all subdirs on path is created
       ])

       # Task taking a file from a directory data object
       d3 = tasks.SliceDirectory(d2, "a/deep/path/x")

       # Task taking a directory from a directory data object
       # This is indicated by  '/' at the end of the path.
       d3 = tasks.SliceDirectory(d2, "a/deep/")

       # Taking directory as outpout of task.execute
       tasks.Execute("git clone https://github.com/substantic/rain",
                     output_paths=[OutputDir("rain")])


.. _fs_mappings:

Mapping data objects onto filesystem
====================================

Rain knows two methods of maping a data objects onto filesystem.

* **write** - creates a fresh copy of data objects is created on filesystem that
  can be freely modified. Changes of the file is *not* propagated back to data
  object.

* **link** - symlink to the internal storage of governor. The user can only read
  this data. This method may silently fall back to 'write' when governor has no file
  system representation of the object.

Task :func:`rain.client.tasks.Execute` maps files by **link** method.
It can be changed by ``write`` argument of ``Input``::

  # Let 'obj' contains a data object

  # THIS IS INVALID! You cannot modified linked objects
  tasks.Execute("echo 'New line' >> myfile", shell=True,
                input_paths=[Input("myfile", dataobj=obj)])

  # This is ok. Writable copy of 'obj' is created.
  tasks.Execute("echo 'New line' >> myfile", shell=True,
                input_paths=[Input("myfile", dataobj=obj, write=True)])

Data instance has methods ``write(path)`` and ``link(path)`` that performs the
mapping to a given path. They can be used on both in executor and in client.
Let us note that in the current version **link** in the client always falls back
to **write**. Example::

  @remote()
  def my_remote_function(ctx, input1):
      input1.write("myfile")  # Writes data into 'myfile' that can be edited
                              # without change of the original object
      input1.link("myfile2")  # Creates a read-only file system representation
                              # of data object


.. warning::

   Read-only property in linking method is forced by setting up file rights.
   Therefore, as far you do not change permissions of files/directories, you are
   proctected against accidental modifications of data objects. If you change
   permissions or content of linked data objects, the behavior is undefined. Let
   us remind that Rain is designed only for execution of trusted codes.
   Obviously this kind of isolation is **not** a protection against malicious
   users.


.. _sessions:

Sessions
========

Overview
--------

The client allows to create one or more sessions. Sessions are the environment
scopes where application create task graphs and submit them into the server.
Sessions follows the following rules:

  * Each client may manage multiple sessions. Tasks and data object in different
    sessions are independent and they may be executed simultaneously.

  * If a client disconnects, all sessions created by the client are terminated,
    i.e. running tasks are stopped and data objects are removed.
    (Persistent sessions are not supported in the current version)

  * If any task in a session fails, the session is labeled as failed, and all
    running tasks in the session are stopped. Any access to tasks/objects in the
    session will throw an exception containing error that caused the problem.
    Destroying the session is the only operation that does not throw the exception.
    Other sessions are not affected.


Active session
--------------

Rain client maintains a global stack of sessions and ``with`` block moves a
session on the top of the stack and removes it from the stack when the block
ends. The session on the top of the stack is called *active* session. The
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

Tasks and data objects are always created within the scope of active session.

.. note::

   Which session is active is always a local information that only influences
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

  The server holds tasks' and objects' metadata (e.g. performance information) as
  long as a session is alive. If you use a long living client with many sessions,
  sessions should be closed as soon as they are not needed.


Multiple submits
----------------

The task graph does not have to be submmited at once; multiple submmits may
occur during the lifetime of a session. Data objects from previous submits
may be used while constructing a new new submit, the only condition is that
they have to be marked as "kept" explicitly.

::

   with client.new_session() as session:
      a = blob("Hello world")
      t1 = tasks.Sleep(a, 1.0)
      t1.output.keep()

      session.submit()  # First submit

      t2 = tasks.Sleep(t1.output, 1.0)

      session.submit()  # Second submit
      session.wait_all()  # Wait until everything is finished

      t3 = tasks.Sleep(t1.output, 1.0)

      session.submit()  # Third submit
      session.wait_all()  # Wait again until everything is finished

Let us remind that method ``wait_all()`` waits until all currently running task
are finished, regardless in which submit they arrived to the server.
