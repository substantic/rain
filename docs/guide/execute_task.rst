
Running external programs
*************************

A smooth integration of external programs into Rain pipelines is an import part
of Rain.

Introduction of ``task.execute``
================================

The whole functionality is built around build-in task ``task.execute``. When a
program is executed through ``tasks.execute``, then a new temporary directory is
created. This directory will be removed at the end of program execution. The
current working directory of the program is set to this directory.

The idea is that this directory is program's playground where input data objects
are mapped and files created in this directory may be moved out at the end as
new data objects. Unlike (as in many workflow systems), program should not work
with absolute paths but to use relative path to its working directory. Input
data objects (as files) are served by worker as needed. Worker tries to avoids
unnecessary copying of data objects when computation of following tasks in the
same worker.

If the executed program terminates with a non-zero code, then tasks fails and
standard error output is put into the error message.

The simple example looks as follow::

  tasks.execute("sleep 1")

This creates a task with no inputs and no outputs executing program "sleep" with
argument "1". Arguments are parsed in shell-like manner.
Arguments can be also explicitly as a list::

  tasks.execute(("sleep",  "1"))

Command may be also interpreted by shell, if the argument ``shell=True`` is
provided::

  tasks.execute("sleep 1 && sleep 1", shell=True)


Outputs
=======

Created files and standard output can be used as output of :func:``task.execute``.


Argument ``outputs``
--------------------

The following example calls program ``wget`` that downloads web page at
https://github.com/ at saves it as `index.html`. The created file is used as
output of the task.

::

  from rain.client import Client, task, Output

  client = Client("localhost", 7210)

  with client.new_session() as session:
      t = tasks.execute("wget https://github.com/",
                         outputs=[Output("index", path="index.html")])
      t.output.keep()

      session.submit()
      result = t.output.fetch()

The class :class:`rain.client.Output` serves for configuring output. The first
argument is the label of the output. The argument ``path`` sets the path to
output file. It is a relative path w.r.t. the working directory of task. If the
path is not defined, then label is used as path; e.g. ``Output("my_output")`` is
equivalent to ``Output("my_output", path="my_output")``. The Output instance may
also serve for additional attributes, like its content type or size hint. Please
see the class documentation for more details.

If we do not want to configure the output, it is possible to use just string
instead of instance of ``Output``. It creates the output with the same label and
path as the given string. Therefore we can create the execute task as follows::

  t = tasks.execute("wget https://github.com/", outputs=["index.html"])

The only difference is that label of the output is now "index.html" (not
"index").

Of course, more than one output may be specified. Program ``wget`` allows
redirect its log to a file through ``--output-file`` option::

  t = tasks.execute("wget https://github.com/ --output-file log",
                    outputs=["index.html", "log"])

This creates tasks with two outputs with labels "index.html" and "log". Outputs
is available via normal multiple outputs API, e.g. ``t.outputs["log"]``.


Outputs in program arguments
----------------------------

Outputs can be also passed directly as program arguments. This is a shortcut for
passing the output path as an argument. The example above can be written as
follows::

  t = tasks.execute(["wget", "https://github.com/", "--output-file", Output("log")],
                    outputs=["index.html"])


Standard output / error output
------------------------------


Inputs
======


Argument ``inputs``
-------------------


Inputs in program arguments
---------------------------


Standard input
--------------


Factory ``Program``
===================


Bigger example
==============
