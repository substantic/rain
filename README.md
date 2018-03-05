
<div align="center">
<img style="width: 250px;" src="docs/imgs/logo.svg?sanitize=true">
</div>

-----------------

[![Gitter](https://badges.gitter.im/substantic/rain.svg)](https://gitter.im/substantic/rain?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) [![Documentation Status](http://readthedocs.org/projects/rain/badge/?version=latest)](http://rain.readthedocs.io/en/latest/?badge=latest) [![Build Status](https://travis-ci.org/substantic/rain.svg?branch=master)](https://travis-ci.org/substantic/rain)


# Rain

**Rain** is an open-source distributed computational framework for processing
of large-scale task-based pipelines.

Rain aims to lower the entry barrier to the world of distributed computing. Our
intention is to provide a light yet robust distributed framework that features
an intuitive Python API, straightforward installation and deployment with
insightful monitoring on top.

> Despite that this is an early release of Rain, it is a fully functional
> project that can be used out-of-the box. Being aware that there is still
> a lot that can be improved and added, we are looking for external
> users and collaborators to help to move this work forward.
> Talk to us online at Gitter or via email and let us know what your
> projects and use-cases need, submit bugs or feature
> requests at GitHub or even contribute with pull requests.

## Features

- **Dataflow programming.** Computation in Rain is defined as a flow graph of
  tasks. Tasks may be build-in functions, Python code, or an external
  applications, short and light or long-running and heavy. The system is
  designed to integrate any code into a pipeline, respecting its resource
  requirements, and to handle very large task graphs (hundreds thousands tasks).

- **Easy to use.** Rain was designed to be easy to deployed anywhere, ranging
  from a single node deployments to large-scale distributed systems and clouds
  ranging thousands of cores.

- **Rust core, Python API.** Rain is written in Rust_ for safety and efficiency
  and has a high-level Python API to Rain core infrastructure, and even supports
  Python tasks out-of-the-box. Nevertheless, Rain core infrastructure provides
  language-indepedent inteface that does not prevent adding support for other
  languages in the future.

- **Monitoring.** Rain is designed to support both online and postmortem
  monitoring.

  ![Dashboard screencast](docs/imgs/rain-dashboard.gif)

## Documentation

* [Documentation](http://rain.readthedocs.io)

## Quick start

* Download binary

```
   $ wget https://github.com/substantic/rain/releases/download/v0.1.0/rain-v0.1.0-linux-x64.tar.xz
   $ tar xvf rain-v0.1.0-linux-x64.tar.xz
```

* Install Python API

```
  $ pip3 install https://github.com/substantic/rain/releases/download/v0.1.0/rain-0.1.0-py3-none-any.whl
```

* Start server & a single local worker

```
$ ./rain-v0.1.0-linux-x86/rain start --simple
```

* Rain "Hello world" in Python

```python
from rain.client import Client, tasks, blob

client = Client("localhost")

with client.new_session() as session:
    task = tasks.concat(blob("Hello "), blob("world!"))
    task.output.keep()
    session.submit()
    result = task.outout.fetch().get_bytes()
    print(result)
```

[Read the docs](http://rain.readthedocs.io/en/latest/examples.html) for more examples.
