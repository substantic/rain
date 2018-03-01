[![Gitter](https://badges.gitter.im/substantic/rain.svg)](https://gitter.im/substantic/rain?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

![Rain Logo](docs/imgs/logo.svg?sanitize=true)

# Rain

**Rain** is an open-source distributed computational framework for large-scale
task-based pipelines.

Rain aims to lower the entry barrier to the world of distributed computing and
to do so efficiently and within any scale. Our intention is to develop a light
yet robust distributed framework that features an intuitive Python_ API,
straightforward installation and deployment with insightful monitoring on top.

> This is an early release of Rain that is already usable and quite efficient,
> with a polished Python API, but there is still a lot that can be improved.

> Most importantly, we are looking for external users and collaborators to drive
> our future work, both enthusiasts, from the industry and the scientific
> community. Talk to us online at Gitter or via email and let us know what your
> project needs and use-cases, submit bugs or feature requests at GitHub_ or
> even contribute with pull requests.

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

- **Monitoring** Rain is designed to support both online and postmortem
  monitoring.

# Documentation

* [Documentation](http://rain.readthedocs.io)


# Quick start

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
