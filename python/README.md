# Rain Python API

A python API for [Rain](https://github.com/substantic/rain) computation framework.

## Documentation

* [Overview at RtD](https://substantic.github.io/rain/docs/overview.html)
* [Python API docs](https://substantic.github.io/rain/docs/python_api.html)

## Installation

Install from [PyPI](https://pypi.org/project/rain-python/):

```shell
pip install rain-python
```

Or locally (building of Rust or C++ binaries not needed):

```shell
git clone https://github.com/substantic/rain
cd rain/python
python3 setup.py install
```

To run the python tests, you need to build binaries of the Rust and C++ code first, see our [testing Dockerfile](https://github.com/substantic/rain/blob/master/Dockerfile). You may need to get a recent version of [rust](https://www.rust-lang.org/) from [rustup](https://rustup.rs/).

