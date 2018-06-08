from setuptools import setup, find_packages
import datetime
import os

PYTHON_DIR = os.path.dirname(os.path.abspath(__file__))
RAIN_DIR = os.path.dirname(PYTHON_DIR)


def load_cargo_version():
    with open(os.path.abspath(os.path.join(RAIN_DIR, "Cargo.toml"))) as f:
        import re
        exp = re.compile('version = "([^"]*)"')
        for line in f:
            m = exp.search(line)
            if m:
                return m.groups()[0]
    raise Exception("Cannot determine version")


def load_version():
    if "RAIN_VERSION" in os.environ and os.environ["RAIN_VERSION"]:
        return os.environ["RAIN_VERSION"]
    else:
        return load_cargo_version()


with open('requirements.txt') as reqs:
    requirements = [line for line in reqs.read().split('\n') if line]

now = datetime.datetime.now()

long_desc = """
This is a Python part of Rain. Rain is distributed computational framework.
See https://github.com/substantic/rain for more details."""

setup(name='rain-python',
      version=load_version(),
      description='Distributed Computational Framework',
      long_description=long_desc,
      url='https://github.com/substantic/rain',
      author='Stanislav Bohm, Vojtech Cima, Tomas Gavenciak',
      author_email='rain@substantic.net',
      license='MIT',
      packages=find_packages(),
      package_data={'rain': ['capnp/*.capnp']},
      install_requires=requirements)
