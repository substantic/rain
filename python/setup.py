from setuptools import setup, find_packages

with open('requirements.txt') as reqs:
    requirements = [line for line in reqs.read().split('\n') if line]

setup(name='rain',
      version='0.1',
      description='Distributed Computational Framework',
      url='substantic.net/rain',
      author='Stanislav Bohm, Vojtech Cima, Tomas Gavenciak',
      author_email='rain@substantic.net',
      license='MIT',
      packages=find_packages(),
      data_files=[('capnp', ['../capnp/client.capnp',
                             '../capnp/datastore.capnp',
                             '../capnp/server.capnp',
                             '../capnp/worker.capnp',
                             '../capnp/common.capnp',
                             '../capnp/monitor.capnp',
                             '../capnp/subworker.capnp'])],
      install_requires=requirements)
