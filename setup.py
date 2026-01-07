from setuptools import setup

setup(name='greendb',
      packages=[],
      py_modules=['greendb', 'greenquery'],
      install_requires=['lmdb', 'gevent', 'msgpack-python'])
