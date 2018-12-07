import os
from setuptools import setup


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as fh:
    readme = fh.read()

try:
    version = __import__('greendb').__version__
except ImportError:
    version = '0.2.0'


setup(
    name='greendb',
    version=version,
    description='greendb',
    long_description=readme,
    author='Charles Leifer',
    author_email='coleifer@gmail.com',
    url='http://github.com/coleifer/greendb/',
    packages=[],
    py_modules=['greendb'],
    install_requires=['lmdb', 'gevent', 'msgpack-python'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    scripts=['greendb.py'],
    test_suite='tests')
