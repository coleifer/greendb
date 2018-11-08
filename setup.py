import os
from setuptools import setup


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as fh:
    readme = fh.read()


setup(
    name='clowndb',
    version=__import__('clowndb').__version__,
    description='clowndb',
    long_description=readme,
    author='Charles Leifer',
    author_email='coleifer@gmail.com',
    url='http://github.com/coleifer/clowndb/',
    packages=[],
    py_modules=['clowndb'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    scripts=['clowndb.py'],
    test_suite='tests')
