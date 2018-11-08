import os
from setuptools import setup


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as fh:
    readme = fh.read()


setup(
    name='glmdb',
    version=__import__('glmdb').__version__,
    description='glmdb',
    long_description=readme,
    author='Charles Leifer',
    author_email='coleifer@gmail.com',
    url='http://github.com/coleifer/glmdb/',
    packages=[],
    py_modules=['glmdb'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    scripts=['glmdb.py'],
    test_suite='tests')
