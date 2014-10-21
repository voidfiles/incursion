#!/usr/bin/env python

import os
import sys

import incursion

from codecs import open

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

packages = [
    'incursion',
]


requires = []
with open('requirements.txt', 'r', 'utf-8') as f:
    requires = f.read().split('/n')

with open('README.rst', 'r', 'utf-8') as f:
    readme = f.read()
with open('HISTORY.rst', 'r', 'utf-8') as f:
    history = f.read()

setup(
    name='incursion',
    version=incursion.__version__,
    description='Python InfluxDB Client for Developers.',
    long_description=readme + '\n\n' + history,
    author='Alex Kessinger',
    author_email='voidfiles@gmail.com',
    url='http://github.com/voidfiles/incursion',
    packages=packages,
    package_data={'': ['LICENSE', 'requirements.txt']},
    package_dir={'incursion': 'incursion'},
    include_package_data=True,
    install_requires=requires,
    license='MIT',
    zip_safe=False,
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
    )
)
