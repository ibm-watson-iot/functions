#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='iotfunctions',
    version='2.0.3',
    packages=find_packages(),
    install_requires=['dill', 'urllib3', 'sqlalchemy'
    ],
)