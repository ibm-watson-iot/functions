#!/usr/bin/env python

from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()
    f.close()

setup(
      name='custom', 
      version='0.0.1', 
      packages=find_packages(),
      python_requires='>=3.6',
      install_requires=requirements
)
