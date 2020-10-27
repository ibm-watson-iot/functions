#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='custom', version='0.0.1', packages=find_packages(),
      install_requires=['iotfunctions@git+https://github.com/ibm-watson-iot/functions.git@production'])
