#!/usr/bin/env python

# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()
    f.close()

with open('iotfunctions/__init__.py') as f:
    version_ = f.read()
    exec(version_)
    f.close()

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='iotfunctions',
    version=__version__,
    author='Sivakumar Rajendren',
    author_email='rsiva@us.ibm.com',
    description='Open source component of the Maximo Asset Manager pipeline',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/ibm-watson-iot/iotfunctions',
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=requirements,
    extras_require={'kafka': ['confluent-kafka==1.6.0']}
)
