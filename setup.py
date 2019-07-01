#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='iotfunctions',
    version='2.0.3',
    packages=find_packages(),
    install_requires=[
        'dill>=0.2.9',
        'urllib3>=1.23',
        'sqlalchemy>=1.3.5',
        'future>=0.16.0',
        'numpy>=1.14.5',
        'pandas>=0.23.3',
        'requests>=2.18.4',
        'lxml>=4.3.3',
        'ibm_db>=3.0.1',
        'ibm_db_sa>=0.3.3',
        'ibm-cos-sdk>=2.1.3',
        'scikit-learn>=0.21.2',
        'joblib>=0.13.2',
        'scipy>=1.3.0',
        'Flask-HTTPAuth>=3.3.0',
        'confluent-kafka>=0.11.5',
        'pyopenssl>=18.0.0',
        'kubernetes>=9.0.0',
        'pyyaml>=5.1',
        'pytest>=4.6.3',
        'pytest-cov>=2.7.1',
    ],
)
