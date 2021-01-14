#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='iotfunctions', version='8.3.0',  # Update the version in iotfunction/__init__.py file.
      packages=find_packages(),
      install_requires=['dill==0.3.0', 'numpy>=1.18.5', 'pandas>=0.25.3', 'scikit-learn==0.23.1', 'scipy==1.5.0',
                        'requests==2.25.0', 'urllib3==1.26.2', 'ibm_db==3.0.2', 'ibm_db_sa==0.3.5', 'lxml==4.3.4',
                        'lightgbm>=2.3.1', 'nose>=1.3.7', 'psycopg2-binary==2.8.4', 'pyod==0.7.5',
                        'scikit-image>=0.16.2', 'sqlalchemy==1.3.17', 'tabulate==0.8.5', 'pyarrow==0.17.1',
                        'stumpy==1.5.1'], extras_require={'kafka': ['confluent-kafka==1.0.0']})
