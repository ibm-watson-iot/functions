#!/usr/bin/env python
import codecs
import os.path

from setuptools import setup, find_packages


def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delimiter = '"' if '"' in line else "'"
            return line.split(delimiter)[1]
    else:
        raise RuntimeError("Unable to find version string.")


setup(name='iotfunctions', version=get_version("iotfunctions/__init__.py"), packages=find_packages(),
      install_requires=['dill==0.3.0', 'numpy>=1.17.3', 'pandas>=0.24.0', 'scikit-learn==0.20.3', 'scipy>=1.1.0',
                        'requests==2.23.0', 'urllib3==1.24.3', 'ibm_db==3.0.1', 'ibm_db_sa==0.3.3', 'lxml==4.3.4',
                        'lightgbm>=2.3.1', 'nose>=1.3.7', 'psycopg2-binary==2.8.4', 'pyod==0.7.5',
                        'scikit-image>=0.16.2', 'sqlalchemy==1.3.10', 'tabulate==0.8.5', 'pyarrow==0.17.1'],
      extras_require={'kafka': ['confluent-kafka==0.11.5']})
