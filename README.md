# IoT Functions

A companion package to IBM Watson IoT Platform Analytics containing sample functions and base classes from which to derive custom functions.

## Getting Started

These instructions will get you up and running in your local environment or in Watson Studio for development and testing purposes. 

### Prerequisites

Python v3.5, v3.6, or v3.7 with the following modules installed:
* ibm-cos-sdk
* ibm_db
* numpy
* pandas
* SQLAlchemy

### Installing

To install in your local environment:
```
python3 -m pip install -r requirements.txt
python3 -m pip install git+https://github.com/ibm-watson-iot/functions.git@ --upgrade
```

To install in IBM Watson Studio from another Jupyter notebook:
```
!pip install git+https://github.com/ibm-watson-iot/functions.git@ --upgrade
```

Test for sucessful install:
```python
import iotfunctions as fn
print(fn.__version__)
```

### Further information

+ [IBM Knowledge Center - IoT Platform Analytics](https://www.ibm.com/support/knowledgecenter/SSQP8H/iot/analytics/as_overview.html)
+ [Sample Notebook](https://www.ibm.com/support/knowledgecenter/SSQP8H/iot/analytics/as_notebook_references.html)


