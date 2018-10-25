# IoT Functions

A companion package to IBM Watson IoT Platform Analytics containing sample functions and base classes from which to derive custom functions.

## Getting Started

These instructions will get you up and running in your local environment or in Watson Studio for development and testing purposes. 

### Prerequisites

 + python 3.X
 + numpy
 + pandas
 + ibm_boto3
 + ibm_db
 + ibm_dbi
 + sqlalchemy

### Installing

To install in your local environment:
```
pip install git+https://github.com/ibm-watson-iot/functions.git@ --upgrade
```

To install in IBM Watson Studio from another Jupyter notebook:
```
!pip install git+https://github.com/ibm-watson-iot/functions.git@ --upgrade
```

Test for sucessful install:
```
import iotfunctions as fn
print(fn.__version__)
```

### Further information

+ [IBM Knowledge Center - IoT Platform Analytics](https://www.ibm.com/support/knowledgecenter/SSQP8H/iot/analytics/as_overview.html)
+ [Sample Notebook](https://www.ibm.com/support/knowledgecenter/SSQP8H/iot/analytics/as_notebook_references.html)


