# Custom Function Starter Package

[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)

This package contains advanced custom-function tutorial, and detailed .. of concepts required to build any custom

In this package you will --- 

[Pre-Requisites](#pre-requisites)

[Repository Structure](#repository-structure)

[Understanding Custom Functions](#understanding-custom-functions)

[Creating Custom Functions](#creating-custom-functions)

[Testing Locally](#testing-locally)

[Registering Custom Functions](#registering-custom-function)

[Unregistering Custom Functions](#unregistering-custom-function)

[Testing In UI](#testing-in-ui)

[Debugging Resources](#debugging-resources)

-----------

##Pre-Requisites

Something about using pycharm for all setup and development

- python > 3.6
- Download Pycharm 
- Creating a project in Pycharm
- Virtual enviroment setup in Pycharm (link)
- Git repository setup in Pycharm (link)
- Testing and debugging in Pycharm
- Dataframes

-----------

## Repository Structure

Can follow this for any repo thatis used to add custom functions to the UI

All files with custom functions go in 
All testing scripts go in 
credential in dev resources and ignore the entire folder in .gitignore

```bash
├── project
│   ├── custom
│   │   ├── **/*.py
│   ├── scripts
|   │   ├── **/*.py
│   ├── dev_resources
├── requirements.txt
├── README.md
└── .gitignore
```

-----------
## Understanding Custom Functions

#### Why custom functions

#### Parts of custom function 
(base class, execute vs _calc, buildui static class)
Custom function is an object that inherits from a provided base class (that follow an order of execution within the
function pipeline). 
Each object must contain an execute method (...) with the calculations
Additionally, each object must contain a buildui static method

1. Base Classes
    - Hierarchy of all available base classes 
        ```bash
        ├── BaseFunction
        |   │
        │   ├── BaseTransformer
        │   │   ├── BaseDataSource
        │   │   │   ├── BaseDBActivityMerge
        │   │   ├── BaseDatabaseLookup
        │   │   ├── BaseEvent
        │   │   ├── BaseEstimatorFunction
        │   │   │   ├── BaseRegressor
        │   │   │   ├── BaseClassifier
        │   │   ├── BasePreload
        │   │   │   ├── BaseMetadataProvider
        │   |   ├── BaseSCDLookup 
        │   |   ├── BaseSCDLookupWithDefault 
        |   │
        │   ├── BaseAggregator
        │   │   ├── BaseSimppleAggregator
        │   │   ├── BaseComplexAggregator
        │
        ```
    - Order of Execution in a function pipeline

2. Execute, Calc
3. Build UI

-----------
## Creating Custom Functions

Add file and link with example

-----------

## Testing Locally

Add a script for each

Disclaimer No gaurentee about pipeline run 

-----------
## Registering Custom Function

-----------
## Unregistering Custom Function

If a metric is dependent on a won't unregister the function

-----------
## Testing in UI

-----------
##Debugging Resources








