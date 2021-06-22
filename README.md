<!--- links to maintain (maximo monitor) --->
<!--- Introduction --->
[documentation]: https://www.ibm.com/docs/en/maximo-monitor/8.4.0?topic=analytics-tutorial-adding-custom-function
<!--- Understanding custom-functions  --->
[the function catalog]: https://www.ibm.com/docs/en/maximo-monitor/8.4.0?topic=calculations-exploring-catalog
[PythonExpression]: https://www.ibm.com/docs/en/maximo-monitor/8.4.0?topic=calculations-using-expressions
[PythonFunction]: https://www.ibm.com/docs/en/maximo-monitor/8.4.0?topic=calculations-using-simple-functions-from-ui
<!--- Understanding custom-functions (BaseClasses)  --->
[IfThenElse]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/bif.py#L1102
[MultilpyTwoItems]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/sample.py#L225
[add data from other sources]: https://www.ibm.com/docs/en/maximo-monitor/8.4.0?topic=data-adding-from-other-sources
[MergeSampleTimeSeries]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/sample.py#L279
[GetEntityData]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/bif.py#L1010
[Activity Duration]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/bif.py#L38
[LookupCompany]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/sample.py#L534
[DatabaseLookup]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/bif.py#L707
[HTTPPreload]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/sample.py#L106
[EntityDataGenerator]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/bif.py#L818
[DeleteInputData]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/bif.py#L746
[SCDLookup]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/bif.py#L1571
[AlertOutOfRange]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/bif.py#L310
[AlertHighValue]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/bif.py#L375
[GBMRegressor]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/anomaly.py#L1959
[BayesRidgeRegressor]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/anomaly.py#L1881
[HelloWorldAggregator]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/sample.py#L833
[AggregateWithExpression]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/bif.py#L73
[DataQualityChecks]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/data_quality.py#L24
<!--- Understanding custom-functions (Base UI)  --->
[BaseUIControl]: https://github.com/ibm-watson-iot/functions/blob/4ab8f8132330f1a6149c3dbc9189063a1373f6be/iotfunctions/ui.py#L16

# custom-function Starter Package

[![Python 3.7](https://img.shields.io/badge/python-3.7-blue.svg)](https://www.python.org/downloads/release/python-370/)

This package builds user understanding of the concepts required for creating custom-functions in Maximo Monitor. It 
contains an
 advanced custom-function tutorial to apply the concepts.

The tutorial builds on the simple tutorial provided in Maximo Asset Monitor [documentation]

-----------

## Table of Contents 

- [Pre-Requisites](#pre-requisites)
  
- [Understanding custom-functions](#understanding-custom-functions)
  - [Why custom-functions](#why-custom-functions)
  - [Parts of custom-function](#parts-of-custom-function)
    - [Base Classes](#i-base-classes)
    - [Execute method](#ii-execute-method)
    - [Build UI classmethod](#iii-build-ui-classmethod)

- [Creating a New Project](#creating-a-new-project)

- [Creating custom-functions](#creating-custom-functions)

- [Debugging Locally](#testing-locally)

- [Registering custom-functions](#registering-custom-function)

- [Verifying in UI](#verifying-in-ui)

- [Debugging In Pipeline](#debugging-in-pipeline)

- [Unregistering custom-functions](#unregistering-custom-function)

-----------

## Pre-Requisites

We start our journey with the set-up required to successfully develop and use a custom-function. All development and
 tutorials in this package use PyCharm IDE. To get started make sure you have the following installed on your machine.

- Python > 3.6
    - Check [python version](https://learnpython.com/blog/check-python-version/)
    - Download [python > 3.6](https://www.python.org/downloads/)
- Install [Pycharm](https://www.jetbrains.com/pycharm/download/#section=windows) community edition
- Learning Resources <br>
  This information is used when creating and testing custom-functions
    - Checkout project from [git repository in Pycharm](https://www.jetbrains.com/help/pycharm/set-up-a-git-repository.html#clone-repo)
    - Creating a [virtual environment in Pycharm](https://www.jetbrains.com/help/pycharm/creating-virtual-environment.html)
    - Testing and [debugging](https://www.jetbrains.com/help/pycharm/debugging-code.html) in Pycharm
        - (POSSIBLE AUTOMATION) should I make a custom run/debug configuration .ipr?
    - Learn about [dataframes](https://pandas.pydata.org/docs/getting_started/intro_tutorials/01_table_oriented.html)
    - Learn about [inheritance](https://pythonbasics.org/inheritance/) 
    - Learn about [classmethod](https://pythonbasics.org/classmethod/)


-----------

## Understanding custom-functions

A custom-function is a multi-argument calculation that produces one or more output items (KPIs). custom-functions are
typically run as part of a multi-function pipeline where the output of the first function is used as input to the
next function and so on. 

#### Why custom-functions

Monitor provides built-in functions; the functionality you want to add might already be present in
[the function catalog]. Within the built-in function monitor contains 
[PythonExpression] and [PythonFunction] that can be used to create one-time-use calculations. 
 
To create re-usable functionality with complicated code patterns you can create custom-functions. custom-functions 
can install other packages, while PythonExpression and PythonFunction are limited to preinstalled analytics 
service packages 

#### Parts of custom-function 

There are three important concepts required in designing a custom-function - base class, execute method, and build_ui 
classmethod. A custom-function is a python object that inherits from one of the provided base classes
. Each object must contain an execute method describing the calculations for which we are designing the function
. Additionally, each object must contain a build_ui classmethod that helps the UI determine the interface to the
 custom-function. The bare bone custom-function looks as shown below.
```python
import BaseClass

class CustomFunctionName(BaseClass):
    def __init__(self, input_item, output_item):
        self.input_item = input_item
        self.output_item = output_item

    def execute(self, df):
        df[self.output_item] = result_of_calculations
        return df

    @classmethod
    def build_ui(cls):
        input = []
        output = []
        input.append(UI("input_item"))
        output.append(UI("output_item"))
        return input,output
```

##### I. Base Classes
First, you need to pick a base class to inherit from.
A base class provides a unique functionality to support data collection, analysis, or analytics. The base classes 
follow an inheritance hierarchy structure as show below

###### Hierarchy of all available base classes 
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
- BaseFunction <br>
Base class for all Analytics Service functions. It sets defaults for all functions and provides generic
 helper methods.  We never inherit from this base class directly. All custom-functions either inherit from
  either BaseTransformer or BaseAggregator
  <br>
  <br>
- BaseTransformer <br>
There are two types of transformers. The transformers that add data add row/s or column/s to the data 
  in the pipeline form another data source, and the "normal" transformers add column/s of calculated metrics.
  You can derive a custom-function from a base class derived from BaseTransformer or directly from 
  BaseTransformer. 
  
  BaseTransformer is used directly to build a custom-function that adds new columns to a dataframe.
  Examples of function that derive from this base class are [IfThenElse], and [MultilpyTwoItems]
  <br>
  <br>
**Transformers that add data from other sources** <br>
Read more about transformers that [add data from other sources].
    <br>
    - BaseDataSource <br>
    Used to combine time series data from another source to pipeline data. This is done by defining a
     `get_data()` method (instead of execute method; see section <INSERT SECTION>) in the custom-function. 
      The method provides code to fetch data from a source external to the pipeline. The external source 
      must contain a timestamp column and a device_id
       column, in addition to the time series data column/s <br>
      Examples of function that derive from this base class are [MergeSampleTimeSeries], and [GetEntityData]
      <br>
    - BaseDBActivityMerge <br>
    Used to merge activity data with time series data. Activities are events that have a start and end 
      date and generally occur sporadically. Activity tables contain an activity column that indicates 
      the type of activity performed. Activities can also be sourced by means of custom tables. This
      function flattens multiple activity types from multiple activity tables into columns indicating 
      the duration of each activity. When aggregating activity data the dimensions over which you 
      aggregate may change during the time taken to perform the activity. To make allowance for this 
      slowly changing dimensions, you may include a customer calendar lookup and one or more resource 
      lookups <br>
    Examples of a function that derive from this base class are [Activity Duration]
      <br>
    - BaseDatabaseLookup <br>
    Used for performing database lookups. Optionally, you can provide sample data for lookup;
    this data will be used to create a new lookup table;data should be provided as a dictionary by 
      setting `self.data`  and used to create a DataFrame. When providing your own 
      data in dictionary you have to set `_auto_create_lookup_table` flag to True. <br>
      The required fields for this class are `lookup_table_name`, `lookup_items`, `lookup_keys`, and are 
      set in the class init method. Classes that inherit from this class don't require an execute method 
      <br>
     Examples of function that derive from this base class are [LookupCompany], and [DatabaseLookup]
      <br>
    - BasePreload <br>
      Preload functions execute before loading entity data into the pipeline. Unlike other functions, 
      preload functions have no input items or output items. Preload functions return a single boolean 
      output on execution. Pipeline will proceed when True. 
      Examples of function that derive from this base class are [HTTPPreload], [EntityDataGenerator], and 
      [DeleteInputData]
      <br>
    - BaseSCDLookup and BaseSCDLookupWithDefault<br>
      Used to add slowly changing property data by doing a lookup from a scd lookup table containing:
      `start_date`, `end_date`, `device_id` and `property` columns. The base class provides both an 
      execute method and a build_ui classmethod. BaseSCDLookupWithDefault provides an additional 
      `default_value` parameter for the item being looked up
      Examples of function that derive from this base class are [SCDLookup]
    <br>
    <br>
      
    **Transformers that perform calculation after all data is gathered**<br>
    - BaseEvent <br>
      Used to produce events or alerts. The base class sets tags that are inferred by the function 
      pipeline to generate alerts.
      Examples of functions that derive from this base class are [AlertOutOfRange], and [AlertHighValue]
      <br>
    - BaseEstimatorFunction <br>
      Used to train, evaluate and predict using sklearn compatible estimators. If training is time 
      intensive it is recommended NOT to derive from this class.
      Method derived from this class use `set_estimators` method to build a sklearn compatible 
      [Pipeline](https://scikit-learn.org/stable/modules/generated/sklearn.pipeline.Pipeline.html)
      Examples of functions that derive from this base class are
      [GBMRegressor], and [BayesRidgeRegressor]
      <br>
      <br>
    
- BaseAggregator <br>
  Used to build a custom-function that aggregates over data at a specified granularity. Monitor supports 
  "Daily" granularity by default, with options to set up and custom granularity. There are two types of 
  bases aggregator classes that derive from this class <br>
  You can 
  [read more](https://pandas.pydata.org/pandas-docs/stable/user_guide/groupby.html#groupby-aggregate-named) 
  about the pandas `agg` and `apply` method to understand the base classes below
  <br>
    - BaseSimpleAggregator <br>
      For simple aggregators the pipeline limits the input parameter name to `source`, and the 
      output parameter name to `name` (Checkout the examples). This further limits the base class to a 
      single output. All simple aggregator methods at the same granularity are parsed into an `agg_dict` and 
      executed using the `agg` method on a pandas.Group (where pandas.Group encodes a granularity)
      ```python
      result = group.agg(agg_dict)
      ```
      Examples of functions that derive from this base class are
      [HelloWorldAggregator], and [AggregateWithExpression]
      <br>
    - BaseComplexAggregator <br>
      Complex aggregators invoke the `apply` method on each sub-group. A sub-group is a pandas.Group defined 
      for a specific granularity
      Using complex aggregators we can generate multiple output columns for one source
      ```python
      result = group.apply(execute)
      ```
      Examples of functions that derive from this base class are [DataQualityChecks]
    <br>
    <br>
      
###### Order of Execution in a function pipeline

A staged pipeline is created to execute all the KPI functions. The stages in the pipeline follow an order of 
execution as show below in a simplified version.Any function inheriting from BasePreload will be executed before 
other functions, 
followed by functions that inherit from base functions in STAGE 2. Transformer functions that inherit from 
base functions in STAGE 3 go next, iff the metrics calculated are not dependent on any metrics calculated in 
STAGE 3. STAGE 4 executes all the remaining transformers, and STAGE 5 executes all aggregators

```markdown
STAGE 1: BasePreload
STAGE 2: BaseDataSource, BaseSCDLookup, BaseSCDLookupWithDefault
STAGE 3: BaseTransformer, BaseEstimatorFunction, BaseEvent, BaseDatabaseLookup (not dependent on any 
transformer metric)
STAGE 4: BaseTransformer, BaseEstimatorFunction, BaseEvent, BaseDatabaseLookup (dependent on transformer metric)
STAGE 5: BaseSimpleAggregator, BaseComplexAggregator
```

##### II. Execute method
After you've picked the base class to inherit from, you need to define your 
calculation in `execute` or `_calc` 
method. This is the calculation we are need the custom-function to perform. As mentioned in the previous section 
BaseDataSource, BaseSCDLookup, BaseSCDLookupWithDefault, and BaseDatabaseLookup base classes don't require an 
execute method ( but it can be overridden).
<br>
The pipeline calls the `execute` method of your function to transform or aggregate data. The execute method 
accepts a dataframe as input and returns a dataframe as output. If the function should be executed on all 
entities combined you can replace the execute method wih a custom one. If the function should be executed by 
entity instance, use the base execute method and provide a custom _calc method instead.
<br>
Example of an execute method added to a function inherited from BaseTransformer (from built-in function
[IfThenElse])
```python
def execute(self, df):
    c = self._entity_type.get_attributes_dict()
    df = df.copy()
    df[self.output_item] = np.where(eval(self.conditional_expression), eval(self.true_expression),
                                    eval(self.false_expression))
    return df
```
For a function that inherits from BaseDataSource, you must specify a `get_data` method that 
returns a dataframe filled with time-series data.
<br>

##### III. Build UI classmethod
The `base_ui` classmethod is used to specify the inputs and outputs in the user interface (UI). The class returns a 
tuple of two array, an input array and an output array respectively (even if the array are empty). 
Below is an example of build_ui classmethod (from built-in function [IfThenElse]) and the UI that is generated 
from that method.
<br>
<br>
**code to build input and output in the UI**
```python
@classmethod
def build_ui(cls):
    # define arguments that behave as function inputs
    inputs = []
    inputs.append(UIExpression(name='conditional_expression', description="expression that returns a True/False value, \
                                eg. if df['temp']>50 then df['temp'] else None"))
    inputs.append(UIExpression(name='true_expression', description="expression when true, eg. df['temp']"))
    inputs.append(UIExpression(name='false_expression', description='expression when false, eg. None'))
    # define arguments that behave as function outputs
    outputs = []
    outputs.append(UIFunctionOutSingle(name='output_item', datatype=bool, description='Dummy function output'))

    return (inputs, outputs)
```

**UI generated from code above**

Input | Output
--- | :----
![IfThenElse_input](/readme-images/IfThenElse_input.png) | ![IfThenElse_output](/readme-images/IfThenElse_output.png)

In this method you build an array of input and output wherein each array item is an object inherited from 
[BaseUIControl]. Each array item is displayed on the UI and used to collect function parameters. Note, that the 
inputs and outputs specified in the base_ui method are same as parameters to the `__init__` method of the custom 
function.
<br>

Below is an example of the `__init__` method for the `base_ui` classmethod from the example above
```python
# The input and output specified in the base_ui should be added as parameters to __init__
def __init__(self, conditional_expression, true_expression, false_expression, output_item=None):
    # makes input and output parameters avialable to other methods
    super().__init__()
    self.conditional_expression = self.parse_expression(conditional_expression)
    self.true_expression = self.parse_expression(true_expression)
    self.false_expression = self.parse_expression(false_expression)
    if output_item is None:
        self.output_item = 'output_item'
    else:
        self.output_item = output_item

```
There are seven different objects that can be used to specify an input, and three different objects to specify an 
output
<br>
**Objects used to specify custom-function input** <br>
- UISingleItem <br>
  Use this object to select a single data item as a function input parameter. This method creates a 
  dropdown of all data  items in the UI (see below), giving the users an option to choose one.
  <br>
  ![UISingleItem](/readme-images/UISingleItem.png)
  <br>
  To use UISingleItem to gather input, the following is added in `build_ui()`
  ```python
  from iotfunctions.ui import UISingleItem
  
  @classmethod
  def build_ui(cls):
      # assumes "input" is defined as a list
      input.append(UISingleItem(
        name="input_item", # same as "input_item" in picture above (required)
        datatype=None,     # float, str, bool, int, dict, datetime.datetime, None
        description=None,  # help text; shows up when user hovers over "input_item"
        required=True,     # set to "True" if this input is required
        tags=None)))
  ```

- UIMultiItem <br>
  Use this object to select multiple data item as a function input parameter. This method creates a dropdown of all 
  data items, giving the users an option to choose one or more items. When `is_output_datatype_derived=True` is 
  set there is an output item defined for every input item selected by the user and we don't need to specify the 
  resulting output items in the output array in `build_ui`
  <br>
  ![UIMultiItem](/readme-images/UIMultiItem.png)
  <br>
  To use UIMultiItem to gather input, the following is added in `build_ui()`
  ```python
  from iotfunctions.ui import UIMultiItem
  
  @classmethod
  def build_ui(cls):
      # assumes "input" is defined as a list
      input.append(UIMultiItem(
        name="input_items",   # same as "input_items" in picture above (required)
        datatype=None,        # float, str, bool, int, dict, datetime.datetime, None
        description=None,     # help text; shows up when user hovers over "input_item"
        required=True,        # set to "True" if this input is required
        min_items=None,       # control minimum input items provided by user
        max_items=None,       # control maximum input items provided by user
        output_item=None,     # name of appended to each output_item  
        is_output_datatype_derived=False, # used when each input_item has a corresonding output_item
        output_datatype=None  # float, str, bool, int, dict, datetime.datetime, None
        tags=None)))
  ```
  
- UISingle <br>
  Use this object to gather single valued constant from the UI. For different datatypes, the UI looks different 
  when collecting the inputs. The examples shown below use `float` and `str` respectively
  <br>
  ![UISingle_float](/readme-images/UISingle_float.png)
  <br>
  ![UISingle_str](/readme-images/UISingle_str.png)
  <br>
  To use UISingle to gather input, the following is added in `build_ui()`
  ```python
  from iotfunctions.ui import UISingle
  
  @classmethod
  def build_ui(cls):
      # assumes "input" is defined as a list
      input.append(UISingle(
        name="constant_name",  # same as "upper_threshold", or "table_name" in pictures above (required)
        datatype=None,         # float (upper_threshold picture), str(table_name picture), bool, datetime.datetime
        description=None,      # help text; shows up when user hovers over "input_item" 
        required=True,         # set to "True" if this input is required
        values=None,           # set to provide a dropdown of valid values to pick from
        default=None,          # default value when no value is provided by user
        tags=None))
  ```
  
- UIMulti <br>
  Use this object to gather multiple constants from the UI. The UI looks different when `values` is 
  specified. In the examples shown below `domain_of_values` do not set `values` parameter in  `UIMulti`, which 
  renders it as a filed that gathers comma separated values. In contrast `checks_with_boolean_output` sets
  `values= ['constant_value', 'stuck_at_zero', 'white_noise']`, which is rendered as a dropdown that allows for 
  single/multi-item selection
  <br>
  ![UIMulti_csv](/readme-images/UIMulti_csv.png)
  <br>
  ![UIMulti_sel_values](/readme-images/UIMulti_sel_values.png)
  <br>
  To use UIMulti to gather input, the following is added in `build_ui()`. When `is_output_datatype_derived=True` is 
  set there is an output item defined for every input item selected by the user and we don't need to specify it 
  in the output array
  ```python
  from iotfunctions.ui import UIMulti
  
  @classmethod
  def build_ui(cls):
      # assumes "input" is defined as a list
      input.append(UIMulti(
        name="constant_name",  # same as "upper_threshold", or "table_name" in pictures above (required)
        datatype=None,         # float (upper_threshold picture), str(table_name picture), bool, datetime.datetime
        description=None,      # help text; shows up when user hovers over "upper_threshold"/"table_name"
        required=True,         # set to "True" if this input is required
        values=None,           # set to provide a dropdown of valid values to pick (one or multiple values) from
        default=None,          # default value when no value is provided by user
        min_items=None,        # control minimum input items provided by user
        max_items=None,        # control maximum input items provided by user
        output_item=None,      # name of appended to each output_item  
        is_output_datatype_derived=False, # used when each input_item has a corresonding output_item
        output_datatype=None,  # float, str, bool, int, dict, datetime.datetime, None
        tags=None))
  ```
  
- UIText <br>
  Use this object to gather multiple lines of text from the UI
  <br>
  ![UIText](/readme-images/UIText.png)
  <br>
  To use UIText to gather input, the following is added in `build_ui()`. 
  ```python
  from iotfunctions.ui import UIText
  
  @classmethod
  def build_ui(cls):
      # assumes "input" is defined as a list
      input.append(UIText(
        name='expression',  # same as "function_code" in pictures above (required)
        description=None,   # help text; shows up when user hovers over "function_code" 
        required=True,      # set to "True" if this input is required
        default=None,       # default value when no value is provided by user,
        tags=None           # set to ["TEXT"] by default
  ))
  ```
      
- UIParameters <br>
  Use this object to capture a json input from the UI. This UI inherits from UISingle wherein the 
  `datatype=dict` parameter is set. <br>
  To use UIText to gather input, the following is added in `build_ui()`
  ```python
  from iotfunctions.ui import UIParameters
  
  @classmethod
  def build_ui(cls):
      # assumes "input" is defined as a list
      input.append(UIParameters(
        name='parameters',                    # default name is "parameters" (required)
        description='enter json parameters',  # help text; shows up when user hovers over "parameters" 
        required=False,                       # set to "True" if this input is required
        default=None,                         # default value when no value is provided by user
        tags=None
  ))
  ```
- UIExpression <br>
  Use this object to enter a python expression from the UI. The UI for this class looks similar to UIText, the 
  only difference is that we store "EXPRESSION" in tags metadata for this object.

**Objects used to specify custom-function output** <br>
- UIFunctionOutSingle <br>
  Use this item to collect information about a single output item. The information one can collect is the name and 
  datatype of the output.
  <br>
  The UI looks similar to the examples shown for UIFunctionOutMulti below. When `datatype=None`, the UI allows user 
  select datatype during custom-function configuration 
  <br>
  To use UIFunctionOutSingle to specify output, the following is added in `build_ui()`
  ```python
  from iotfunctions.ui import UIFunctionOutSingle
  
  @classmethod
  def build_ui(cls):
      # assumes "output" is defined as a list
      output.append(UIFunctionOutSingle(
        name,              # default name for output_item  (required)
        datatype=None,     # str, float, dt.datetime, bool
                           # When "None" let's user select datatype during custom-function configuration 
        description=None,  # help text; shows up when user hovers over name 
        tags=None))
  ```
- UIFunctionOutMulti <br>
  Use this object to collect information for multiple outputs that are dependent on  multi-select 
  input item. The input item is specified in `cardinality_from`. Alternatively, you can set `is_output_derived` flag 
  within the object used to collect the input that this output is dependent on. The number of output items generated 
  at runtime depend on the number of input items then user selects.
  <br>
  The examples below show illustrates different UI rendered when using this object. In the first example the parameter
  `is_datatype_derived=False`, creates a UI that allows users to specify a datatype for each output item. In the 
  second example, the parameter `datatype=str` creates a UI that doesn't need additional user data for output 
  datatypes. The second example can also be achieved when using `is_datatype_derived=True`
  <br>
  ![UIMultiOut_datatype_sel](/readme-images/UIMultiOut_datatype_sel.png)
  <br>
  ![UIMultiOut_datatype_set](/readme-images/UIMultiOut_datatype_set.png)
  <br>
  To use UIFunctionOutSingle to specify output, the following is added in `build_ui()`
  ```python
  from iotfunctions.ui import UIFunctionOutMulti
  
  @classmethod
  def build_ui(cls):
      # assumes "output" is defined as a list
      output.append(UIFunctionOutMulti(
        name,                        # default name for output_item  (required)
        cardinality_from,            # the input item that determines the number of outtpus to create  (required)
        is_datatype_derived=False,   # set to "True" when datatype of output item same as datatype of
                                     # corresponding input item. Let's  user pick datatype when set to "False"
        datatype=None,               # float, str, bool, datetime.datetime, None 
        description=None,            # help text; shows up when user hovers over name  
        output_item=None,            # Not used  
        tags=None))
  ```
  
- UIStatusFlag <br>
  Use this object to output a boolean value indicating that function was executed. This function inherits from 
  UIFunctionOutSingle, with `datatype=bool` parameter.
  <br>
  To use UIStatusFlag to specify output, the following is added in `build_ui()`. Unlike the UIFunctionOutSingle, 
  this object only has one parameter
  ```python
  from iotfunctions.ui import UIStatusFlag
  
  @classmethod
  def build_ui(cls):
      # assumes "output" is defined as a list
      output.append(UIStatusFlag(
        name  # name for output_item  (required)
      ))
  ```
      


-----------

## Creating a new Project

Make your own repo, probably the best where you will store all custom-functions
if it's private  you will need to create PAT vs
public
** add a gif

#### Repository Structure

We use the directory structure shown below for creating and organizing the custom-functions. While this structure is
 not required we will be using and referring to it in the tutorial

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

- All the python files with custom-function classes go in the **custom** directory
- All testing scripts go in the **scripts** directory
- The credentials go in **dev_resources** directory. NOTE that in this package we add **dev_resources**
  directory in .gitignore to prevent credential leaks. If you chose to put your credentials in a
  different folder make sure to NOT push the credentials file to your github
  
#### Open Project in Pycharm

-----------
## Creating custom-functions

#### 
Set up repository structure  
File in custom folder that will contain the new class
Define calculation (in execute for most cases)
do a build_ui

The bare bone function looks as shown below. Except when we use _calc and except when we do preload
```python
import BaseClass

class CustomFunctionName(BaseClass):
    def __init__(self):
        pass

    def execute(self, df):
        pass

    @classmethod
    def build_ui(cls):
        pass
```


#### Example functions for each base class
BaseTransformer
BaseDataSource
BaseDBActivityMerge
BaseDatabaseLookup
BasePreload
BaseSCDLookup
BaseSCDLookupWithDefault
BaseEvent
BaseClassifier
BaseRegressor
BaseSimpleAggregator
BaseComplexAggregator

#### Generic helper methods and variables from BaseFunction
Are set by the pipeline so yeah but how do we test this
- self._entity_type.index_df(dataframe_that_needs_to_be_indexed)

-----------

## Testing Locally

Credentials

execute_local_test (random data) vs. creating your own data (will need appropriate indexing)

Indexing in the dataframe for Transformers vs aggregators

Add a script for each

Disclaimer No gaurantee about pipeline run 

-----------
## Registering custom-function

What do we need - credentials to connect to the database + `register_function` + A way to find the package where the
 custom-function resides + a pip installable package
name the database that the function goes in when it registers

#### Retrieving and saving credentials in SaaS

**Important**: The credentials file is used to run or test functions locally. Do not push this file to any external
 repository. In this package we add **dev_resources** directory in .gitignore to prevent credential leaks. If
  you chose to put your credentials in a different folder make sure to NOT push the credentials file to your github

1. Create a credentials.json file in the dev_resources folder in your working directory. 
2. On the user interface, go to the **Services** tab.
3. Select Watson IoT Platform Analytics and click **View Details**.
4. In the Environment Variables field, click **Copy to Clipboard**.
5. Paste the contents of the clipboard into the credentials.json file.
   
#### Loading credentials in a script

```python
credentials_path=<path to credentials.json>
with open(credentials_path, 'r') as F:
    credentials = json.load(F)
```
   
#### Setting PACKAGE_URL

PACKAGE_URL vs url parameter in register_function vs other ways to save your PAT from getting stolen

-----------
## Unregistering custom-function

If a metric is dependent on a won't unregister the function

-----------
## Verifying in UI

-----------
## Debugging In Pipeline








