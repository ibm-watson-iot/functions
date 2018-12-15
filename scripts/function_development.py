
# ## Developing functions in IBM Watson IoT Platform Analytics

# Use the instructions in this notebook to develop and register custom functions in Watson IoT Platform Analytics.

# ### Installing the Watson IoT Platform Analytics sample repository

# The sample repository contains sample functions. Before you start developing custom functions, it is recommended that you get to know the sample functions and how they work.
# 
# To install the sample repository, run the following commands:

# In[3]:

import datetime as dt
import json
import os

import pandas as pd

with open('credentials_demo_as.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())


os.environ['DB_CONNECTION_STRING'] = 'DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s;' %(credentials["database"],credentials["hostname"],credentials["port"],credentials["username"],credentials["password"])
os.environ['API_BASEURL'] = 'https://%s' %credentials['as_api_host']
os.environ['API_KEY'] = credentials['as_api_key']
os.environ['API_TOKEN'] = credentials['as_api_token']


# A separate set of credentials is available to allow access to IBM Cloud Object Storage. The credentials below are "external credentials" as they allow access from outside of the IBM Cloud.

# In[2]:


with open('credentials_cos_iam.json', encoding='utf-8') as F:
    cos_credentials = json.loads(F.read())


# **Tip:** If you want to use environment variables to access the database and API, create them in advance before you import the Watson IoT Platform libraries.

# In[9]:


from iotfunctions.preprocessor import BaseTransformer,ExecuteFunctionSingleOut, BaseDatabaseLookup, ComputationsOnStringArray
from iotfunctions.util import cosSave,cosLoad


# ### Registering simple functions

# The Watson IoT Platform Analytics engine uses Pandas dataframes internally to transform entity data. The most basic functions operate on a subset of entity data items to produce a single new data item as a new column in a Pandas dataframe. You can author simple functions directly in a notebook or script and register them using the Watson IoT Platform Analytics API.
# 
# Here is an example of a simple function that does a column-wise sum of a list of items.

# In[5]:


def column_sum(df,parameters):
    '''
    Columnwise sum of multiple input columns. No custom parameters required.
    '''
    rf = df.copy()
    rf[parameters['output_item']] = df[parameters['input_items']].sum(axis=1)
    return rf


# The class that calls this function will include all if its instance variables as parameters to the function, including 'input_items' and 'output_item'. There are no additional parameters required for this function.
# 
# This function will be executed by a Function Pipeline that consists of a number of stages. Each stage is carried out using a Python class. The Python class that is used to execute a simple function as part of pipeline is called *ExecuteFunctionSingleOut*. To register a new simple function, we will create an instance of this class and pass to it the function that we will be registering, along with a set of parameters that are predefined in the class.
# 
# The *ExecuteFunctionSingleOut* object that is created below needs to give the function object that we are registering a permanent home. At the moment, it exists only in local memory. Its permanent home will be a bucket in Cloud Object Storage. To place the function in its permanent home, we will need COS credentials and a COS bucket. It will be placed by using the function name as the object name, so make sure that the function name is unique in the bucket.

# To register a function, you will also need some test data. You can define your own test data or use the **get_test_data()** method to retrieve some generic test data. The generic test data includes numeric columns x_1,x_2 and x_3. We will test the function with these values as inputs.

# In[6]:


colsum = ExecuteFunctionSingleOut(function_name = column_sum,
                                  cos_credentials = cos_credentials,
                                  bucket = 'models-bucket',
                                  input_items = ['x_1','x_2','x_3'] ,
                                  output_item = 'output_item')
df = colsum.get_test_data()
df


# Let's look at the provided test data:

# Next, let's test the function on this data:

# In[7]:


test_df = colsum.execute(df)
test_df


# The function returns the expected result. Now that we have tested the function, it is time to register it in the function catalog. To register it, we will need the credentials for Watson IoT Platform.

# In[8]:


colsum.register(df=df,credentials = credentials)


# After you register the function, it becomes available for selection in Watson IoT Platform Analytics. It is a general-purpose function that can be used with an entity type. The parameters that were passed to the colsum instance (which we used to test and register the function) were used to identify types and cardinalities of arguments - not actual argument values.
# 
# When it is invoked, the UI will ask for a function_name, COS credentials and bucket from where to retrieve this function, a list of input_items and an output_item.

# #### Serializing functions to COS

# Now that *ExecuteFunctionSingleOut* is registered in the function catalog, you can use it to execute any function that has been serialized to COS. The only restriction is that the function must take a list of inputs, produce a single output item and get all of its other arguments from the "parameters" dict. We will create another function to test it. *column_max* is like *column_sum* except that it calculates a columnwise maximum, rather than calculate the sum.

# In[9]:


def column_max(df,parameters):
    '''
    Columnwise max of multiple input columns. No custom parameters required.
    '''
    rf = df.copy()
    rf[parameters['output_item']] = df[parameters['input_items']].max(axis=1)
    return rf


# Serialize the function:

# In[10]:


cosSave(obj=column_max,bucket='models-bucket',filename='column_max',credentials=cos_credentials)


# To use *column_max* in the UI, choose the *ExecuteFunctionSingleOut* function when adding a new item. Specify 'column_max' as the function_name. Provide the same bucket and credentials.

# #### Using custom parameters

# The function *column_sum_x_constant* takes the sum of a list of columns and then multiplies that value by a constant.

# In[11]:


def column_sum_x_constant (df,parameters):
    '''
    Columnwise sum of multiple input columns multiplied by a constant. Requires customer parameter: 'multiplier'.
    '''
    rf = df.copy()
    rf[parameters['output_item']] = df[parameters['input_items']].sum(axis=1) * parameters['multiplier']
    return rf


# You can use any number of custom parameters in your function definition. Simply formulate them as a single dict. Test this function by using a multiplier of 3.41.

# In[12]:


colsumx = ExecuteFunctionSingleOut(function_name = column_sum_x_constant,
                                   cos_credentials = cos_credentials,
                                   bucket = 'models-bucket',
                                   input_items = ['x_1','x_2','x_3'] ,
                                   output_item = 'output_item',
                                   parameters = {'multiplier':3.41})
test_df = colsumx.execute(df)
test_df


# When using *ExecuteFunctionSingleOut* with *column_sum_x_constant*, make sure to supply the parameter 'multiplier' inside of the JSON input that will be provided in the UI.

# ## Extending the function catalog with custom classes

# In the previous section, we learned how to register a function by writing it directly in the notebook. This is the easiest way to register a function, but there some limitations to this approach:
# 
# 1) All parameters that are passed to a simple function must be included within a single Python dictionary. This dictionary manifests as a single JSON object in the Watson IoT Platform Analytics UI and doesn't take advantage of the UI's ability to expose each separate parameter with appropropriate type and cardinality validation.
# 
# 2) The complete set of business logic for the function must be contained in the function. The function cannot call out to any other classes or functions that are not already included in the Analytics runtime.
# 
# You can overcome these limitations by defining new custom Python Classes to extend the Analytics runtime. When using functions registered from these classes in the UI, each of the Class' parameters shows as a descrete property with type and cardinality validation. It is also possible to reference other Python libraries when extending the Analytics runtime using custom classes.
# 
# Custom classes fall into two categories: **tranformers** and **aggregators**. Transformers add new columns to a dataframe. Aggregators aggregate dataframes. When you create a new transformer, you must create a new Python class. Inherit from BaseTransformer.
# 
# We will start by looking at a really simple transformer function. It multiplies a data item by a hard-coded constant of 2.

# In[13]:


package_url = 'git+https://@github.com/ibm-watson-iot/functions.git@'


# In[14]:


class MultiplyByTwo(BaseTransformer):
    '''
    Multiply input column by 2 to produce output column
    '''
    url = package_url
    
    def __init__(self, input_item, output_item='output_item'):
        
        self.input_item = input_item
        self.output_item = output_item
        super().__init__()

    def execute(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_item] * 2
        return df


# This example introduces three common elements of custom function classes.
# 
# - **Doc String:** The doc string of the function will is displayed in the UI, so ensure that you provide a good description. 
# 
# - **URL:** The URL is a link to the repository where the source code for this function may be found. The clasess show in this notebook are included for illustrative purposes. When Watson IoT Platform Analytics uses these functions it will install them inside a container that it uses as its runtime engine. The install takes place from a pip accessible URL such as the package_url above that contains a link to a git repository. When using a private repository you will need to include a personal access token in the URL.
# 
# - **`__init__`:** The **`__init__`** method can contain any number of custom parameters for your function.
# 
# - **execute:** The execute method only accepts one parameter: a dataframe. It must return a dataframe as output too. Most of your custom code will reside in the execute method. 
# 
# In the case of the function above, the transformation code is very simple: The result of the calculation (output_item) = input_item * 2.
# 

# ### Registering a function

# To register the function in the Watson IoT Platform Analytics function catalog, you must create an instance of it and call the **register()** method. There is no need to define a **register()** method for each function that you create. It will be inherited from the base class. 
# 
# Before you execute the **register()** method, you need some test data. If you would like you use your own test data rather than the data provided by the **get_test_data()** method, you can create your own DataFrame.
# 
# **Note:** It doesn't have to look exactly like your Analytics entity data. It is only used so that the **register()** method can understand datatypes and cardinalities of data items.

# In[15]:


data = {
    'x1' : [1,2,3],
    'x2' : [4,5,6],
    'x3' : [7,8,9]
}
df = pd.DataFrame(data=data)
df


# Create an instance of your function class. Choose appropriate parameter values for your input parameters. In this example, we tested on the 'x1' column, which produced a new column called 'x1_doubled'.

# In[16]:


from iotfunctions.preprocessor import MultiplyByTwo
mb2 = MultiplyByTwo(input_item='x1')


# Test your function by calling the **execute** method.

# In[17]:


test_df = mb2.execute(df)
test_df


# It's working!

# Register the function by calling the **register()** method. Pass the dataframe and your credentials.

# In[18]:


mb2.register(df=df,credentials=credentials)


# **Important:** The register method attempts to degregister the function before re-registering. If you see an error indicating that the function couldn't be de-registered because it is in use, you will have to remove the items that are using it before you can re-register it. 

# After you register the function, it will become available for selection in Watson IoT Platform Analytics. When it is invoked, the UI will ask for an *input_item* and an *output_item*. The registration method will detect that 'x1' was a single data item rather than a constant or a list. It will pass this on the UI so that the UI can present a pick list that allows selection of a single data item.

# **Note:** Before registering the function, we imported it by using the statement `from ascustomfuncs.preprocessor import <function_name>`. This import statement replaces the class definition in the notebook. 
# 
# In general, class definitions are not defined in a notebook. They appear here for documentation purposes only. The official source of the class is the package that contains the source for the class.

# ### Passing other parameters to functions

# The *MultiplyByConstant* function is similar to the previous one, except that instead of multiplying by a hard-coded value, the user should be prompted to enter a constant for the multiplier. Notice how this function has an extra parameter called *constant*.

# In[20]:


class MultiplyByConstant(BaseTransformer):
    '''
    Multiply input column by a constant to produce output column
    '''
    url = package_url
    
    def __init__(self, input_item, constant, output_item):
                
        self.input_item = input_item
        self.output_item = output_item
        self.constant = constant
        super().__init__()

    def execute(self, df):
        df = df.copy()        
        df[self.output_item] = df[self.input_item] * self.constant
        return df


# We will test this function by multiplying x1 by 3.4 to produce an item called *output_item*.

# When this function is invoked, the UI prompts you to enter both an "input item" and a "constant". The UI will also ensure that the type of the constant is consistent with the type of constant used for testing. In this case, the UI will expect a numeric "constant".

# In[21]:


from iotfunctions.preprocessor import MultiplyByConstant
mc = MultiplyByConstant(input_item='x1',output_item='output_item',constant=3.4)
mc.register(credentials = credentials,df=df )


# When you import a function from the installed repository, Python uses metadata to link the class to the module that contains it. The **register()** function introspects the class to determine the module. 

# ### Including more than one item in a function

# *MultiplyTwoItems* has two input items (input_item_1) and (input_item_2). These are multiplied together to produce the output.

# In[22]:


class MultiplyTwoItems(BaseTransformer):
    '''
    Multiply two input items together to produce output column
    '''
    url = package_url
    
    def __init__(self, input_item_1, input_item_2, output_item):
        self.input_item_1 = input_item_1
        self.input_item_2 = input_item_2
        self.output_item = output_item
        super().__init__()
        self.itemDescriptions['input_item_1'] = 'The first item used'
        self.itemDescriptions['input_item_2'] = 'The second item used'

    def execute(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_item_1] * df[self.input_item_2]
        return df        


# This example introduces a predefined instance variable called **itemDescriptions**. 
# 
# **itemDescriptions** is a dictionary that is created when initializing an instance of this class from its base class (**BaseTransformer**) using `_super().init()_`. 
# 
# **Important:** Please use good descriptions in the metadata, as the descriptions captured here are displayed in the UI. 
# 
# In this notebook example, there is no need to distinguish between the items that are used in a multiplication equation. However, if this function was doing a division calculation instead, the descriptions become very important. 
# 
# In the case of `_input_item_1_ / _input_item_2_` it is very important for the user who is selecting items to know which item is used as the numerator and which item is used as the denominator.

# In[23]:


from iotfunctions.preprocessor import MultiplyTwoItems
m2 = MultiplyTwoItems(input_item_1='x1',input_item_2='x2',output_item='output_item')
m2.register(credentials = credentials,df=df )


# When this function is invoked, the UI prompts you to enter two input items.

# ### Including a list of items as input

# This function uses an arbitrary list of items as inputs. All inputs are multiplied together to calculate the product.

# In[24]:


class MultiplyNItems(BaseTransformer): 
    ''''
    Multiply multiple input items together to produce output column
    '''
    url = package_url
    
    def __init__(self, input_items, output_item):
    
        self.input_items = input_items
        self.output_item = output_item
        super().__init__()

    def execute(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_items] * 2
        return df
        

    def execute(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_items].product(axis=1)
        return df  


# Since Python is a dynamically-typed language, we do not differentiate between single items and lists of items when we define the input parameters. 
# 
# We named the parameter using the plural "inputs_items" instead of singular "input_item" as a reminder to developers to ensure that - when coding - allowances are made for multiple items.
# 
# It is in the **execute()** method that we define how the function operates on a list of items. In this example, we are simply passing the function a list in a DataFrame operation, so there is no need to explicitly code for a list. 
# 
# The Pandas **product()** method accepts a DataFrame with multiple columns and returns a single new column (series) that contains the product of the itemns defined as inputs.
# 
# When you register the function, it is neccessary to state whether parameters are single-value or act as arrays with mutiple values. The **register()** method uses the types assigned to a test instance to infer how parameters behave in the UI. 
# 
# The "input items" parameter in the UI should accept a list of numeric inputs, so we create a test instance and include a list of numeric inputs as parameter values. The **register()** method recognises 'x1','x2' and 'x3' as being data items as they are column names that are present in the test dataframe that is passed.
# 

# In[25]:


from iotfunctions.preprocessor import MultiplyNItems
mN = MultiplyNItems(input_items=['x1','x2','x3'],output_item='output_item')
mN.register(credentials = credentials,df=df)


# ### Functions that return an array of outputs

# The function `MultiplyArrayByConstant` takes a list of input items and multiplies each one by a constant to return a new list of output items.

# In[26]:


class MultiplyArrayByConstant(BaseTransformer):
    '''
    Multiply a list of input columns by a constant to produce a new output column for each input column in the list.
    The names of the new output columns are defined in a list(array) rather than as discrete parameters.
    '''
    def __init__(self, input_items, constant, output_items):
                
        self.input_items = input_items
        self.output_items = output_items
        self.constant = float(constant)
        super().__init__()

    def execute(self, df):
        df = df.copy()
        for i,input_item in enumerate(self.input_items):
            df[self.output_items[i]] = df[input_item] * self.constant
        return df


# This function was coded to produce an `_output_item_` for each `_input_item_`. When we register the function, we will create an instance of the class using representative `_input_items_` and `_output_items_`, so that the **register()** method understands that that both contain lists of numeric items.

# In[27]:


from iotfunctions.preprocessor import MultiplyArrayByConstant
mac = MultiplyArrayByConstant(input_items = ['x_1','x_2'],constant = 3, output_items = ['xout_1','xout_2'])
mac.register(credentials = credentials,df=mac.get_test_data() )


# **Important:** Data items that are produced as the output of a function must always have different names to the input items. This allows the **register()** method to distingish between inputs and outputs. It is vitally important that the Watson IoT Platform Analytics engine understands all of the inputs and outputs of each function, as it uses this information to automatically sequence functions based on their dependencies. 

# ### Defining picklists for constants

# The UI can present a dropdown menu of one or more constants. To create a dropdown menu, add the values to the *itemValues* instance variable after you initialize the base class. The *itemValues* dictionary is keyed by parameter name.

# In[28]:


class MultiplyByConstantPicklist(BaseTransformer):
    '''
    Multiply input value by a constant that will be entered via a picklist.
    '''
    
    def __init__(self, input_item, constant, output_item = 'output_item'):
                
        self.input_item = input_item
        self.output_item = output_item
        self.constant = float(constant)
        super().__init__()
        
        self.itemValues['constant'] = [-1,2,3,4,5]

    def execute(self, df):
        df = df.copy()        
        df[self.output_item] = df[self.input_item] * self.constant
        return df


# In[29]:


from iotfunctions.preprocessor import MultiplyByConstantPicklist
mcp = MultiplyByConstantPicklist(input_item='x1',constant=3,output_item='output_item')
mcp.register(credentials = credentials,df=df )


# ### Functions that must be executed separately for each entity instance

# In most cases, the function operates on a row of data at a time, so you can safely execute it against a dataframe that contains data for many entities. This is not always the case, however. 
# 
# Consider a function that fills missing values by using the previously-known values. When filling values for entity instance 2, it would be incorrect to use the last-known value for entity instance 1. 

# In[30]:


class FillForwardByEntity(BaseTransformer):    
    '''
    Fill null values forward from last item for the same entity instance
    '''
    url = package_url
    execute_by = ['id']
    
    def __init__(self, input_item, output_item):
    
        self.input_item = input_item
        self.output_item = output_item
        super().__init__()
        
    def _calc(self, df):
        df = df.copy()
        df[self.output_item] = df[self.input_item].ffill()
        return df  


# You will notice differences between this class and the classes that we have looked at so far. 
# 
# 1) It has an *execute_by* class variable, containing a list with a single item (deviceid). 'deviceid' is the column that identifies different entity instances. When Watson IoT Platform Analytics executes the function, it will split the data by deviceid.
# 
# 2) It has a **_calc()** method instead of the execute method that we saw previously. The reason for this is that it inherits the execute method from the BaseTransformer. The execute method on the BaseTransformer groups by the *execute_by* columns and then calls the **_calc** method, applying it to the members of each group.
# 
# To test this function, we will need a dataframe with some nulls and a device id.

# In[31]:


data = {
    'id' : [1,1,1,1,1,2,2,2,2,2],
    'x1' : [4.1,4.2,None,4.1,3.9,None,3.2,3.1,None,3.4]
}
df = pd.DataFrame(data=data)
df


# In[32]:


from iotfunctions.preprocessor import FillForwardByEntity
ff = FillForwardByEntity(input_item='x1',output_item='output_item')
test_df = ff.execute(df)
test_df


# In[33]:


ff.register(credentials = credentials,df=df )


# The test was successful. Notice how the first null row for **deviceid 2** is still null because there was no prior value to use to fill it. If the calculation was not executed by device id, the first row for device id would have been filled to 3.9 - the previous row for deviceid1.

# ## Performing Database Operations

# Watson IoT Platform Analytics keeps calculated values up-to-date by monitoring the incoming time series data for entities, feeding them into functions to transform data, and writing summary results back to Db2. 
# 
# Since the engine automates data movement, the simple functions and custom classes that we have looked at do not include any database operations.
# 
# In some cases, you might need to augment the automated data operations as part of function execution. 

# ### Writing custom outputs from a function

# By default, Watson IoT Platform Analytics writes the results of the execution of a set of functions to the database in a key value pair structure. All writes are upserts. 
# 
# In some cases, you might need to write outputs in a different way. For example, you can version the write of a forecast by the date of forecast instead of updating prior forecasts. You can invoke a database write inside a function if you need to. For example:

# In[5]:


class WriteDataFrame(BaseTransformer):
    '''
    Write the current contents of the pipeline to a database table
    '''
    out_table_prefix = ''
    version_db_writes = False
    out_table_if_exists = 'append'

    def __init__(self, input_items, output_status,out_table_name,db_credentials=None):
        self.input_items = input_items
        self.output_status = output_status
        self.db_credentials = db_credentials
        self.out_table_name = out_table_name
        super().__init__()
        
    def execute (self, df):
        df = df.copy()
        df[self.output_status] = self.write_frame(df=df)
        return df  


# 
# This class defines a function that writes the contents of a pipeline to the database. 
# 
# To invoke the database write, you can use the **write_frame()** method that is inherited from the base class. The table name is defined in another predefined class or instance variable called *out_table_name*. There is an optional prefix that you can supply too. You can use this like a namespace to prevent table name clashes. 
# 
# This particular function only writes to the database. It doesn't do any transformation of the *input_item*. The *input_item* is defined to help the Watson IoT Platform Analytics engine work out when to perform the write. 
# 
# If you were to define this as a list of items instead of a single item, the write takes place only when the list of dependent items has been calculated. The output of the function is a status flag that indicates that the write took place.

# In[6]:

from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean
from iotfunctions.preprocessor import WriteDataFrame
from iotfunctions.metadata import EntityType
from iotfunctions.db import Database
db = Database(credentials = credentials)
entity = EntityType("widgets",db,
                    Column("temp", Float),
                    **{
                        '_timestamp' : 'obs_timestamp'
                      }
                    )
wf = WriteDataFrame(input_items = ['temp'] , output_status = 'output_status',out_table_name='test_function_custom_out' )
wf._entity_type = entity
df = entity.get_data()
wf.register(df ,credentials=credentials)
test_df = wf.execute(df)
test_df


# 
# After this function is executed, a table called TEST_FUNCTION_CUSTOM_OUT is written to the database.
# 
# This sample has been setup to append to the table with each subsequent write. You can change this behavior by setting *out_table_if_exists* to replace. You can also set *version_db_writes* to True to version each write with a timestamp.
# 
# You can change this default behavior by using the class variables *versionDBWrite* (default True) and *out_table_if_exists* (default 'append', other values are 'fail' and 'replace').

# ### Doing a Database Lookup

# A database lookup is a function that retrives one or more column of data from a database table (or query) and combines it with input data. The most common use for lookups is gathering data that is required for function input but was not present in the incoming message data. Unlike the "Database Merge" that we will cover in a subsequent update, the lookup is used to get access to non-time variant data.
# 
# Lookup tables are expected to have one or more business keys and produce one or more lookup items. The example below is a Company lookup. The lookup table is keyed on the column "company_code". It has columns: "currency_code"; "employee_count"; "inception_date" all of which are available as _lookup_items_. 
# 
# We provide a special base class for lookups. Since most lookups behave the same way, you can use the provided LookupCompany sample as a template when building your own lookup functions.

# In[10]:


class LookupCompany(BaseDatabaseLookup):
    """
    Lookup Company information from a database table        
    """

    
    def __init__ (self, company_key , lookup_items= None, output_items=None):
        
        # sample data will be used to create table if it doesn't already exist
        # sample data will be converted into a dataframe before being written to a  table
        # make sure that the dictionary that you provide is understood by pandas
        # use lower case column names
        self.data = {
                'company_code' : ['ABC','ACME','JDI'] ,
                'currency_code' : ['USD','CAD','USD'] ,
                'employee_count' : [100,120,352],
                'inception_date' : [
                        dt.datetime.strptime('Feb 1 2005 7:00AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Jan 1 1988 5:00AM', '%b %d %Y %I:%M%p'),
                        dt.datetime.strptime('Feb 28 1967 9:00AM', '%b %d %Y %I:%M%p')
                        ]
                }
        
        # Must specify:
        #One or more business key for the lookup
        #The UI will ask you to map each business key to an AS Data Item
        self.company_key = company_key
        #must specify a name of the lookup name even if you are supplying your own sql.
        #use lower case table names
        lookup_table_name = 'company'                
        # A sql statement that will be used to retrieve data for the loookup
        sql = 'select * from %s' %lookup_table_name
        # Indicate which of the input parameters are lookup keys
        lookup_keys = [company_key] 
        # Indicate which of the column returned should be converted into dates
        parse_dates = ['inception_date']
        
        #db credentials are optional. Only required to connect to a non AS DB2.
        #Make use that you have your DB_CONNECTION_STRING environment variable set to test locally
        #Or supply credentials in an instance variable
        #self.db_credentials = <blah>
        
        super().__init__(
             lookup_table_name = lookup_table_name,
             sql= sql,
             lookup_keys= lookup_keys,
             lookup_items = lookup_items,
             parse_dates= parse_dates, 
             output_items = output_items
             )
    
        # The base class takes care of the rest
        # No execute() method required


# Next, test the lookup.

# In[11]:


from iotfunctions.preprocessor import LookupCompany
lup = LookupCompany(company_key='company_code',lookup_items=['currency_code'],output_items=['company_currency_code'])
lup._entity_type = entity
test_df = lup.execute(df = df)
test_df


# Finally, register the lookup.

# In[13]:


lup.register(credentials=credentials,df= lup.get_test_data())


# When you select this function in the UI, you will be asked to specify a "company_key" data item in your incoming entity data. You will be allowed to choose from a dropdown menu of available `_lookup_items_`. 
