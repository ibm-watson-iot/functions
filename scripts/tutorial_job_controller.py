import json
import logging
import datetime as dt
import pandas as pd
from iotfunctions.pipeline import JobController, DataWriterFile, DataAggregator
from iotfunctions.db import Database
from iotfunctions.base import BaseTransformer, BaseDataSource
from iotfunctions.enginelog import EngineLogging
from iotfunctions.metadata import Granularity

EngineLogging.configure_console_logging(logging.DEBUG)

'''
JobController objects manage the execution of jobs, taking care of:
-- Managing dependencies between the tasks (stages) in a multi-stage job
-- Managing dataflow between stages 
-- Manage job metadata
-- Error handling
-- Logging
-- Writing results

JobController objects call the execute() method on the stages included in
a job. Jobs are considered the payload of the JobController.

Generally when working with AS the payload will be an EntityType. There
are convenience methods such as exec_local_pipeline defined on the EntityType
that build and execute a JobController so there is no need to interact with
the JobController directly.

This tutorial is aimed at developers of alternative payloads or developers
wanting to get an in depth understanding of the JobController from first
principles.  

Firstly we will need a job to execute. We will create a custom one in this
tutorial. This job will execute a single data transformation task.

'''

class CustomTask(BaseTransformer):

    _allow_empty_df = True          # allow this task to run even if it receives no incoming data
    produces_output_items = False   # this task does not contribute new data items
    requires_input_items = False    # this task does not require dependent data items

    def __init__(self,name):

        self.name = name

    def execute(self,df):

        result = '**** Executed task %s ****' %self.name
        print (result)

        return result

class CustomJob(object):

    _abort_on_fail = True           # abort at the end of a failed execution

    def __init__(self,name,db,stages):

        self.name = name
        self.db = db
        self._stages = stages

    def classify_stages(self):

        return self._stages

'''
Before creating an instance of the this job, we will need a Database object.
This will provide the JobController access to the AS APIs and database.
'''

# replace with a credentials dictionary or provide a credentials file
with open('credentials_as_dev.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

db = Database(credentials=credentials)

'''
We will also need to create a task to execute. 
'''

job_task = CustomTask('my_task')

'''
The various tasks that a job performs are defined in a dictionary.

The dictionary is keyed on a tuple. The tuple contains a string
denoting a classification of the task according to what it does and
a granulariity object - indicating at what level of granularity the
task operates at. Each entry of the dictionary contains a list of
stage objects.

Our simple task doesn't do very much, but it does return a constant.
We will think of it as a 'transform' task of the simplest type.

Any task that operates on input level (raw) data is known has
a granularity of None. 

We will cover other types of task and otther granularities later.

Our dictionary will have a list of stages - containing a single task.

'''

stages = {
    ('transform',None) :  [job_task]
}

my_job = CustomJob('basic_tutorial_job',db=db,stages=stages)

j = JobController(payload=my_job, save_trace_to_file = True )
j.execute()

'''
The easiest way to see what the job did is to consult the trace.
The trace is a json file located in the script folder.
Execution is also logged to a database table: job_log
object_name is the name of the job (basic_totorial_job).

If execution was successful, you will see a status of 'complete' in the job_log

This simple example demonstrates how the JobController allows
you to drop a payload into a prebuilt management infrastructure. 

To better appreciate what this infrastructure provides, lets explore
error handling. The easiest way to to this is to add a task that
raises an exception.

'''

class BrokenTask(BaseTransformer):

    _allow_empty_df = True          # allow this task to run even if it receives no incoming data
    produces_output_items = False   # this task does not contribute new data items
    requires_input_items = False    # this task does not require dependent data items

    def __init__(self,name):

        self.name = name

    def execute(self,df):

        result = 'Self inflicted error executing %s' %self.name
        raise RuntimeError(result)

        return result


broken_task = BrokenTask('broken_task')
stages = {
    ('transform',None) :  [job_task,broken_task]
}
my_job = CustomJob('basic_tutorial_job',db=db,stages=stages)
j = JobController(payload=my_job, save_trace_to_file = True )
try:
    j.execute()
except BaseException as e:
    print('Job failed as expected. %s' %e)


'''
This job fails as expected, but it fails in a managed way.
After the exception occurs, the JobController examines metadata to
understand how to deal with the failure. The default behavior is
to abort the job. Before aborting, the JobController writes an
entry to job_log, writes the trace and writes the log.

Look at the job_log table. You should see a row with a status of
'aborted'. Look at the trace. You will see details of the job -
including the error and a stack trace.

You can also configure a task or job to allow the JobController
to complete the job even if one or more tasks fail.

To configure the job to allow errors in any stage use the
keyword argument _abort_on_fail

'''

j = JobController(payload=my_job, save_trace_to_file = True, _abort_on_fail=False)
j.execute()

'''

There are however generally mandatory tasks that must complete successfully for a job
to complete. It is more common to configure the tasks than the job.

'''

class BrokenTask(BaseTransformer):

    _abort_on_fail = False          # allow the job to complete even though this task fails
    _allow_empty_df = True          # allow this task to run even if it receives no incoming data
    produces_output_items = False   # this task does not contribute new data items
    requires_input_items = False    # this task does not require dependent data items

    def __init__(self,name):

        self.name = name

    def execute(self,df):

        result = 'Self inflicted error executing %s ****' %self.name
        raise RuntimeError(result)

        return result


broken_task = BrokenTask('broken_task')
stages = {
    ('transform',None) :  [job_task,broken_task]
}
my_job = CustomJob('basic_tutorial_job',db=db,stages=stages)
j = JobController(payload=my_job, save_trace_to_file = True )
j.execute()

'''
The status of the job in the job_log is 'complete' even though BrokenTask raised an exception.

The trace still contains the details of the error:
    {
        "created_by": "broken_task",
        "text": "Execution of stage broken_task failed.  Completed stage.",
        "produces_output_items": "False",
        "timestamp": "2019-08-07 20:35:45.091931",
        "can_proceed": "True",
        "exception_type": "RuntimeError",
        "stack_trace": "Traceback (most recent call last):\n  File \"C:\\Users\\mike\\Documents\\GitHub\\functions\\iotfunctions\\pipeline.py\", line 2074, in execute_stages\n    end_ts=end_ts)\n  File \"C:\\Users\\mike\\Documents\\GitHub\\functions\\iotfunctions\\pipeline.py\", line 2187, in execute_stage\n    result = stage.execute(df=df)\n  File \"C:/Users/mike/Documents/GitHub/functions/scripts/tutorial_job_controller.py\", line 203, in execute\n    raise RuntimeError(result)\nRuntimeError: Self inflicted error executing broken_task ****\n",
        "output_items": "None",
        "discard_prior_data": "False",
        "elapsed_time": "0.0",
        "exception": "Self inflicted error executing broken_task",
        "updated": "2019-08-07 20:35:45.092928",
        "cumulative_usage": "0"
    },

'''

'''
When we introduced the broken task, we included this task as an additional stage
in the stages_dict that we used to initialize the JobController object

The JobController is designed for complex multi-stage jobs, where individual
stages are executed as a pipeline that operates on a shared dataset.

There is a special class of task that is used a data source to collect data
for the shared dataset.

Let's build a task that will be used as data source in a multistage job.

'''

class MyDataSource(BaseTransformer):

    _abort_on_fail = True          # allow the job to complete even though this task fails
    _allow_empty_df = True          # allow this task to run even if it receives no incoming data
    produces_output_items = True   # this task does not contribute new data items
    requires_input_items = False    # this task does not require dependent data items

    def __init__(self,name,output_column_names):

        self.name = name
        self.output_column_names = output_column_names

        # since this task produces outputs, it needs to
        # expose those outputs to the JobController
        # it does this through an instance variable
        # called _output_list

        # We will collect the names of the outputs
        # using the argument 'output_column_names'
        self._output_list = self.output_column_names

    def execute(self,df):

        # implement a get_data() method that returns a dataframe
        # the names of the columns are derived from the
        #output_column_names argument

        data = {self.output_column_names[0]: [1,2],
                self.output_column_names[1]: [3,4]}
        df = pd.DataFrame(data=data)
        df['evt_timestamp'] = dt.datetime.utcnow()
        df['id'] = 'A01'
        df = df.set_index(['evt_timestamp','id'])

        return df

'''

Since our job will now have some data, let's change our basic task so that it contributes data too

'''

class CustomTask(BaseTransformer):

    _allow_empty_df = True          # allow this task to run even if it receives no incoming data
    produces_output_items = True    # contributes new data items
    requires_input_items = False    # this task does not require dependent data items

    def __init__(self,name):

        self.name = name
        # This class needs an _output_list instance variable too
        # In this case, the task outputs a single data item
        # The name of this data item is derived from the name of the task

        self._output_list = ['%s_output' %self.name]

    def execute(self,df):

        result = '**** Executed task %s ****' %self.name

        return result

job_task = CustomTask('my_task')
# need to decide what we want to call the data items produced by our data source
# we will call them x1 and x2
sample_data = MyDataSource(name='sample_data',output_column_names=['x1','x2'])

stages = {
    ('get_data',None) : [sample_data],
    ('transform',None) :  [job_task]
}
my_job = CustomJob('basic_tutorial_job',db=db,stages=stages)

'''
By default the JobController writes output data to a Key Value Pair table in DB2.
This write operation requires more metadata than we have defined in this simple
job. Instead we will reroute the output data to a file using the data_writer keyword arg.
'''

j = JobController(payload=my_job, save_trace_to_file = True, data_writer = DataWriterFile )
j.execute()

'''
After execution, you will see a new csv file in the script folder
This is the output that you will see in this file

evt_timestamp	    id	    x1	x2	    my_task_output
2019-08-08 01:57	A01	    1	3	    **** Executed task my_task ****
2019-08-08 01:57	A01	    2	4	    **** Executed task my_task ****

The data source supplied two rows of data along with data items x1 and x2
The task supplied a new column called "my task output'

Earlier on you saw the JobController has built in error handling.
Now that we have data, lets look at the data we get from the "broken_task"

First we will have to modify BrokenTask to produce outputs

'''

class BrokenTask(BaseTransformer):

    _abort_on_fail = False          # allow the job to complete even though this task fails
    _allow_empty_df = True          # allow this task to run even if it receives no incoming data
    produces_output_items = True   # this task contributes new data items
    requires_input_items = False    # this task does not require dependent data items

    def __init__(self,name):

        self.name = name
        self._output_list = ['%s_output' % self.name]

    def execute(self,df):

        result = 'Self inflicted error executing %s ****' %self.name
        raise RuntimeError(result)

        return result


broken_task = BrokenTask('broken_task')
stages = {
    ('get_data',None) : [sample_data],
    ('transform',None) :  [job_task, broken_task]
}
my_job = CustomJob('basic_tutorial_job',db=db,stages=stages)
j = JobController(payload=my_job, save_trace_to_file = True, data_writer = DataWriterFile )
j.execute()

'''
The output will now look as follows:

evt_timestamp	    id	    x1	x2	    my_task_output                      broken_task_output
2019-08-08 01:57	A01	    1	3	    **** Executed task my_task ****     <null>
2019-08-08 01:57	A01	    2	4	    **** Executed task my_task ****     <null>

After the broken_task failed during execution, the JobController added a blank column
called 'broken_task_output'

'''

'''
The JobController can handle complex jobs with dependencies between tasks

Let's create a new task that processes the result of my_task and broken_task_output
'''

class DependentTask(BaseTransformer):

    _abort_on_fail = False          # allow the job to complete even though this task fails
    _allow_empty_df = False          # do not allow this task to run even if it receives no incoming data
    produces_output_items = True   # this task contributes new data items
    requires_input_items = True    # this task requires dependent data items

    def __init__(self,name,input_colums):

        self.name = name
        self.input_columns = input_colums

        # tasks that act on data that was produced by other tasks must
        # declare their dependencies
        # in this example, dependencies are declared using an argument input_columns

        self._input_set = set(self.input_columns)
        self._output_list = [self.name]

    def execute(self,df):

        # replace null values with a string
        # concatenate the input columns

        df[self.name] = ''
        for i in self.input_columns:
            df[self.name] = df[self.name] + df[i].fillna('null')

        return df


dependent_task = DependentTask('dependent_output',['my_task_output','broken_task_output'])
stages = {
    ('get_data',None) : [sample_data],
    ('transform',None) :  [job_task, broken_task, dependent_task]
}
my_job = CustomJob('basic_tutorial_job',db=db,stages=stages)
j = JobController(payload=my_job, save_trace_to_file = True, data_writer = DataWriterFile )
j.execute()

'''
The output will now look as follows:

evt_timestamp	    id	    x1	x2	    my_task_output                      broken_task_output      dependent_output
2019-08-08 01:57	A01	    1	3	    **** Executed task my_task ****     <null>                  **** Executed task my_task ****null
2019-08-08 01:57	A01	    2	4	    **** Executed task my_task ****     <null>                  **** Executed task my_task ****null

The dependent_task ran after the job_task and broken_task, combining the results of these two tasks

'''

'''
So far we have used the JobController to read and transform low level input data.
The JobController can also create summary data. When creating summary data,
you need to indicate the level of granularity for the summary by building a
Granularity object. 

The daily grain, summaries by "id" at the "day" grain.

'''

daily = Granularity(
    name = 'daily',
    freq = '1D',                 # pandas frequency string
    timestamp= 'evt_timestamp', # build time aggregations using this datetime col
    entity_id = 'id',            # aggregate by id
    dimensions = None,
    entity_name = None
)

'''
You also need a DataAggregator object that contains the metadata about
which aggregation functions are applied to which data items, which
input items are needed for aggregation and names the output items
produced by the aggregation.

In this example we will supply a prebuilt DataAggregator object. You
can also have the JobController build a DataAggregator object for you
using a lists of "simple_aggregate" and "complex_aggregate" functions.

'''

day_agg = DataAggregator(
    name= 'day_agg',
    granularity = daily,
    agg_dict = {
        'x1' : ['sum'],
        'x2' : ['min','max']
    },
    input_items = ['x1','x2'],
    output_items = ['x1','x2_min','x2_max']
    )

stages = {
    ('get_data',None) : [sample_data],
    ('transform',None) :  [job_task],
    ('aggregate',daily) : day_agg
}

my_job = CustomJob('basic_tutorial_job',db=db,stages=stages)
j = JobController(payload=my_job, save_trace_to_file = True, data_writer = DataWriterFile )
j.execute()

'''
After executing the JobController object you will find two new csv files in the scripts folder.
1) The input level file that contains the input data and calcs performed at the input level
2) A summary level file that contains the aggregation results

This is what the aggregation results look like:
id	evt_timestamp	x1	x2_min	x2_max
A01	8/8/2019 0:00	3	4 	    3

'''


