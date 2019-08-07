import json
import logging
from iotfunctions.pipeline import JobController
from iotfunctions.db import Database
from iotfunctions.base import BaseTransformer
from iotfunctions.enginelog import EngineLogging

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
'''





