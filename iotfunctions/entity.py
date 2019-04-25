# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

'''
The entity module contains sample entity types
'''

import logging
import datetime as dt
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func
from .metadata import EntityType, Granularity
from . import bif
from . import ui
from . import aggregate as agg

logger = logging.getLogger(__name__)

SAMPLE_FN_1 = '''
def f(df,parameters):
    series = df[parameters["input_items"][0]]
    out = series*parameters['param_1']
    return(out)
'''


class EmptyEntityType(EntityType):
    is_entity_type = True
    def __init__(self,name,db,db_schema=None,timestamp='evt_timestamp',
                 description = ''):
        args = []
        kw = {'_timestamp' : 'evt_timestamp',
              '_db_schema' : db_schema,
              'description' : description
              }        
        super().__init__(name,db, *args,**kw)

class Boiler(EntityType):

    def __init__(self,name,db,db_schema=None,timestamp='evt_timestamp',
                 description = 'Industrial boiler'):
        args = []
        args.append(Column('company_code',String(50)))
        args.append(Column('temp_set_point',Float()))
        args.append(Column('temperature',Float()))
        args.append(Column('pressure',Float()))
        args.append(Column('input_flow_rate',Float()))
        args.append(Column('output_flow_rate',Float()))
        args.append(Column('discharge_rate',Float()))
        args.append(Column('fuel_flow_rate',Float()))
        args.append(Column('air_flow_rate',Float()))
        args.append(bif.IoTEntityDataGenerator(ids=None))
        args.append(bif.TimestampCol(dummy_items = ['pressure'], output_item = 'timestamp_col'))
        kw = {'_timestamp' : timestamp,
              '_db_schema' : db_schema,
              'description' : description
              }
        
        super().__init__(name,db, *args,**kw)
        
class TestBed(EntityType):

    def __init__(self,name,db,db_schema=None,timestamp='evt_timestamp',
                 description = 'Test entity type'):
        args = []
        args.append(Column('str_1',String(50)))
        args.append(Column('str_2',String(50)))
        args.append(Column('x_1',Float()))
        args.append(Column('x_2',Float()))
        args.append(Column('x_3',Float()))
        args.append(Column('date_1',DateTime))
        args.append(Column('date_2',DateTime))
        args.append(ui.UISingle(name='alpha',
                 description = 'Sample single valued parameter',
                 datatype= float,
                 default = 0.3)
                )
        args.append(bif.IoTShiftCalendar(
                shift_definition=None,
                period_start_date = 'shift_start_date',
                period_end_date = 'shift_end_date',
                shift_day = 'shift_day',
                shift_id = 'shift_id'
                ))
        args.append(bif.IoTEntityDataGenerator(
                ids=['A01','A02','A03','A04','A05','B01']
                ))
        args.append(bif.IoTDeleteInputData(
                dummy_items=[],
                older_than_days=5,
                output_item='delete_done'
                ))
        args.append(bif.IoTDropNull(
                exclude_items = ['str_1','str_2'],
                drop_all_null_rows = True,
                output_item = 'nulls_dropped'
                ))
        args.append(bif.IoTEntityFilter(
                entity_list = ['A01','A02','A03']
                ))
        args.append(bif.IoTAlertExpression(
                input_items=['x_1','x_2'],
                expression = "df['x_1']>3*df['x_2']",
                alert_name = 'alert_1'
                ))
        args.append(bif.IoTAlertOutOfRange(
                input_item = 'x_1',
                lower_threshold=.25,
                upper_threshold= 3,
                output_alert_upper = 'alert_2_upper',
                output_alert_lower = 'alert_2_lower'
                ))
        args.append(bif.IoTAlertHighValue(
                input_item = 'x_1',
                upper_threshold=3,
                alert_name = 'alert_3'
                ))
        args.append(bif.IoTAlertLowValue(
                input_item = 'x_1',
                lower_threshold=0.25,
                alert_name = 'alert_4'
                ))
        args.append(bif.RandomNull(
                input_items = ['x_1','x_2','str_1','str_2','date_1','date_2'],
                output_items = ['x_1_null','x_2_null','str_1_null',
                               'str_2_null','date_1_null','date_2_null'], 
                ))
        args.append(bif.Coalesce(
                data_items = ['x_1_null','x_2_null'],
                output_item = 'x_1_2'
                ))
        args.append(bif.IoTConditionalItems(
                conditional_expression = "df['alert_1']==True",
                conditional_items = ['x_1','x_2'],
                output_items = ['x_1_alert_1','x_2_alert_1']
                ))
        args.append(bif.TimestampCol(
                dummy_items = None,
                output_item = 'timestamp_col'))
        args.append(bif.DateDifference(
                date_1='date_1',
                date_2='date_2',
                num_days='date_diff_2_1'))
        args.append(bif.DateDifferenceReference(
                date_1='timestamp_col',
                ref_date=dt.datetime.utcnow(),
                num_days = 'date_diff_ts_now'
                ))
        args.append(bif.IoTExpression(
                expression = 'df["x_1"]*c["alpha"]',
                output_name = 'x1_alpha'
                ))
        '''
        args.append(bif.IoTExpression(
                expression = 'df["x1"]+df["x1"]+df["x3"]',
                output_name = 'x_4_invalid'
                ))        
        args.append(bif.IoTExpression(
                expression = 'df["x_1"]*c["not_existing_constant"]',
                output_name = 'x1_non_existing_constant'
                ))        
        '''
        args.append(bif.IoTExpression(
                expression = 'df["x_1"]+df["x_1"]+df["x_3"]',
                output_name = 'x_4'
                ))
        args.append(bif.IoTIfThenElse(
                conditional_expression = 'df["x_1"]>df["x_2"]',
                true_expression = 'df["x_1"]',
                false_expression = 'df["x_2"]',
                output_item = 'x_1_or_2'
                ))
        args.append(bif.PythonFunction(
                function_code = SAMPLE_FN_1,
                input_items = ['x_1'],
                parameters = {'param_1': 3},
                output_item = 'fn_out',
                ))
        args.append(Granularity(
                 name = 'day',
                 dimensions = [],
                 timestamp = 'evt_timestamp',
                 freq = '1D',
                 entity_name = name,
                 entity_id = 'deviceid'
                 )
                )
        args.append(agg.AggregateItems(
                input_items = ['x_1','x_2'],
                aggregation_function = 'sum',
                output_items = ['x_1_sum_day','x_2_sum_day']))
        
        kw = {'_timestamp' : timestamp,
              '_db_schema' : db_schema,
              'description' : description
              }
        
        super().__init__(name,db, *args,**kw)        
        
    
    
    
    
    