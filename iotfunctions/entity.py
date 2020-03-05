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
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, func, SmallInteger

from . import metadata
from . import bif
from . import ui
from . import estimator as est

logger = logging.getLogger(__name__)

SAMPLE_FN_1 = '''
def f(df,parameters):
    series = df[parameters["input_items"][0]]
    out = series*parameters['param_1']
    return(out)
'''


def make_sample_entity(db, schema=None, name='as_sample_entity', register=False, data_days=1, freq='1min',
                       entity_count=5, float_cols=5, string_cols=2, bool_cols=2, date_cols=2, drop_existing=True,
                       include_generator=True):
    """
    Build a sample entity to use for testing.

    Parameters
    ----------
    db : Database object
        database where entity resides.
    schema: str (optional)
        name of database schema. Will be placed in the default schema if none specified.
    name: str (optional)
        by default the entity type will be called as_sample_entity
    register: bool
        register so that it is available in the UI
    data_days : number
        Number of days of sample data to generate
    float_cols: list
        Name of float columns to add
    string_cols : list
        Name of string columns to add
    """

    if entity_count is None:
        entities = None
    else:
        entities = ['E%s' % x for x in list(range(entity_count))]

    if isinstance(float_cols, int):
        float_cols = ['float_%s' % x for x in list(range(float_cols))]
    if isinstance(string_cols, int):
        string_cols = ['string_%s' % x for x in list(range(string_cols))]
    if isinstance(date_cols, int):
        date_cols = ['date_%s' % x for x in list(range(date_cols))]
    if isinstance(bool_cols, int):
        bool_cols = ['bool_%s' % x for x in list(range(bool_cols))]

    if drop_existing:
        db.drop_table(table_name=name, schema=schema)

    float_cols = [Column(x.lower(), Float()) for x in float_cols]
    string_cols = [Column(x.lower(), String(255)) for x in string_cols]
    bool_cols = [Column(x.lower(), SmallInteger) for x in bool_cols]
    date_cols = [Column(x.lower(), DateTime) for x in date_cols]

    functions = []
    if include_generator:
        sim = {'freq': freq}
        generator = bif.EntityDataGenerator(ids=entities, parameters=sim)
        functions.append(generator)

    cols = []
    cols.extend(float_cols)
    cols.extend(string_cols)
    cols.extend(bool_cols)
    cols.extend(date_cols)

    entity = metadata.BaseCustomEntityType(name=name, db=db, columns=cols, functions=functions, generate_days=data_days,
                                           drop_existing=drop_existing, db_schema=schema)

    if register:
        entity.register(publish_kpis=True, raise_error=True)
    return entity


class EmptyEntityType(metadata.EntityType):

    def __init__(self, name, db, db_schema=None, timestamp='evt_timestamp', description=''):
        args = []
        kw = {'_timestamp': 'evt_timestamp', '_db_schema': db_schema, 'description': description}
        super().__init__(name, db, *args, **kw)


class SampleBlankEntity(metadata.BaseCustomEntityType):
    '''
    This sample shows simulated time series data for an industrial boiler.
    It demostrates how to perform Monte Carlo simulation. It also
    shows how to apply heuristics to detect leaks.
    '''

    def __init__(self, name, db, db_schema=None, description=None, generate_days=10, drop_existing=False):
        # constants
        constants = []

        # granularities
        granularities = []

        columns = []
        # columns

        functions = []
        # simulation settings
        sim = {'data_item_mean': {}, 'drop_existing': False}

        # dimension columns
        dimension_columns = []

        super().__init__(name=name, db=db, constants=constants, granularities=granularities, columns=columns,
                         functions=functions, dimension_columns=dimension_columns, output_items_extended_metadata={},
                         generate_days=generate_days, drop_existing=drop_existing, description=description,
                         db_schema=db_schema)


class Boiler(metadata.BaseCustomEntityType):
    '''
    This sample shows simulated time series data for an industrial boiler.
    It demostrates how to perform Monte Carlo simulation. It also
    shows how to apply heuristics to detect leaks.
    '''

    def __init__(self, name, db, db_schema=None, description=None, generate_days=10, drop_existing=False):
        # constants
        constants = []

        # granularities
        granularities = []

        columns = []
        # columns
        columns.append(Column('company_code', String(50)))
        columns.append(Column('temp_set_point', Float()))
        columns.append(Column('pressure', Float()))
        columns.append(Column('input_flow_rate', Float()))
        columns.append(Column('fuel_flow_rate', Float()))
        columns.append(Column('air_flow_rate', Float()))

        functions = []
        # simulation settings
        sim = {'data_item_mean': {'temp_set_point': 200, 'pressure': 400, 'input_flow_rate': 10, 'fuel_flow_rate': 5,
                                  'air_flow_rate': 2}, 'drop_existing': False}

        generator = bif.EntityDataGenerator(ids=None, parameters=sim)
        functions.append(generator)

        # temperature depends on set point
        functions.append(
            bif.RandomNoise(input_items=['temp_set_point'], standard_deviation=1, output_items=['temperature']))
        # discharge percent is a uniform random value
        functions.append(bif.RandomUniform(min_value=0.1, max_value=0.2, output_item='discharge_perc'))
        # discharge_rate
        functions.append(bif.PythonExpression(expression='df["input_flow_rate"] * df["discharge_perc"]',
                                              output_name='discharge_flow_rate'))
        # output_flow_rate
        functions.append(bif.PythonExpression(expression='df["input_flow_rate"] * df["discharge_flow_rate"]',
                                              output_name='output_flow_rate'))

        # roughing out design of entity with fake recommendations
        functions.append(bif.RandomDiscreteNumeric(discrete_values=[0.001, 0.001, 0.001, 0.5, 0.7],
                                                   probabilities=[0.9, 0.05, 0.02, 0.02, 0.01], output_item='p_leak'))

        # dimension columns
        dimension_columns = [Column('firmware', String(50)), Column('manufacturer', String(50)),
                             Column('devicetype', String(50)), Column('evt_timestamp', DateTime)]

        super().__init__(name=name, db=db, constants=constants, granularities=granularities, columns=columns,
                         functions=functions, dimension_columns=dimension_columns, output_items_extended_metadata={},
                         generate_days=generate_days, drop_existing=drop_existing, description=description,
                         db_schema=db_schema)


class BuildingWorkstation(metadata.BaseCustomEntityType):
    '''
    Sample entity type for monitoring a building. Monitor comfort levels, energy
    consumption and occupany.
    '''

    def __init__(self, name, db, db_schema=None, description=None, generate_days=10, drop_existing=False):
        # constants
        constants = []

        physical_name = name.lower()

        # granularities
        granularities = []

        # columns
        columns = []
        columns.append(Column('temperature', Float()))
        columns.append(Column('motion', Float()))
        columns.append(Column('humidity', Float()))
        columns.append(Column('co2', Float()))

        # dimension columns
        dimension_columns = []
        dimension_columns.append(Column('building', String(50)))
        dimension_columns.append(Column('floor', String(50)))
        dimension_columns.append(Column('zone', String(50)))

        # functions
        functions = []
        # simulation settings
        sim = {'freq': '5min', 'auto_entity_count': 100,
               'data_item_mean': {'temperature': 22, 'motion': 1, 'humidity': 50, 'co2': 1},
               'data_item_domain': {'building': ['Riverside', 'Collonade', 'Mariners Way'], 'floor': [1, 2, 3, 4, 5],
                                    'zone': ['NE', 'NW', 'SE', 'SW']}, 'drop_existing': False}
        generator = bif.EntityDataGenerator(ids=None, parameters=sim)
        functions.append(generator)

        # data type for operator cannot be inferred automatically
        # state it explicitly

        output_items_extended_metadata = {}

        super().__init__(name=name, db=db, constants=constants, granularities=granularities, columns=columns,
                         functions=functions, dimension_columns=dimension_columns,
                         output_items_extended_metadata=output_items_extended_metadata, generate_days=generate_days,
                         drop_existing=drop_existing, description=description, db_schema=db_schema)


class Robot(metadata.BaseCustomEntityType):
    '''
    Sample entity type based on data commonly available for industrial robots.
    This sample illustrates the ability to combine timeseries sensor data
    with other data. It shows how to calculate activity durations from an activity
    log, map timestamps to shifts time align changes to slowly changing dimensions
    '''

    def __init__(self, name, db, db_schema=None, description=None, generate_days=10, drop_existing=False):
        physical_name = name.lower()

        # constants
        constants = []

        # granularities
        granularities = []

        # columns
        columns = []
        columns.append(Column('plant_code', String(50)))
        columns.append(Column('tool_type', Float()))
        columns.append(Column('acc', Float()))
        columns.append(Column('torque', Float()))
        columns.append(Column('load', Float()))
        columns.append(Column('speed', Float()))
        columns.append(Column('travel_time', Float()))

        # functions
        functions = []
        # simulation settings
        sim = {'freq': '5min', 'scd_frequency': '90min', 'activity_frequency': '4H',
               'data_item_mean': {'torque': 12, 'load': 375, 'load_rating': 400, 'speed': 3, 'travel_time': 1},
               'data_item_domain': {'axes': [1, 2, 3], 'tool_type': [907, 803, 691, 909]},
               'scds': {'operator': ['Fred K', 'Mary J', 'Jane S', 'Jeff H', 'Harry L', 'Steve S']},
               'activities': {'maintenance': ['scheduled_maint', 'unscheduled_maint', 'firmware_upgrade', 'testing'],
                              'setup': ['normal_setup', 'reconfiguration'], }, 'drop_existing': False}
        generator = bif.EntityDataGenerator(ids=None, parameters=sim)
        functions.append(generator)

        functions.append(bif.PythonExpression(expression='df["torque"]*df["load"]', output_name='work_performed'))

        functions.append(bif.ShiftCalendar(shift_definition={"1": [5.5, 14], "2": [14, 21], "3": [21, 29.5]},
                                           period_start_date='shift_start_date', period_end_date='shift_end_date',
                                           shift_day='shift_day', shift_id='shift_id'))

        functions.append(bif.SCDLookup(table_name='%s_scd_operator' % physical_name, output_item='operator', ))

        functions.append(bif.ActivityDuration(table_name='%s_maintenance' % physical_name,
                                              activity_codes=['scheduled_maint', 'unscheduled_maint',
                                                              'firmware_upgrade', 'testing'],
                                              activity_duration=['scheduled_maint', 'unscheduled_maint',
                                                                 'firmware_upgrade', 'testing'],
                                              additional_items=['start_date'],
                                              additional_output_names=['maintenance_start_date']))

        functions.append(bif.RandomDiscreteNumeric(discrete_values=[0, 1, 2, 3, 4, 5, 6, 7, 8],
                                                   probabilities=[0.2, 0.05, 0.05, .2, .3, 0.05, 0.05, 0.05, 0.05],
                                                   output_item='completed_movement_count'))

        functions.append(
            bif.RandomDiscreteNumeric(discrete_values=[0, 1, 2, 4, 5], probabilities=[.8, 0.05, 0.05, 0.05, 0.05],
                                      output_item='abnormal_stop_count'))

        functions.append(
            bif.RandomDiscreteNumeric(discrete_values=[0, 3, 5, 9, 12], probabilities=[.9, 0.25, 0.25, 0.25, 0.25],
                                      output_item='safety_stop_count'))

        functions.append(
            bif.RandomUniform(min_value=0.8, max_value=0.95, output_item='percent_meeting_target_duration'))

        # data type for operator cannot be infered automatically
        # state it explicitly

        output_items_extended_metadata = {'operator': {"dataType": "LITERAL"}}

        # dimension columns
        dimension_columns = [Column('firmware', String(50)), Column('manufacturer', String(50)),
                             Column('load_rating', Float()), Column('axes', Float()), Column('stats_acc', Float()),
                             Column('devicetype', String(50)), Column('evt_timestamp', DateTime)]

        super().__init__(name=name, db=db, constants=constants, granularities=granularities, columns=columns,
                         functions=functions, dimension_columns=dimension_columns,
                         output_items_extended_metadata=output_items_extended_metadata, generate_days=generate_days,
                         drop_existing=drop_existing, description=description, db_schema=db_schema)


class PackagingHopper(metadata.BaseCustomEntityType):
    '''
    This sample demonstrates anomaly detection on simulated data from a cereal
    packaging plant.
    '''

    def __init__(self, name, db, db_schema=None, description=None, generate_days=10, drop_existing=False):
        constants = []
        granularities = []
        columns = []
        columns.append(Column('company_code', String(50)))
        columns.append(Column('product_code', String(50)))
        columns.append(Column('ambient_temp', Float()))
        columns.append(Column('ambient_humidity', Float()))
        functions = []

        # simulation settings

        sim = {'data_item_mean': {'ambient_temp': 20, 'ambient_humidity': 60},
               'data_item_sd': {'ambient_temp': 5, 'ambient_humidity': 5}, 'drop_existing': False}

        generator = bif.EntityDataGenerator(ids=None, parameters=sim)
        functions.append(generator)
        # fill rate depends on temp
        functions.append(
            bif.PythonExpression(expression='502 + 9 * df["ambient_temp"]/20', output_name='dispensed_mass_predicted'))
        functions.append(bif.RandomNoise(input_items=['dispensed_mass_predicted'], standard_deviation=0.5,
                                         output_items=['dispensed_mass_actual']))
        # difference between prediction and actual
        functions.append(bif.PythonExpression(expression=('(df["dispensed_mass_predicted"]-'
                                                          ' df["dispensed_mass_actual"]).abs()'),
                                              output_name='prediction_abs_error'))
        ''' alert
        functions.append(bif.AlertHighValue(input_item='prediction_abs_error', upper_threshold=3,
                                            alert_name='anomaly_in_fill_detected', Severity='High', Status='New'))'''
        # dimension columns

        dimension_columns = [Column('firmware', String(50)), Column('manufacturer', String(50)),
                             Column('plant', String(50)), Column('line', String(50)), Column('devicetype', String(50)),
                             Column('evt_timestamp', DateTime)]

        super().__init__(name=name, db=db, constants=constants, granularities=granularities, columns=columns,
                         functions=functions, dimension_columns=dimension_columns, generate_days=generate_days,
                         drop_existing=drop_existing, description=description, db_schema=db_schema)


class SourdoughLeavening(metadata.BaseCustomEntityType):
    '''
    This sample demostrates using AI to make recommendations about the
    leavening process during the production of bread
    '''

    def __init__(self, name, db, db_schema=None, description=None, generate_days=10, drop_existing=False):
        constants = []
        granularities = []

        columns = []
        columns.append(Column('company_code', String(50)))
        columns.append(Column('product_code', String(50)))
        columns.append(Column('ambient_temp', Float()))
        columns.append(Column('ambient_humidity', Float()))

        functions = []
        # simulation settings
        sim = {'data_item_mean': {'ambient_temp': 20, 'ambient_humidity': 60},
               'data_item_sd': {'ambient_temp': 5, 'ambient_humidity': 5}, 'drop_existing': False}

        generator = bif.EntityDataGenerator(ids=None, parameters=sim)
        functions.append(generator)

        functions.append(bif.PythonExpression(expression='df["ambient_temp"]*df["ambient_humidity"]/50',
                                              output_name='adjusted_temp'))

        functions.append(bif.RandomNormal(mean=6, standard_deviation=1, output_item='predicted_hours_till_bake'))

        functions.append(bif.RandomNoise(input_items=['predicted_hours_till_bake'], standard_deviation=0.5,
                                         output_items=['target_hours_till_bake']))

        functions.append(bif.RandomChoiceString(
            domain_of_values=['bake now', 'wait for futher instructions', 'refrigerate now', 'place in warmer location',
                              'discard dough'], probabilities=[1, 10, 0.2, 1, 0.2], output_item='recommendation'))

        # dimension columns
        dimension_columns = [Column('firmware', String(50)), Column('manufacturer', String(50)),
                             Column('plant', String(50)), Column('line', String(50)), Column('devicetype', String(50)),
                             Column('evt_timestamp', DateTime)]

        super().__init__(name=name, db=db, constants=constants, granularities=granularities, columns=columns,
                         functions=functions, dimension_columns=dimension_columns, generate_days=generate_days,
                         drop_existing=drop_existing, description=description, db_schema=db_schema)


class TestBed(metadata.BaseCustomEntityType):
    '''
    Test entity type. Excercises a number of functions.
    '''

    def __init__(self, name, db, db_schema=None, description=None, generate_days=0, drop_existing=False):
        columns = []
        columns.append(Column('str_1', String(50)))
        columns.append(Column('str_2', String(50)))
        columns.append(Column('x_1', Float()))
        columns.append(Column('x_2', Float()))
        columns.append(Column('x_3', Float()))
        columns.append(Column('date_1', DateTime))
        columns.append(Column('date_2', DateTime))

        day = metadata.Granularity(name='day', dimensions=[], timestamp='evt_timestamp', freq='1D', entity_name=name,
                                   entity_id='deviceid')
        granularities = [day]

        constants = []
        constants.append(
            ui.UISingle(name='alpha', description='Sample single valued parameter', datatype=float, default=0.3))

        functions = []

        generator = bif.EntityDataGenerator(ids=None)
        functions.append(generator)

        functions.append(bif.ShiftCalendar(shift_definition=None, period_start_date='shift_start_date',
                                           period_end_date='shift_end_date', shift_day='shift_day',
                                           shift_id='shift_id'))
        functions.append(bif.EntityDataGenerator(ids=['A01', 'A02', 'A03', 'A04', 'A05', 'B01']))
        functions.append(bif.DeleteInputData(dummy_items=['x_1'], older_than_days=5, output_item='delete_done'))
        functions.append(
            bif.DropNull(exclude_items=['str_1', 'str_2'], drop_all_null_rows=True, output_item='nulls_dropped'))
        functions.append(bif.EntityFilter(entity_list=['A01', 'A02', 'A03']))
        functions.append(
            bif.AlertExpression(input_items=['x_1', 'x_2'], expression="df['x_1']>3*df['x_2']", alert_name='alert_1'))
        functions.append(bif.AlertOutOfRange(input_item='x_1', lower_threshold=.25, upper_threshold=3,
                                             output_alert_upper='alert_2_upper', output_alert_lower='alert_2_lower'))
        functions.append(bif.AlertHighValue(input_item='x_1', upper_threshold=3, alert_name='alert_3', Severity='Medium', Status='New'))
        functions.append(bif.AlertLowValue(input_item='x_1', lower_threshold=0.25, alert_name='alert_4'))
        functions.append(bif.RandomNull(input_items=['x_1', 'x_2', 'str_1', 'str_2', 'date_1', 'date_2'],
                                        output_items=['x_1_null', 'x_2_null', 'str_1_null', 'str_2_null', 'date_1_null',
                                                      'date_2_null'], ))
        functions.append(bif.Coalesce(data_items=['x_1_null', 'x_2_null'], output_item='x_1_2'))
        functions.append(
            bif.ConditionalItems(conditional_expression="df['alert_1']==True", conditional_items=['x_1', 'x_2'],
                                 output_items=['x_1_alert_1', 'x_2_alert_1']))
        functions.append(bif.TimestampCol(dummy_items=None, output_item='timestamp_col'))
        functions.append(bif.DateDifference(date_1='date_1', date_2='date_2', num_days='date_diff_2_1'))
        functions.append(bif.DateDifferenceReference(date_1='timestamp_col', ref_date=dt.datetime.utcnow(),
                                                     num_days='date_diff_ts_now'))
        functions.append(bif.PythonExpression(expression='df["x_1"]*c["alpha"]', output_name='x1_alpha'))
        functions.append(bif.PythonExpression(expression='df["x1"]+df["x1"]+df["x3"]', output_name='x_4_invalid'))
        functions.append(bif.PythonExpression(expression='df["x_1"]*c["not_existing_constant"]',
                                              output_name='x1_non_existing_constant'))
        functions.append(bif.PythonExpression(expression='df["x_1"]+df["x_1"]+df["x_3"]', output_name='x_4'))
        functions.append(bif.IfThenElse(conditional_expression='df["x_1"]>df["x_2"]', true_expression='df["x_1"]',
                                        false_expression='df["x_2"]', output_item='x_1_or_2'))
        functions.append(bif.PythonFunction(function_code=SAMPLE_FN_1, input_items=['x_1'], parameters={'param_1': 3},
                                            output_item='fn_out', ))

        # aggregates
        day_functions = []
        day_functions.append(bif.AggregateItems(input_items=['x_1', 'x_2'], aggregation_function='sum',
                                                output_items=['x_1_sum_day', 'x_2_sum_day']))

        for f in day_functions:
            f.granularity = day.name

        functions.extend(day_functions)

        # dimension columns
        dimension_columns = [Column('firmware', String(50)), Column('manufacturer', String(50)),
                             Column('plant', String(50)), Column('line', String(50))]

        output_items_extended_metadata = {'output_items': {"dataType": "BOOLEAN"}}

        super().__init__(name=name, db=db, constants=constants, granularities=granularities, columns=columns,
                         functions=functions, dimension_columns=dimension_columns, generate_days=generate_days,
                         drop_existing=drop_existing, output_items_extended_metadata=output_items_extended_metadata,
                         description=description, db_schema=db_schema)
