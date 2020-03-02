# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import logging
import datetime as dt
import numpy as np
import json
import importlib
import time
import threading
import warnings
import traceback
from collections import OrderedDict
import pandas as pd
from pandas.api.types import is_bool, is_number, is_string_dtype, is_timedelta64_dtype
from sqlalchemy import Table, Column, Integer, SmallInteger, String, DateTime, Float, func, MetaData
from sqlalchemy.sql.sqltypes import TIMESTAMP, VARCHAR, FLOAT, INTEGER

from . import db as db_module
from .automation import (TimeSeriesGenerator, DateGenerator, MetricGenerator, CategoricalGenerator)
from .pipeline import (CalcPipeline, DropNull, JobController, JobLogNull, Trace, AggregateItems)
from .util import (MemoryOptimizer, build_grouper, categorize_args, reset_df_index)
from .stages import (DataReader, DataWriter, DataWriterFile)
from .exceptions import StageException
import iotfunctions as iotf

logger = logging.getLogger(__name__)


def retrieve_entity_type_metadata(raise_error=True, **kwargs):
    '''
    Get server metadata for entity type
    '''
    db = kwargs['_db']
    # get kpi functions metadata
    meta = db.http_request(object_type='engineInput', object_name=kwargs['logical_name'], request='GET',
                           raise_error=raise_error)
    try:
        meta = json.loads(meta)
    except (TypeError, json.JSONDecodeError):
        meta = None
    if meta is None or 'exception' in meta:
        raise RuntimeError(('API call to server did not retrieve valid entity '
                            ' type properties for %s.' % kwargs['logical_name']))

    if meta['kpiDeclarations'] is None:
        meta['kpiDeclarations'] = []
        logger.warning(('This entity type has no calculated kpis'))

    # cache function catalog metadata in the db object
    function_list = [x['functionName'] for x in meta['kpiDeclarations']]
    db.load_catalog(install_missing=True, function_list=function_list)

    # map server properties
    params = {}
    params['_entity_type_id'] = meta['entityTypeId']
    params['_db_schema'] = meta['schemaName']
    params['name'] = meta['metricsTableName']
    params['_timestamp'] = meta['metricTimestampColumn']
    params['_dimension_table_name'] = meta['dimensionsTable']
    params['_data_items'] = meta['dataItems']

    # constants
    c_meta = db.http_request(object_type='constants', object_name=kwargs['logical_name'], request='GET')
    try:
        c_meta = json.loads(c_meta)
    except (TypeError, json.JSONDecodeError):
        logger.debug(('API call to server did not retrieve valid entity type'
                      ' properties. No properties set.'))
    else:
        for p in c_meta:
            key = p['name']
            if isinstance(p['value'], dict):
                params[key] = p['value'].get('value', p['value'])
            else:
                params[key] = p['value']
            logger.debug('Retrieved server constant %s with value %s', key, params[key])

    params = {**kwargs, **params}

    return (params, meta)


class EntityType(object):
    '''
    Data is organised around Entity Types. Entity Types have one or more
    physical database object for their data. When creating a new Entity Type,
    it will attempt to connect itself to a table of the same name in the
    database. If no table exists the Entity Type will create one.

    Entity types describe the payload of an AS job. A job is built by a
    JobController using functions metadata prepared by the Entity Type.
    Metadata prepared is:

    _functions:

        List of function objects

    _data_items:

        List of data items and all of their metadata such as their
        datatype.

    _granularities_dict:

        Dictionary keyed on granularity name. Contains a granularity object
        that provides access to granularity metadata such as the time
        level and other dimensions involved in the aggregation.

    _schedules_dict:

        Dictionary keyed on a schedule frequency containing other metadata
        about the operations to be run at this frequency, e.g. how many days
        should be backtracked when retrieving daat.


    Entity types may be initialized as client objects for local testing
    or may be loaded from the server. After initialization all of the
    above instance variables will be populated. The metadata looks the same
    regardless of whether the entity type was loaded from the server
    or initialized on the client. The logic to build the metadata is
    different though.

    Parameters
    ----------
    name: str
        Name of the entity type. Use lower case. Will be used as the physical
        database table name so don't use database reserved works of special
        characters.
    db: Database object
        Contains the connection info for the database
    *args:
        Additional positional arguments are used to add the list of SQL Alchemy
        Column objects contained within this table. Similar to the style of a
        CREATE TABLE sql statement. There is no need to specify column names
        if you are using an existing database table as an entity type.
    **kwargs
        Additional keywork args.
        _timestamp: str
            Overide the timestamp column name from the default of 'evt_timestamp'
    '''

    is_entity_type = True
    is_local = False
    auto_create_table = True
    aggregate_complete_periods = True  # align data for aggregation with time grain to avoid partial periods
    log_table = 'KPI_LOGGING'  # deprecated, to be removed
    checkpoint_table = 'KPI_CHECKPOINT'  # deprecated,to be removed
    chunk_size = None  # use job controller default chunk
    default_backtrack = None
    trace_df_changes = True
    drop_existing = False
    # These two columns will be available in the dataframe of a pipeline
    _entity_id = 'deviceid'  # identify the instance
    _timestamp_col = '_timestamp'  # copy of the event timestamp from the index
    # This column will identify an instance in the index
    _df_index_entity_id = 'id'
    # when automatically creating a new dimension, use this suffix
    _auto_dim_suffix = '_auto_dim'
    # when looking for an automatically created numeric index it should be named:
    auto_index_name = '_auto_index_'
    # constants declared as part of an entity type definition
    ui_constants = None
    _functions = None
    # generator
    _scd_frequency = '2D'  # deprecated. Use parameters on EntityDataGenerator
    _activity_frequency = '3D'  # deprecated. Use parameters on EntityDataGenerator
    _start_entity_id = 73000  # deprecated. Use parameters on EntityDataGenerator
    _auto_entity_count = 5  # deprecated. Use parameters on EntityDataGenerator

    # pipeline work variables stages
    _dimension_table = None
    _scd_stages = None
    _custom_calendar = None

    # variabes that will be set when loading from the server
    _entity_type_id = None
    logical_name = None
    _timestamp = 'evt_timestamp'
    _dimension_table_name = None
    _db_connection_dbi = None
    _db_schema = None
    _data_items = None
    tenant_id = None
    _entity_filter_list = None
    _start_ts_override = None
    _end_ts_override = None
    _stages = None
    _schedules_dict = None
    _granularities_dict = None
    _input_set = None
    _output_list = None
    _invalid_stages = None
    _disabled_stages = None
    # processing defaults
    _pre_aggregate_time_grain = None  # aggregate incoming data before processing
    _auto_read_from_ts_table = True  # read new data from designated time series table for the entity
    _pre_agg_rules = None  # pandas agg dictionary containing list of aggregates to apply for each item
    _pre_agg_outputs = None  # dictionary containing list of output items names for each item
    _data_reader = DataReader
    _abort_on_fail = False
    _auto_save_trace = 30
    save_trace_to_file = False
    drop_null_class = DropNull
    enable_downcast = False
    allow_projection_list_trim = True
    _write_usage = False

    # deprecated class variables (to be removed)
    _checkpoint_by_entity = True  # manage a separate checkpoint for each entity instance
    _is_initial_transform = True
    _is_preload_complete = False

    def __init__(self, name, db, *args, **kwargs):

        logger.debug('Initializing new entity type using iotfunctions %s', iotf.__version__)

        try:
            if self.logical_name is None:
                self.logical_name = name
        except AttributeError:
            self.logical_name = name


        if db == None:
            name = 'None'
        elif db.db_type == 'db2':
            name = name.upper()
        else:
            name = name.lower()
        self.name = name
        self.description = kwargs.get('description', None)
        if self.description is None:
            self.description = ''
        else:
            del (kwargs['description'])
        self.activity_tables = {}
        self.scd = {}
        self.db = db
        if self.db is not None:
            self.tenant_id = self.db.tenant_id
        self._system_columns = [self._entity_id, self._timestamp_col, 'logicalinterface_id', 'devicetype', 'format',
                                'updated_utc', self._timestamp]
        self._stage_type_map = self.default_stage_type_map()
        self._custom_exclude_col_from_auto_drop_nulls = []
        self._drop_all_null_rows = True

        if self._scd_stages is None:
            self._scd_stages = []

        if self._data_items is None:
            self._data_items = []
        if self._granularities_dict is None:
            self._granularities_dict = {}

            # additional params set from kwargs
        self.set_params(**kwargs)

        # Start a trace to record activity on the entity type
        self._trace = Trace(object_name=None, parent=self, db=db)

        if self._disabled_stages is None:
            self._disabled_stages = []
        if self._invalid_stages is None:
            self._invalid_stages = []

        if len(self._disabled_stages) > 0 or len(self._invalid_stages) > 0:
            self.trace_append(created_by=self, msg='Skipping disabled and invalid stages', log_method=logger.info,
                              **{'skipped_disabled_stages': [s['functionName'] for s in self._disabled_stages],
                                 'skipped_disabled_data_items': [s['output'] for s in self._disabled_stages],
                                 'skipped_invalid_stages': [s['functionName'] for s in self._invalid_stages],
                                 'skipped_invalid_data_items': [s['output'] for s in self._invalid_stages]})

            # attach to time series table
        if self._db_schema is None:
            logger.warning(('No _db_schema specified in **kwargs. Using'
                            'default database schema.'))

        self._mandatory_columns = [self._timestamp, self._entity_id]

        # separate args into categories
        categories = [('constant', 'is_ui_control', None), ('granularity', 'is_granularity', None),
                      ('function', 'is_function', None), ('column', None, Column)]

        categorized = categorize_args(categories, 'functions', *args)

        cols = list(categorized.get('column', []))
        functions = list(categorized.get('function', []))
        constants = list(categorized.get('constant', []))
        grains = list(categorized.get('granularity', []))

        if self.drop_existing and db is not None and not self.is_local:
            self.drop_tables()

        #  create a database table if needed using cols
        if name is not None and db is not None and not self.is_local:
            try:
                self.table = self.db.get_table(self.name, self._db_schema)
            except KeyError:
                if self.auto_create_table:
                    ts = db_module.TimeSeriesTable(self.name, self.db, *cols, **kwargs)
                    self.table = ts.table
                    self.db.create()
                    msg = 'Create table %s' % self.name
                    logger.info(msg)
                else:

                    msg = ('Database table %s not found. Unable to create'
                           ' entity type instance. Provide a valid table name'
                           ' or use the auto_create_table = True keyword arg'
                           ' to create a table. ' % (self.name))
                    raise ValueError(msg)
            # populate the data items metadata from the supplied columns
            if isinstance(self._data_items, list) and len(self._data_items) == 0:
                self._data_items = self.build_item_metadata(self.table)
        else:
            logger.warning((
                'Created a logical entity type. It is not connected to a real database table, so it cannot perform any database operations.'))

        # add granularities
        for g in grains:
            logger.debug('Adding granularity to entity type: %s', g.name)
            self._granularities_dict[g.name] = g

        #  add constants
        self.ui_constants = constants
        self.build_ui_constants()

        #  _functions
        #  functions may have been provided as kwarg and may be includes as args
        #  compbine all
        if self._functions is None:
            self._functions = []
        self._functions.extend(functions)

        if name is not None and db is not None and not self.is_local:
            db.entity_type_metadata[self.logical_name] = self

        logger.debug(('Initialized entity type %s'), str(self))

    def add_activity_table(self, name, activities, *args, **kwargs):
        '''
        add an activity table for this entity type.

        parameters
        ----------
        name: str
            table name
        activities: list of strs
            activity type codes: these identify the nature of the activity, e.g. PM is Preventative Maintenance
        *args: Column objects
            other columns describing the activity, e.g. materials_cost
        '''

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' are not allowed to add activity tables ')
            raise ValueError(msg)

        kwargs['_activities'] = activities
        kwargs['schema'] = self._db_schema
        # name = name.lower()
        if self.db.db_type == 'db2':
            name = name.upper()
        else:
            name = name.lower()

        table = db_module.ActivityTable(name, self.db, *args, **kwargs)
        try:
            sqltable = self.db.get_table(name, self._db_schema)
        except KeyError:
            table.create()
        self.activity_tables[name] = table

    def add_slowly_changing_dimension(self, property_name, datatype, **kwargs):
        '''
        add a slowly changing dimension table containing a single property for this entity type

        parameters
        ----------
        property_name : str
            name of property, e.g. firmware_version (lower case, no database reserved words)
        datatype: sqlalchemy datatype
        '''

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' are not allowed to add slowly changing dimensions ')
            raise ValueError(msg)

        property_name = property_name.lower()

        name = '%s_scd_%s' % (self.name, property_name)
        kwargs['schema'] = self._db_schema
        if self.db.db_type == 'db2':
            name = name.upper()
        else:
            name = name.lower()
        table = db_module.SlowlyChangingDimension(name=name, database=self.db, property_name=property_name,
                                                  datatype=datatype, **kwargs)

        try:
            self.db.get_table(name, self._db_schema)
        except KeyError:
            table.create()
        self.scd[property_name] = table

    def _add_scd_pipeline_stage(self, scd_lookup):

        self._scd_stages.append(scd_lookup)

    def build_agg_dict_from_meta_list(self, meta_list):

        agg_dict = OrderedDict()
        input_items = set()
        output_items = []

        for f in meta_list:

            input_item = f['input'].get('source')
            output_item = f['output'].get('name')
            aggregate = f['functionName']

            try:
                agg_dict[input_item].append(aggregate)
            except KeyError:
                agg_dict[input_item] = [aggregate]

            input_items.add(input_item)
            output_items.append(output_item)

        return (agg_dict, input_items, output_items)

    def build_arg_metadata(self, obj):

        '''
        Examine the metadata provided by build_ui() to understand more about
        the arguments to a function.

        Place the values of inputs and outputs into 2 dicts
        Return these two dicts in a tuple along with an output_meta dict
        that contains argument values and types

        Build the _input_set and _output list. These describe the set of
        data items required as inputs to a function and the list of data
        items produced by the function.

        '''

        name = obj.__class__.__name__

        try:
            (inputs, outputs) = obj.build_ui()
        except (AttributeError, NotImplementedError) as e:
            try:
                fn_metadata = obj.metadata()
                inputs = fn_metadata.get('input', None)
                outputs = fn_metadata.get('output', None)
            except (AttributeError, KeyError) as ex:
                msg = ('Can\'t get metadata for function %s. Implement the'
                       ' build_ui() method for this function. %s' % (name, str(e)))
                raise NotImplementedError(msg)

        input_args = {}  # this is not used. Included only to maintain compatibility of return signature
        output_args = {}  # this is not used. Included only to maintain compatibility of return signature
        output_meta = {}  # this is not used. Included only to maintain compatibility of return signature
        output_list = []

        # There are two ways to gather inputs to a function.
        # 1) from the arguments of the function
        # 2) from the an explicit list of items returned by the get_input_items
        #    method

        try:
            input_set = set(obj.get_input_items())
        except AttributeError:
            input_set = set()
        else:
            if len(input_set) > 0:
                logger.debug(('Function %s has explicit required input items '
                              ' delivered by the get_input_items() method: %s'), name, input_set)

        if not isinstance(inputs, list):
            raise TypeError(('Function registration metadata must be defined',
                             ' using a list of objects derived from iotfunctions',
                             ' BaseUIControl. Check metadata for %s'
                             ' %s ' % (name, inputs)))

        if not isinstance(outputs, list):
            raise TypeError(('Function registration metadata must be defined',
                             ' using a list of objects derived from iotfunctions',
                             ' BaseUIControl. Check metadata for %s'
                             ' %s ' % (name, outputs)))

        args = []
        args.extend(inputs)
        args.extend(outputs)

        for a in args:
            try:
                # get arg name and type from UI object
                type_ = a.type_
                arg = a.name
            except AttributeError as e:
                try:
                    # get arg name and type from legacy dict
                    type_ = a.get('type', None)
                    arg = a.get('name', None)
                except AttributeError:
                    type_ = None
                    arg = None

            if type_ is None or arg is None:
                msg = ('Error while getting metadata from function. The inputs'
                       ' and outputs of the function are not described correctly'
                       ' using UIcontrols with a type_ %s and name %s' % (type_, arg))
                raise TypeError(msg)

            arg_value = getattr(obj, arg)
            out_arg = None
            out_arg_value = None

            if type_ == 'DATA_ITEM':
                # the argument is an input that contains a data item or
                # list of data items

                if isinstance(arg_value, list):
                    input_set |= set(arg_value)
                else:
                    input_set.add(arg_value)

                logger.debug('Using input items %s for %s', arg_value, arg)

            elif type_ == 'OUTPUT_DATA_ITEM':

                # the arg is an output item or list of them

                out_arg = arg
                out_arg_value = arg_value

            # some inputs implicitly describe outputs
            try:
                out_arg = a.output_item
            except AttributeError:
                pass  # no need to check legacy dict for this property as it was not supported in the legacy dict
            else:
                if out_arg is not None:
                    out_arg_value = getattr(obj, out_arg)

            # process output args
            if out_arg is not None:

                if isinstance(out_arg_value, list):
                    output_list.extend(out_arg_value)
                else:
                    output_list.append(out_arg_value)

                logger.debug('Using output items %s for %s', out_arg_value, out_arg)

        # output_meta is present in the AS metadata structure, but not
        # currently produced for local functions

        return (input_args, output_args, output_meta, input_set, output_list)

    def build_ui_constants(self):
        '''
        Build attributes for each ui constants declared with the entity type
        '''

        if self.ui_constants is None:
            logger.debug('No constants declared in entity definition')
            self.ui_constants = []
        params = {}
        for c in self.ui_constants:
            try:
                params[c.name] = c.default
            except AttributeError:
                logger.warning(('Cannot set value of parameter %s as it does'
                                ' not have a default value'), c.name)
        self.set_params(**params)

    def build_flat_stage_list(self):
        '''
        Build a flat list of all function objects defined for entity type
        '''

        stages = []

        for stage in self._functions:
            try:
                is_system = stage.is_system_function
            except AttributeError:
                is_system = False
                logger.warning(('Function %s has no is_system_function property.'
                                ' This means it was not inherited from '
                                ' an iotfunctions base class. AS authors are'
                                ' strongly encouraged to always inherit '
                                ' from iotfunctions base classes'), stage.__class__.__name__)
            if not is_system:
                stages.append(stage)

        return stages

    def build_granularities(self, grain_meta, freq_lookup):
        '''
        Convert AS granularity metadata to granularity objects.

        '''

        out = {}
        for g in grain_meta:
            grouper = []
            freq = None
            entity_id = None
            if g['entityFirst']:
                grouper.append(pd.Grouper(key=self._entity_id))
                entity_id = self._entity_id
            if g['frequency'] is not None:
                freq = (self.get_grain_freq(g['frequency'], freq_lookup, None))
                if freq is None:
                    raise ValueError(('Invalid frequency name %s. The frequency name'
                                      ' must exist in the frequency lookup %s' % (g['frequency'], freq_lookup)))
                # add a number to the frequency to make it compatible with pd.Timedelta
                if freq[0] not in ['1', '2', '3', '4', '5', '6', '7', '8', '9']:
                    freq = '1' + freq
                grouper.append(pd.Grouper(key=self._timestamp, freq=freq))
            custom_calendar = None
            custom_calendar_keys = []
            dimensions = []
            # differentiate between dimensions and custom calendar items
            for d in g['dataItems']:
                grouper.append(pd.Grouper(key=d))
                if self._custom_calendar is not None:
                    if d in self._custom_calendar._output_list:
                        custom_calendar_keys.append(d)
                dimensions.append(d)

            granularity = Granularity(name=g['name'], grouper=grouper, dimensions=dimensions,
                                      entity_name=self.logical_name, timestamp=self._timestamp, entity_id=entity_id,
                                      custom_calendar_keys=custom_calendar_keys, freq=freq,
                                      custom_calendar=custom_calendar)

            out[g['name']] = granularity

        return out

    def build_item_metadata(self, table):
        '''
        Build a client generated version of AS server metadata from a
        sql alachemy table object.
        '''

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' cannot build item metadata from tables ')
            raise ValueError(msg)

        for col_name, col in list(table.c.items()):
            item = {}
            if col_name not in self.get_excluded_cols():
                item['name'] = col_name
                item['type'] = 'METRIC'
                item['parentDataItem'] = None
                item['kpiFunctionDto'] = None
                item['columnName'] = col.name
                item['columnType'] = self.db.get_as_datatype(col)
                item['sourceTableName'] = self.name
                item['tags'] = []
                item['transient'] = False
                self._data_items.append(item)

        return self._data_items

    def build_schedules(self, metadata):
        '''
        Build a dictionary of schedule metadata from the schedules contained
        within function definitions.

        The schedule dictionary is keyed on a pandas freq string. This
        frequency denotes the schedule interval. The dictionary contains a
        tuple (start_hour,start_minute,backtrack_days)

        Returns
        -------
        tuple containing updated metadata and a dict of schedules

        Example
        -------

        { '5min': [16,3,7] }
        5 minute schedule interval with a start time of 4:03pm and backtrack of 7 days.

        '''

        freqs = {}

        for f in metadata:

            if f['schedule'] is not None:

                freq = f['schedule']['every']
                start = time.strptime(f['schedule']['starting_at'], '%H:%M:%S')
                start_hour = start[3]
                start_min = start[4]
                backtrack = f['backtrack']
                if backtrack is not None:
                    backtrack_days = (backtrack['days'] + backtrack['hours'] / 24 + backtrack['minutes'] / 1440)
                else:
                    backtrack_days = None

                existing_schedule = freqs.get(freq, None)
                if existing_schedule is None:
                    f[freq] = (start_hour, start_min, backtrack_days)
                else:
                    corrected_schedule = list(existing_schedule)

                    if existing_schedule[0] > start_hour:
                        corrected_schedule[0] = start_hour
                        logger.warning(('There is a conflict in the schedule metadata.'
                                        ' Picked the earlier start hour of %s instead of %s'
                                        ' for schedule %s.' % (start_hour, corrected_schedule[0], freq)))
                    if existing_schedule[1] > start_min:
                        corrected_schedule[1] = start_min
                        logger.warning(('There is a conflict in the schedule metadata.'
                                        ' Picked the earlier start minute of %s instead of %s'
                                        ' for schedule %s.' % (start_min, existing_schedule[1], freq)))
                    if backtrack_days is not None:
                        if existing_schedule[2] is None or existing_schedule[2] < backtrack_days:
                            corrected_schedule[2] = backtrack_days
                            logger.warning(('There is a conflict in the schedule metadata.'
                                            ' Picked the longer backtrack of %s instead of %s'
                                            ' for schedule %s.' % (backtrack_days, existing_schedule[2], freq)))
                    f[freq] = tuple(corrected_schedule)

                freqs[freq] = f[freq]
                f['schedule'] = freq

        return freqs

    def classify_stages(self):
        '''
        Create a dictionary of stage objects. Dictionary is keyed by
        stage type and a granularity obj. It contains a list of stage
        objects. Stages are classified by timing of execution, ie: preload,
        get_data, transform, aggregate
        '''

        stage_metadata = dict()
        active_granularities = set()

        # Add a data_reader stage. This will read entity data.

        if self._auto_read_from_ts_table:
            auto_reader = self._data_reader(name='read_entity_data', obj=self)
            stage_type = self.get_stage_type(auto_reader)
            granularity = None  # input level
            stage_metadata[(stage_type, granularity)] = [auto_reader]
            auto_reader.schedule = None
            auto_reader._entity_type = self
        else:
            logger.debug(('Skipped auto read of payload data as'
                          ' payload does not have _auto_read_from_ts_table'
                          ' set to True'))

        # Build a stage for each function.
        for s in self._functions:

            #  replace deprecated function
            obj = self.get_replacement(s)

            #  add metadata to stage
            try:
                obj.name
            except AttributeError:
                obj.name = obj.__class__.__name__
            try:
                obj._schedule
            except AttributeError:
                obj._schedule = None
            try:
                obj.granularity
            except AttributeError:
                obj.granularity = None

            # the stage needs to know what entity type it belongs to
            obj._entity_type = self

            # classify stage
            stage_type = self.get_stage_type(obj)
            granularity = obj.granularity

            if granularity is not None and isinstance(granularity, str):
                granularity = self._granularities_dict.get(granularity, False)
                if not granularity:
                    msg = ('Cannot build stage metdata. The granularity metadata'
                           ' is invalid. Granularity of function is %s. Valid '
                           ' granularities are %s' % (granularity, list(self._granularities_dict.keys())))
                    raise StageException(msg, obj.name)
            elif isinstance(granularity, Granularity):
                pass
            else:
                granularity = None
            try:
                #  add to stage_type / granularity
                stage_metadata[(stage_type, granularity)].append(obj)
            except KeyError:
                #  start a new stage_type / granularity
                stage_metadata[(stage_type, granularity)] = [obj]

            # Remember all active granularities
            if granularity is not None:
                active_granularities.add(granularity)

            # add metadata derived from function registration and function args
            # input set and output list are critical metadata for the dependency model

            # there are three ways to set them
            # 1) using the instance variables _input_set and _output_list
            # 2) using the methods get_input_set and get_output_list
            # 3) using the function's registration metadata

            if obj._input_set is not None:
                logger.debug('Input set was preset for function %s', obj.name)
                input_set = obj._input_set
            else:
                try:
                    input_set = obj.get_input_set()
                except AttributeError:
                    input_set = None

            if obj._output_list is not None:
                logger.debug('Output list set was preset for function %s', obj.name)
                output_list = obj._output_list
            else:
                try:
                    output_list = obj.get_output_list()
                except AttributeError:
                    output_list = None

            if input_set is None or output_list is None:
                # get the input set and output list from the function argument metadata
                (in_, out, out_meta, reg_input_set, reg_output_list) = self.build_arg_metadata(obj)
                if input_set is None:
                    input_set = reg_input_set
                if output_list is None:
                    output_list = reg_output_list

            # set the _input_set and _output_list

            obj._input_set = input_set
            obj._output_list = output_list

            # The stage may have metadata parameters that need to be
            # copied onto the entity type
            try:
                entity_metadata = obj._metadata_params
            except AttributeError:
                entity_metadata = {}
                logger.debug(('Function %s has no _metadata_params'
                              ' property. This property allows the stage'
                              ' to add properties to the entity type.'
                              ' Using default of %s'), obj.name, entity_metadata)
            if entity_metadata is not None and entity_metadata:
                self.set_params(**entity_metadata)
                self.trace_append(created_by=obj, msg='Adding entity type properties from function',
                                  log_method=logger.debug, **entity_metadata)

            # The stage may be a special stage that should be added to
            # a special stages list, e.g. stages that have
            # the property is_scd_lookup = True should be added to the
            # _scd_stages list

            specials = {'is_scd_lookup': self._scd_stages}

            for function_prop, list_obj in list(specials.items()):
                try:
                    is_function_prop = getattr(obj, function_prop)
                except AttributeError:
                    is_function_prop = False

                if is_function_prop:
                    list_obj.append(obj)

        # Add for each granularity without frequency two AggregateItem stages. The result columns of these stages
        # are used in the DataWriter when the aggregation results are pushed to the database
        for gran in active_granularities:
            if gran.freq is None:
                for func_name, output_name in {('max', DataWriter.ITEM_NAME_TIMESTAMP_MAX),
                                               ('min', DataWriter.ITEM_NAME_TIMESTAMP_MIN)}:
                    new_stage = AggregateItems(input_items=[self._timestamp], aggregation_function=func_name,
                                               output_items=[output_name])
                    new_stage._entity_type = self
                    new_stage.name = new_stage.__class__.__name__
                    new_stage._schedule = None
                    new_stage.granularity = gran
                    new_stage._input_set = {self._timestamp}
                    new_stage._output_list = [output_name]
                    stage_type = self.get_stage_type(new_stage)
                    stage_metadata[(stage_type, gran)].append(new_stage)

        return stage_metadata

    def build_stage_metadata(self, *args):
        '''
        Make a new JobController payload from a list of function objects
        '''
        metadata = []
        for f in args:
            # if function is deprecated it may have a replacement
            f = self.get_replacement(f)
            fn = {}
            try:
                name = f.name
            except AttributeError:
                name = f.__class__.__name__
            fn['name'] = name
            fn['object_instance'] = f
            fn['description'] = f.__doc__
            fn['functionName'] = f.__class__.__name__
            fn['enabled'] = True
            fn['execStatus'] = False
            fn['schedule'] = None
            fn['backtrack'] = None
            fn['granularity'] = f.granularity
            (fn['input'], fn['output'], fn['outputMeta'], fn['input_set'], fn['output_list']) = self.build_arg_metadata(
                f)
            fn['inputMeta'] = None
            metadata.append(fn)
            logger.debug(('Added local function instance as job stage: %s'), fn)

        self._stages = self.build_stages(function_meta=metadata, granularities_dict=self._granularities_dict)

        return metadata

    def index_df(self, df):
        '''
        Create an index on the deviceid and the timestamp
        '''

        if self._df_index_entity_id is None:
            self._df_index_entity_id = self._entity_id

        if self._timestamp_col is None:
            self._timestamp_col = self._timestamp

        if df.index.names != [self._df_index_entity_id, self._timestamp]:
            try:
                df = df.set_index([self._df_index_entity_id, self._timestamp])
            except KeyError:
                df = reset_df_index(df, auto_index_name=self.auto_index_name)
                try:
                    df = df.set_index([self._df_index_entity_id, self._timestamp])
                except KeyError:
                    try:
                        df[self._df_index_entity_id] = df[self._entity_id]
                        df = df.set_index([self._df_index_entity_id, self._timestamp])
                    except KeyError:
                        raise KeyError(('Error attempting to index time series'
                                        ' dataframe. Unable to locate index'
                                        ' columns: %s or %s, %s') % (
                                           self._df_index_entity_id, self._entity_id, self._timestamp))
            logger.debug(('Indexed dataframe on %s, %s'), self._df_index_entity_id, self._timestamp)

        else:
            logger.debug(('Found existing index on %s, %s.'
                          'No need to recreate index'), self._df_index_entity_id, self._timestamp)

        # create a dummy column for _entity_id
        if self._entity_id != self._df_index_entity_id:
            df[self._entity_id] = df.index.get_level_values(self._df_index_entity_id)

        # create a dummy column for _timestamp
        if self._timestamp != self._timestamp_col:
            df[self._timestamp_col] = df.index.get_level_values(self._timestamp)

        return df

    def cos_save(self):

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' are not allowed to save to cloud object storage ')
            raise ValueError(msg)

        name = ['entity_type', self.name]
        name = '.'.join(name)
        self.db.cos_save(self, name)

    @classmethod
    def default_stage_type_map(cls):
        '''
        Configure how properties of stages are used to set the stage type
        that is used by the job controller to decide how to process a stage
        '''

        return [('preload', 'is_preload'), ('get_data', 'is_data_source'), ('transform', 'is_transformer'),
                ('aggregate', 'is_data_aggregator'), ('simple_aggregate', 'is_simple_aggregator'),
                ('complex_aggregate', 'is_complex_aggregator'), ]

    def df_sort_timestamp(self, df):

        '''
        Sort a dataframe on the timestamp column. Returns a tuple containing
        the sorted dataframe and a column_name for the timestamp column.
        '''

        ts_col_name = self._timestamp

        # timestamp may be column or in index
        try:
            df.sort_values([ts_col_name], inplace=True)
        except KeyError:
            try:
                # legacy check for a redundant _timestamp alternative column
                df.sort_values([self._timestamp_col], inplace=True)
                ts_col_name = self._timestamp_col
            except KeyError:
                try:
                    df.sort_index(level=[ts_col_name], inplace=True)
                except:
                    raise

        return (df, ts_col_name)

    def drop_tables(self, recreate=False):
        '''
        Drop tables known to be associated with this entity type

        '''

        self.db.drop_table(self.name, schema=self._db_schema, recreate=recreate)
        self.drop_child_tables(recreate=recreate)

    def drop_child_tables(self, recreate=False):
        '''
        Drop all child tables
        '''

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' are not allowed to drop child tables ')
            raise ValueError(msg)

        if self._dimension_table_name is None:
            tables = []
        else:
            tables = [self._dimension_table_name]
        tables.extend(self.activity_tables.values())
        tables.extend(self.scd.values())
        [self.db.drop_table(x, self._db_schema, recreate=recreate) for x in tables]
        msg = 'dropped tables %s' % tables
        logger.info(msg)

    def exec_local_pipeline(self, start_ts=None, end_ts=None, entities=None, **kw):
        '''
        Test the functions on an entity type
        Test will be run on local metadata. It will not use the server
        job log. Results will be written to file.
        '''

        params = {'data_writer': DataWriterFile, 'keep_alive_duration': None, 'save_trace_to_file': True,
                  'default_backtrack': 'checkpoint', 'trace_df_changes': True, '_abort_on_fail': True,
                  'job_log_class': JobLogNull, '_auto_save_trace': None, '_start_ts_override': start_ts,
                  '_end_ts_override': end_ts, '_entity_filter_list': entities}

        kw = {**params, **kw}

        job = JobController(payload=self, **kw)

        # propagate parameters to functions

        for f in self._functions:
            for key, value in list(kw.items()):
                setattr(f, key, value)

        job.execute()

    def get_attributes_dict(self):
        '''
        Produce a dictionary containing all attributes
        '''
        c = {}
        for att in dir(self):
            value = getattr(self, att)
            if not callable(value):
                c[att] = value
        return c

    def get_calc_pipeline(self, stages=None):
        '''
        Make a new CalcPipeline object. Reset processing variables.
        '''
        warnings.warn('get_calc_pipeline() is deprecated. Use build_job()', DeprecationWarning)
        self._scd_stages = []
        self._custom_calendar = None
        self._is_initial_transform = True
        return CalcPipeline(stages=stages, entity_type=self)

    def get_function_replacement_metadata(self, meta):

        '''
        replace incoming function metadata for aggregate functions with
        metadata that will be used to build a DataAggregator
        '''

        replacement = {'Sum': 'sum', 'Minimum': 'min', 'Maximum': 'max', 'Mean': 'mean', 'Median': 'median',
                       'Count': 'count', 'DistinctCount': 'count_distinct', 'StandardDeviation': 'std',
                       'Variance': 'var', 'Product': 'product', 'First': 'first', 'Last': 'last'}

        name = meta.get('functionName', None)
        replacement_name = replacement.get(name, None)

        if replacement_name is not None:

            meta['functionName'] = replacement_name
            return (meta.get('granularity', None), meta)

        else:

            return (None, None)

    def get_local_column_lists_by_type(self, columns, known_categoricals_set=None):

        '''
        Examine a list of columns and poduce a tuple containing names
        of metric,dates,categoricals and others
        '''

        if known_categoricals_set is None:
            known_categoricals_set = set()

        metrics = []
        dates = []
        categoricals = []
        others = []

        if columns is None:
            columns = []

        all_cols = set([x.name for x in columns])
        # exclude known categoricals that are not present in table
        known_categoricals_set = known_categoricals_set.intersection(all_cols)

        for c in columns:
            data_type = c.type
            if isinstance(data_type, (FLOAT, Float, INTEGER, Integer)):
                metrics.append(c.name)
            elif db_module.DB2_DOUBLE is not None and isinstance(data_type, db_module.DB2_DOUBLE):
                metrics.append(c.name)
            elif isinstance(data_type, (VARCHAR, String)):
                categoricals.append(c.name)
            elif isinstance(data_type, (TIMESTAMP, DateTime)):
                dates.append(c.name)
            else:
                others.append(c.name)
                msg = 'Found column %s of unknown data type %s' % (c, data_type.__class__.__name__)
                logger.warning(msg)

        # reclassify categoricals that did were not correctly classified based on data type

        for c in known_categoricals_set:
            if c not in categoricals:
                categoricals.append(c)
            metrics = [x for x in metrics if x != c]
            dates = [x for x in dates if x != c]
            others = [x for x in others if x != c]

        return (metrics, dates, categoricals, others)

    def get_custom_calendar(self):

        return self._custom_calendar

    def get_data(self, start_ts=None, end_ts=None, entities=None, columns=None):
        '''
        Retrieve entity data at input grain or preaggregated
        '''

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' are not allowed to retrieve database data ')
            raise ValueError(msg)

        tw = {}  # info to add to trace
        if entities is None:
            tw['entity_filter'] = 'all'
        else:
            tw['entity_filter'] = '%s entities' % len(entities)

        dimension_table_exists = self.db.if_exists(table_name=self._dimension_table_name, schema=self._db_schema)
        if not dimension_table_exists:
            self._dimension_table_name = None

        if self._pre_aggregate_time_grain is None:
            df = self.db.read_table(table_name=self.name, schema=self._db_schema, timestamp_col=self._timestamp,
                                    parse_dates=None, columns=columns, start_ts=start_ts, end_ts=end_ts,
                                    entities=entities, dimension=self._dimension_table_name)
            tw['pre-aggregeted'] = None

        else:
            (metrics, dates, categoricals, others) = self.db.get_column_lists_by_type(self.name, self._db_schema)
            if self._dimension_table_name is not None:
                categoricals.extend(self.db.get_column_names(self._dimension_table_name, self._db_schema))
            if columns is None:
                columns = []
                columns.extend(metrics)
                columns.extend(dates)
                columns.extend(categoricals)
                columns.extend(others)

            # make sure each column is in the aggregate dictionary
            # apply a default aggregate for each column not specified in the aggregation metadata
            if self._pre_agg_rules is None:
                self._pre_agg_rules = {}
                self._pre_agg_outputs = {}
            for c in columns:
                try:
                    self._pre_agg_rules[c]
                except KeyError:
                    if c not in [self._timestamp, self._entity_id]:
                        if c in metrics:
                            self._pre_agg_rules[c] = 'mean'
                            self._pre_agg_outputs[c] = 'mean_%s' % c
                        else:
                            self._pre_agg_rules[c] = 'max'
                            self._pre_agg_outputs[c] = 'max_%s' % c
                else:
                    pass

            df = self.db.read_agg(table_name=self.name, schema=self._db_schema, groupby=[self._entity_id],
                                  timestamp=self._timestamp, time_grain=self._pre_aggregate_time_grain,
                                  agg_dict=self._pre_agg_rules, agg_outputs=self._pre_agg_outputs, start_ts=start_ts,
                                  end_ts=end_ts, entities=entities, dimension=self._dimension_table_name)

            tw['pre-aggregeted'] = self._pre_aggregate_time_grain

        tw['rows_retrieved'] = len(df.index)
        tw['start_ts'] = start_ts
        tw['end_ts'] = end_ts
        self.trace_append(created_by=self, msg='Retrieved entity timeseries data for %s' % self.name, **tw)

        # Optimizing the data frame size using downcasting
        if self.enable_downcast:
            memo = MemoryOptimizer()
            df = memo.downcastNumeric(df)
        try:
            df = self.index_df(df)
        except (AttributeError, KeyError):
            pass

        return df

    def get_data_items(self):
        '''
        Get the list of data items defined

        :return: list of dicts containting data item metadata
        '''
        return self._data_items

    def get_excluded_cols(self):
        '''
        Return a list of physical columns that should be excluded when returning
        the list of data items
        '''

        return ['logicalinterface_id', 'format', 'updated_utc', 'devicetype', 'eventtype']

    def get_grain_freq(self, grain_name, lookup, default):
        '''
        Lookup a pandas frequency string from an AS granularity name
        '''
        for l in lookup:
            if grain_name == l['name']:
                return l['alias']
        return default

    def get_output_items(self):
        '''
        Get a list of non calculated items: outputs from the time series table
        '''

        items = [x.get('columnName') for x in self._data_items if
                 x.get('type') == 'METRIC' or x.get('type') == 'DIMENSION']

        return items

    def get_log(self, rows=100):
        '''
        Get KPI execution log info. Returns a dataframe.
        '''

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' are not allowed to get log data ')
            raise ValueError(msg)

        query, log = self.db.query(self.log_table, self._db_schema)
        query = query.filter(log.c.entity_type == self.name).order_by(log.c.timestamp_utc.desc()).limit(rows)
        df = self.db.get_query_data(query)
        return df

    def get_latest_log_entry(self):
        '''
        Get the most recent log entry. Returns dict.
        '''
        last = self.get_log(rows=1)
        last = last.to_dict('records')[0]
        return last

    def get_param(self, param, default=None):

        try:
            out = getattr(self, param)
        except AttributeError:
            out = default

        return out

    def get_end_ts_override(self):
        if self._end_ts_override is not None:
            if isinstance(self._end_ts_override, dt.datetime):
                return self._end_ts_override
            date_time_obj = dt.datetime.strptime(self._end_ts_override[0], '%Y-%m-%d %H:%M:%S')
            return date_time_obj
        return None

    def get_stage_type(self, stage):
        '''
        Examine the stage object to determine how it should be processed by
        the JobController

        Sets the stage type to the first valid entry in the stage map
        the stage map is a list of tuples containing a stage type and
        a boolean property name:

        example:
            [('get_data','is_data_source'),
             ('simple_aggregate','is_simple_aggregate')]

        if a stage has both an is_data_source = True and
        a is_simple_aggregate = True, the stage type will be returned as
        'get_data'
        '''

        for (stage_type, prop) in self._stage_type_map:
            try:
                prop_value = getattr(stage, prop)
            except AttributeError:
                pass
            else:
                if prop_value:
                    return stage_type

        raise TypeError(('Could not identify stage type for stage'
                         ' %s from the stage map. Adjust the stage map'
                         ' for the entity type or define an appropriate'
                         ' is_<something> property on the class of the '
                         ' stage. Stage map is %s' % (stage.name, self._stage_type_map)))

    def get_start_ts_override(self):
        if self._start_ts_override is not None:
            if isinstance(self._start_ts_override, dt.datetime):
                date_time_obj = self._start_ts_override
            else:
                date_time_obj = dt.datetime.strptime(self._start_ts_override[0], '%Y-%m-%d %H:%M:%S')
            return date_time_obj
        return None

    def get_replacement(self, obj):
        '''
        Get replacement for deprecated function
        '''
        try:
            is_deprecated = obj.is_deprecated
        except AttributeError:
            is_deprecated = False

        if is_deprecated:
            try:
                obj = obj.get_replacement()
            except AttributeError:
                msg = ('Skipped deprecated function. The function'
                       ' %s has no designated replacement. Provide a'
                       ' replacement by implementing the get_replacement()'
                       ' method or rework entity type to remove the reference'
                       ' to the deprecated function' % obj.__class__.__name__)
                raise StageException(msg, obj.__class__.__name__)
            else:
                logger.debug('Entity Type has a reference to a deprecated'
                             ' function. This function was automatically'
                             ' replaced by %s', obj.__class__.__name__)
        return obj

    def generate_data(self, entities=None, days=0, seconds=300, freq='1min', scd_freq='1D', write=True,
                      drop_existing=False, data_item_mean=None, data_item_sd=None, data_item_domain=None, columns=None,
                      start_entity_id=None, auto_entity_count=None, datasource=None, datasourcemetrics=None):
        '''
        Generate random time series data for entities

        Parameters
        ----------
        entities: list
            List of entity ids to genenerate data for
        days: number
            Number of days worth of data to generate (back from system date)
        seconds: number
            Number of seconds of worth of data to generate (back from system date)
        freq: str
            Pandas frequency string - interval of time between subsequent rows of data
        write: bool
            write generated data back to table with same name as entity
        drop_existing: bool
            drop existing time series, dimension, activity and scd table
        data_item_mean: dict
            mean values for generated data items. dict is keyed on data item name
        data_item_sd: dict
            std values for generated data items. dict is keyed on data item name
        data_item_domain: dict
            domains of values for categorical data items. dict is keyed on data item name
        datasource: dataframe
            dataframe as data source
        datasourcemetrics : list of strings
            list of relevant column for datasource
        '''
        if entities is None:
            if start_entity_id is None:
                start_entity_id = self._start_entity_id
            if auto_entity_count is None:
                auto_entity_count = self._auto_entity_count
            entities = [str(start_entity_id + x) for x in list(range(auto_entity_count))]

        if data_item_mean is None:
            data_item_mean = {}
        if data_item_sd is None:
            data_item_sd = {}
        if data_item_domain is None:
            data_item_domain = {}

        if drop_existing and self.db is not None:
            self.drop_tables(recreate=True)

        known_categoricals = set(data_item_domain.keys())

        exclude_cols = ['deviceid', 'devicetype', 'format', 'updated_utc', 'logicalinterface_id', self._timestamp]
        if self.db is None or self.is_local:
            write = False
            msg = 'This is a local entity or entity with no database connection, test data will not be written'
            logger.debug(msg)
            (metrics, dates, categoricals, others) = self.get_local_column_lists_by_type(columns,
                                                                                         known_categoricals_set=known_categoricals)
        else:
            (metrics, dates, categoricals, others) = self.db.get_column_lists_by_type(self.table, self._db_schema,
                                                                                      exclude_cols=exclude_cols,
                                                                                      known_categoricals_set=known_categoricals)
        msg = 'Generating data for %s with metrics %s and dimensions %s and dates %s' % (
            self.name, metrics, categoricals, dates)
        logger.debug(msg)

        ts = TimeSeriesGenerator(metrics=metrics, ids=entities, days=days, seconds=seconds, freq=freq,
                                 categoricals=categoricals, dates=dates, timestamp=self._timestamp,
                                 domains=data_item_domain, datasource=datasource, datasourcemetrics=datasourcemetrics)
        ts.data_item_mean = data_item_mean
        ts.data_item_sd = data_item_sd
        ts.data_item_domain = data_item_domain
        df = ts.execute()

        dimension_table_exists = False
        try:
            dimension_table_exists = self.db.if_exists(table_name=self._dimension_table_name, schema=self._db_schema)
        except Exception:
            pass

        if self._dimension_table_name is not None and dimension_table_exists:
            self.generate_dimension_data(entities, write=write, data_item_mean=data_item_mean,
                                         data_item_sd=data_item_sd, data_item_domain=data_item_domain)

        if write and self.db is not None:
            for o in others:
                if o not in df.columns:
                    df[o] = None
            df['logicalinterface_id'] = ''
            df['devicetype'] = self.logical_name
            df['format'] = ''
            df['updated_utc'] = dt.datetime.utcnow()
            df['updated_utc'] = df['updated_utc'].astype('datetime64[ms]')
            self.db.write_frame(table_name=self.name, df=df, schema=self._db_schema, timestamp_col=self._timestamp)

        for (at_name, at_table) in list(self.activity_tables.items()):
            adf = self.generate_activity_data(table_name=at_name, activities=at_table._activities, entities=entities,
                                              days=days, seconds=seconds, write=write)
            msg = 'generated data for activity table %s' % at_name
            logger.debug(msg)

        for scd in list(self.scd.values()):
            sdf = self.generate_scd_data(scd_obj=scd, entities=entities, days=days, seconds=seconds, write=write,
                                         freq=scd_freq, domains=data_item_domain)
            msg = 'generated data for scd table %s' % scd.name
            logger.debug(msg)

        return df

    def generate_activity_data(self, table_name, activities, entities, days, seconds, write=True):

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' are not allowed to generate activity data ')
            raise ValueError(msg)

        try:
            (metrics, dates, categoricals, others) = self.db.get_column_lists_by_type(table_name, self._db_schema,
                                                                                      exclude_cols=[self._entity_id,
                                                                                                    'start_date',
                                                                                                    'end_date'])
        except KeyError:
            metrics = []
            dates = []
            categoricals = []
            others = []
        metrics.append('duration')
        categoricals.append('activity')
        ts = TimeSeriesGenerator(metrics=metrics, dates=dates, categoricals=categoricals, ids=entities, days=days,
                                 seconds=seconds, freq=self._activity_frequency)
        ts.set_domain('activity', activities)
        df = ts.execute()
        df['start_date'] = df[self._timestamp]
        duration = df['duration'].abs()
        df['end_date'] = df['start_date'] + pd.to_timedelta(duration, unit='h')
        # probability that an activity took place in the interval
        p_activity = (days * 60 * 60 * 24 + seconds) / pd.to_timedelta(self._activity_frequency).total_seconds()
        is_activity = p_activity >= np.random.uniform(0, 1, len(df.index))
        df = df[is_activity]
        cols = [x for x in df.columns if x not in ['duration', self._timestamp]]
        df = df[cols]
        if write:
            msg = 'Generated %s rows of data and inserted into %s' % (len(df.index), table_name)
            logger.debug(msg)
            self.db.write_frame(table_name=table_name, df=df, schema=self._db_schema)
        return df

    def generate_dimension_data(self, entities, write=True, data_item_mean=None, data_item_sd=None,
                                data_item_domain=None):

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' are not allowed to generate dimension data ')
            raise ValueError(msg)

        # check for existing dimension data
        df_existing = self.db.read_dimension(self._dimension_table_name, schema=self._db_schema, entities=entities)
        existing_entities = set(df_existing[self._entity_id])
        # do not generate data for existing entities
        entities = list(set(entities) - existing_entities)
        if len(entities) > 0:

            if data_item_mean is None:
                data_item_mean = {}
            if data_item_sd is None:
                data_item_sd = {}
            if data_item_domain is None:
                data_item_domain = {}

            known_categoricals = set(data_item_domain.keys())

            (metrics, dates, categoricals, others) = self.db.get_column_lists_by_type(self._dimension_table_name,
                                                                                      self._db_schema,
                                                                                      exclude_cols=[self._entity_id],
                                                                                      known_categoricals_set=known_categoricals)

            rows = len(entities)
            data = {}

            for m in metrics:
                mean = data_item_mean.get(m, 0)
                sd = data_item_sd.get(m, 1)
                data[m] = MetricGenerator(m, mean=mean, sd=sd).get_data(rows=rows)

            for c in categoricals:
                categories = data_item_domain.get(c, None)
                data[c] = CategoricalGenerator(c, categories).get_data(rows=rows)

            data[self._entity_id] = entities

            df = pd.DataFrame(data=data)

            for d in dates:
                df[d] = DateGenerator(d).get_data(rows=rows)
                df[d] = pd.to_datetime(df[d])

            if write:
                if self.db.db_type == 'db2':
                    self._dimension_table_name = self._dimension_table_name.upper()
                else:
                    self._dimension_table_name = self._dimension_table_name.lower()
                self.db.write_frame(df, table_name=self._dimension_table_name, if_exists='append',
                                    schema=self._db_schema)

        else:
            logger.debug('No new entities. Did not generate dimension data.')

    def get_entity_filter(self):
        '''
        Get the list of entity ids that are valid for pipeline processing.
        '''

        return self._entity_filter_list

    def get_last_checkpoint(self):
        '''
        Get the last checkpoint recorded for entity type
        '''
        if self.db is None:
            msg = ('Entity type has no db connection. Local entity'
                   ' types do not have a checkpoint')
            logger.debug(msg)
            return None

        (query, table) = self.db.query_column_aggregate(table_name=self.checkpoint_table, schema=self._db_schema,
                                                        column='TIMESTAMP', aggregate='max')

        query.filter(table.c.entity_type_id == self._entity_type_id)
        return query.scalar()

    def generate_scd_data(self, scd_obj, entities, days, seconds, freq, write=True, domains=None):

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' are not allowed to generate scd data ')
            raise ValueError(msg)

        if domains is None:
            domains = {}

        table_name = scd_obj.name
        msg = 'generating data for %s for %s days and %s seconds' % (table_name, days, seconds)
        try:
            (metrics, dates, categoricals, others) = self.db.get_column_lists_by_type(table_name, self._db_schema,
                                                                                      exclude_cols=[self._entity_id,
                                                                                                    'start_date',
                                                                                                    'end_date'])
        except KeyError:
            metrics = []
            dates = []
            categoricals = [scd_obj.property_name.name]
            others = []
        msg = msg + ' with metrics %s, dates %s, categorials %s and others %s' % (metrics, dates, categoricals, others)
        logger.debug(msg)
        ts = TimeSeriesGenerator(metrics=metrics, dates=dates, categoricals=categoricals, ids=entities, days=days,
                                 seconds=seconds, freq=freq, domains=domains)

        df = ts.execute()
        df['start_date'] = df[self._timestamp]
        # probability that a change took place in the interval
        p_activity = (days * 60 * 60 * 24 + seconds) / pd.to_timedelta(freq).total_seconds()
        is_activity = p_activity >= np.random.uniform(0, 1, len(df.index))
        df = df[is_activity]
        cols = [x for x in df.columns if x not in [self._timestamp]]
        df = df[cols]
        df['end_date'] = None
        if len(df.index) > 0:
            df = df.groupby([self._entity_id]).apply(self._set_end_date)
            try:
                self.db.truncate(table_name, schema=self._db_schema)
            except KeyError:
                pass
            if write:
                msg = 'Generated %s rows of data and inserted into %s' % (len(df.index), table_name)
                logger.debug(msg)
                self.db.write_frame(table_name=table_name, df=df, schema=self._db_schema, if_exists='append')
        return df

    def _get_scd_list(self):
        return [(s.output_item, s.table_name) for s in self._scd_stages]

    def is_base_item(self, item_name):
        '''
        Base items are non calculated data items.
        '''

        item_type = self._data_items[item_name]['columnType']
        if item_type == 'METRIC':
            return True
        else:
            return False

    def is_data_item(self, name):
        '''
        Determine whether an item is a data item
        '''

        if name in [x.name for x in self._data_items]:
            return True
        else:
            return False

    def make_dimension(self, name=None, *args, **kw):
        '''
        Add dimension table by specifying additional columns

        Parameters
        ----------
        name: str
            dimension table name
        *args: sql alchemchy Column objects
        * kw: : schema
        '''

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' are not allowed to make dimensions ')
            raise ValueError(msg)

        kw['schema'] = self._db_schema
        if name is None:
            name = '%s_dimension' % self.name

        # name = name.lower()
        # self._dimension_table_name = name
        if self.db.db_type == 'db2':
            self._dimension_table_name = name.upper()
        else:
            self._dimension_table_name = name.lower()

        try:
            self._dimension_table = self.db.get_table(self._dimension_table_name, self._db_schema)
        except KeyError:
            dim = db_module.Dimension(self._dimension_table_name, self.db, *args, **kw)
            self._dimension_table = dim.table
            dim.create()
            msg = 'Created dimension table %s' % self._dimension_table_name
            logger.debug(msg)

    def publish_kpis(self, raise_error=True):

        warnings.warn(('publish_kpis() is deprecated for EntityType. Instead'
                       ' use an EntityType inherited from BaseCustomEntityType'), DeprecationWarning)

        '''
        Publish the stages assigned to this entity type to the AS Server
        '''

        export = []
        stages = self.build_flat_stage_list()

        self.db.register_functions(stages)

        for s in stages:
            try:
                name = s.name
            except AttributeError:
                name = s.__class__.__name__
                logger.debug(('Function class %s has no name property.'
                              ' Using the class name'), name)

            try:
                args = s._get_arg_metadata()
            except AttributeError:
                msg = ('Attempting to publish kpis for an entity type.'
                       ' Function %s has no _get_arg_spec() method.'
                       ' It cannot be published') % name
                raise NotImplementedError(msg)

            metadata = {'name': name, 'args': args}
            export.append(metadata)

        logger.debug('Published kpis to entity type')
        logger.debug(export)

        response = self.db.http_request(object_type='kpiFunctions', object_name=self.logical_name, request='POST',
                                        payload=export, raise_error=raise_error)

        logger.debug(response)

        return response

    def raise_error(self, exception, msg='', abort_on_fail=False, stageName=None):
        '''
        Raise an exception. Append a message and the current trace to the stacktrace.
        '''
        msg = ('Execution of function %s failed due to %s'
               ' Error message: %s '
               ' Stack trace : %s '
               ' Execution trace : %s' % (
                   stageName, exception.__class__.__name__, msg, traceback.format_exc(), str(self._trace)))

        if abort_on_fail:
            raise StageException(msg, stageName)
        else:
            logger.warning(msg)

        return msg

    def register(self, publish_kpis=False, raise_error=False):
        '''
        Register entity type so that it appears in the UI. Create a table for input data.

        Parameters
        ----------
        credentials: dict
            credentials for the ICS metadata service

        '''

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' may not be registered ')
            raise ValueError(msg)

        cols = []
        columns = []
        metric_column_names = []
        table = {}
        table['name'] = self.logical_name
        table['metricTableName'] = self.name
        table['metricTimestampColumn'] = self._timestamp
        table['description'] = self.description
        table['origin'] = 'AS_SAMPLE'
        for c in self.db.get_column_names(self.table, schema=self._db_schema):
            cols.append((self.table, c, 'METRIC'))
            metric_column_names.append(c)

        if self._dimension_table is not None:
            table['dimensionTableName'] = self._dimension_table_name
            for c in self.db.get_column_names(self._dimension_table, schema=self._db_schema):
                if c not in metric_column_names:
                    cols.append((self._dimension_table, c, 'DIMENSION'))
        for (table_obj, column_name, col_type) in cols:
            msg = 'found %s column %s' % (col_type, column_name)
            logger.debug(msg)
            if column_name not in self.get_excluded_cols():
                data_type = table_obj.c[column_name].type
                if isinstance(data_type, (FLOAT, Float, INTEGER, Integer)):
                    data_type = 'NUMBER'
                elif db_module.DB2_DOUBLE is not None and isinstance(data_type, db_module.DB2_DOUBLE):
                    data_type = 'NUMBER'
                elif isinstance(data_type, (VARCHAR, String)):
                    data_type = 'LITERAL'
                elif isinstance(data_type, (TIMESTAMP, DateTime)):
                    data_type = 'TIMESTAMP'
                else:
                    data_type = str(data_type)
                    logger.warning('Unknown datatype %s for column %s' % (data_type, column_name))
                columns.append(
                    {'name': column_name, 'type': col_type, 'columnName': column_name, 'columnType': data_type,
                     'tags': None, 'transient': False})
        table['dataItemDto'] = columns
        if self._db_schema is not None:
            table['schemaName'] = self._db_schema
        else:
            try:
                table['schemaName'] = self.db.credentials['db2']['username']
            except KeyError:
                try:
                    username = self.db.credentials["postgresql"]['username']
                    table["schemaName"] = "public"
                except KeyError:
                    raise KeyError('No database credentials found. Unable to register table.')
        payload = [table]
        response = self.db.http_request(request='POST', object_type='entityType', object_name=self.name,
                                        payload=payload, raise_error=raise_error)

        msg = 'Metadata registered for table %s ' % self.name
        logger.debug(msg)
        if publish_kpis:
            self.publish_kpis(raise_error=raise_error)
            self.db.register_constants(self.ui_constants)

        return response

    def trace_append(self, created_by, msg, log_method=None, df=None, **kwargs):
        '''
        Write to entity type trace
        '''
        self._trace.write(created_by=created_by, log_method=log_method, text=msg, df=df, **kwargs)

    def set_custom_calendar(self, custom_calendar):
        '''
        Set a custom calendar for the entity type.
        '''
        if custom_calendar is not None:
            self._custom_calendar = custom_calendar

    def get_server_params(self):
        '''
        Retrieve the set of properties assigned through the UI
        Assign to instance variables
        '''

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' are not able to get server params ')
            logger.debug(msg)
            return {}

        meta = self.db.http_request(object_type='constants', object_name=self.logical_name, request='GET')
        try:
            meta = json.loads(meta)
        except (TypeError, json.JSONDecodeError):
            params = {}
            logger.debug('API call to server did not retrieve valid entity type properties. No properties set.')
        else:
            params = {}
            for p in meta:
                key = p['name']
                if isinstance(p['value'], dict):
                    params[key] = p['value'].get('value', p['value'])
                else:
                    params[key] = p['value']
                logger.debug('Adding server property %s with value %s to entity type', key, params[key])
            self.set_params(**params)

        return params

    def _set_end_date(self, df):

        df['end_date'] = df['start_date'].shift(-1)
        df['end_date'] = df['end_date'] - pd.Timedelta(microseconds=1)
        df['end_date'] = df['end_date'].fillna(pd.Timestamp.max)
        return df

    def __str__(self):
        out = '\n%s:%s' % (self.__class__.__name__, self.name)
        # out = out + '\nData items:'
        # for i in self._data_items:
        #    out = out + '\n   %s (%s) of type %s' %(
        #            i.get('name'),i.get('type'),i.get('columnType'))
        out = out + '\nFunctions:'
        for f in self._functions:
            out = out + '\n' + str(f)
        out = out + '\nGranularities:'
        for g in list(self._granularities_dict.values()):
            out = out + '\n   ' + str(g)
        if self._schedules_dict is not None and self._schedules_dict:
            out = out + '\nSchedules:'
            for s in list(self._schedules_dict.keys()):
                out = out + '\n   ' + str(s)
        else:
            out = out + '\nNo schedules metadata'

        return out

    def set_params(self, **params):
        '''
        Set parameters based using supplied dictionary
        '''
        for key, value in list(params.items()):
            setattr(self, key, value)
        return self

    def write_unmatched_members(self, df):
        '''
        Write a row to the dimension table for every entity instance in the dataframe supplied
        '''

        if df.empty or self.db is None:
            return []
        else:
            # add a dimension table if there is none
            if self._dimension_table_name is None:
                new_dim_name = '%s%s' % (self.name, self._auto_dim_suffix)
                self.make_dimension(name=new_dim_name)
                self.register()
            # get existing dimension keys
            ef = self.db.read_table(self._dimension_table_name, schema=self._db_schema, columns=[self._entity_id])
            ids = set(ef[self._entity_id].unique())
            # get new members from the dataframe supplied
            new_ids = set(df[self._entity_id].unique()) - ids
            # write
            self.db.start_session()
            table = self.db.get_table(self._dimension_table_name, self._db_schema)
            for i in new_ids:
                stmt = table.insert().values({self._entity_id: i})
                self.db.connection.execute(stmt)
            self.db.commit()
            return new_ids


class ServerEntityType(EntityType):
    '''
    Initialize an entity type using AS Server metadata
    '''

    def __init__(self, logical_name, db, db_schema):

        self.db = db
        self.logical_name = logical_name

        # get server metadata
        server_meta = db.http_request(object_type='engineInput', object_name=logical_name, request='GET',
                                      raise_error=True)
        try:
            server_meta = json.loads(server_meta)
        except (TypeError, json.JSONDecodeError):
            server_meta = None
        if server_meta is None or 'exception' in server_meta:
            raise RuntimeError(('API call to server did not retrieve valid entity '
                                ' type properties for %s.' % logical_name))

        # functions
        kpis = server_meta.get('kpiDeclarations', [])
        if kpis is None:
            kpis = []
            logger.warning(('This entity type has no calculated kpis'))
            function_list = []
        else:
            function_list = [x['functionName'] for x in kpis]

        # build a dictionary of granularity objects keyed by granularity name
        self._granularities_dict = self.build_granularities(grain_meta=server_meta['granularities'],
                                                            freq_lookup=server_meta.get('frequencies'))

        # replace granularity name with granularity object
        for k in kpis:
            gran_name = k.get('granularity')
            if gran_name is not None:
                gran = self._granularities_dict.get(gran_name)
                if gran is not None:
                    k['granularity'] = gran
                else:
                    k['granularity'] = None
                    logger.warning('Invalid granularity %s for function %s. No granularity (None) will be used instead',
                                   gran_name, k['functionName'])

        # build a schedules dict keyed by freq

        self._schedules_dict = self.build_schedules(kpis)

        # turn function metadata into function objects
        (self._functions, self._invalid_stages, self._disabled_stages) = self.build_function_objects(kpis)

        #  map server properties to entitty type properties
        self._entity_type_id = server_meta['entityTypeId']
        self._db_schema = server_meta['schemaName']
        self._timestamp = server_meta['metricTimestampColumn']
        self._dimension_table_name = server_meta['dimensionsTable']

        #  set the data items metadata directly - no need to create cols
        #  as table is assumed to exist already since this is a
        #  server entity type

        #  _data_items is a list of dicts. elements are:
        #   name
        #   type: METRIC or DERIVED_METRIC
        #   columnName
        #   columnType: NUMBER, LITERAL, JSON, BOOLEAN, TIMESTAMP
        #   sourceTableName
        #   kpiFunctionDto: May have a all of the metadata for the KPI function
        #                   That produced the item
        #   parentDataItemName : None
        #   tags ['EVENT','DIMENSION']
        #   tranient

        self._data_items = server_meta['dataItems']

        #  constants
        #    These are arbitrary parmeter values provided by the
        #    server and copied onto the entity type

        params = {}
        c_meta = db.http_request(object_type='constants', object_name=logical_name, request='GET')
        try:
            c_meta = json.loads(c_meta)
        except (TypeError, json.JSONDecodeError):
            logger.warning(('API call to server did not retrieve valid entity type'
                            ' constant. No costants set.'))
        else:
            for p in c_meta:
                key = p['name']
                if isinstance(p['value'], dict):
                    value = p['value'].get('value', p['value'])
                else:
                    value = p['value']
                params[key] = value
                logger.debug('Retrieved server constant %s with value%s', key, value)

        params['_timestamp'] = self._timestamp
        params['_dimension_table_name'] = server_meta['dimensionsTable']

        # initialize entity type

        super().__init__(name=server_meta['metricsTableName'], db=db, **params)

    def build_function_objects(self, server_kpis):
        '''
        Create function objects from server kpi definitions
        '''

        functions = []
        invalid = []
        disabled = []
        valid_kpis = []

        for f in server_kpis:

            # skip disabled functions

            if not f.get('enabled', False):
                disabled.append(f)
            else:

                # find functions that must be replaced

                (granularity, replacement_metadata) = self.get_function_replacement_metadata(f)
                if replacement_metadata is not None:

                    obj = AggregateItems(input_items=[replacement_metadata.get('input').get('source')],
                                         aggregation_function=replacement_metadata.get('functionName'),
                                         output_items=[replacement_metadata.get('output').get('name')])
                    obj.granularity = replacement_metadata.get('granularity', None)
                    obj.schedule = replacement_metadata.get('schedule', None)
                    functions.append(obj)

                else:
                    valid_kpis.append(f)

        #  cache function catalog metadata in the db object
        valid_kpi_names = [x.get('functionName') for x in valid_kpis]
        self.db.load_catalog(install_missing=True, function_list=valid_kpi_names)

        for f in valid_kpis:

            # build function object using metadata

            (package, module, class_name) = self.db.get_catalog_module(f['functionName'])
            meta = {}
            mod = importlib.import_module('%s.%s' % (package, module))
            meta = {**meta, **f['input']}
            meta = {**meta, **f['output']}
            cls = getattr(mod, class_name)
            try:
                obj = cls(**meta)
            except TypeError as e:
                logger.warning(('Unable build %s object. The arguments are mismatched'
                                ' with the function metadata. You may need to'
                                ' re-register the function. %s' % (f['functionName'], str(e))))
                invalid.append(f)
            else:
                obj.granularity = f.get('granularity', None)
                obj.schedule = f.get('schedule', None)
                functions.append(obj)

        return (functions, invalid, disabled)


class LocalEntityType(EntityType):
    '''
    Entity type for local testing. No db connection required.
    '''

    is_local = True

    def __init__(self, name, columns=None, constants=None, granularities=None, functions=None, db=None, db_schema=None,
                 **kwargs):

        if columns is None:
            columns = []
        self.local_columns = list(self._build_cols_for_fns(functions=functions, columns=columns))

        args = []
        args.extend(self.local_columns)
        if not constants is None:
            args.extend(constants)
        if not functions is None:
            args.extend(functions)
        if not granularities is None:
            args.extend(granularities)

        if db_schema is not None:
            kwargs['_db_schema'] = db_schema

        super().__init__(name, db, *args, **kwargs)

    def _build_cols_for_fns(self, functions, columns=None):

        '''
        Get a list of dataframe column names referenced as function
        arguments

        functions is a list of function objects
        columns is a list of known sql alchemy column object

        returns a set of sql alchemy column objects
        This set contains the previously known columns and the
        required columns inferred from function inputs

        '''

        # build a dictionary of the known column objects
        if columns is None:
            columns = []
        cols = set(columns)
        cols_dict = {}
        for c in columns:
            cols_dict[c.name] = c

        # examine the function objects to discover columns not referenced in the dictionary
        if functions is not None:
            if not isinstance(functions, list):
                functions = [functions]
            for f in functions:
                # use the metadata about the function inputs to find all the
                # input data items
                # use the argument values to find the input item names
                (inputs, outputs) = f.build_ui()
                args = f._get_arg_metadata()
                for i in inputs:
                    if i.type_ == 'DATA_ITEM':
                        values = args[i.name]
                        if not isinstance(values, list):
                            values = [values]
                        for value in values:
                            existing_col = cols_dict.get(value, None)
                            if existing_col is None:
                                logger.warning(('Building column for function input %s with unknown type'
                                                'Assuming datatype of Float. To change the type, '
                                                'define a column called %s with the appropriate type'), value, value)
                                cols_dict[value] = Column(value.lower(), Float)

                    if 'EXPRESSION' in i.tags:
                        expression_cols = f.get_input_items()
                        for c in expression_cols:
                            existing_col = cols_dict.get(c, None)
                            if existing_col is None:
                                logger.warning(('Building column for expression input %s with unknown type'
                                                'Assuming datatype of Float. To change the type, '
                                                'define a column called %s with the appropriate type'), c, c)
                                cols_dict[c] = Column(c.lower(), Float)

            cols = set(cols_dict.values())

        return cols


class BaseCustomEntityType(EntityType):
    '''
    Base class for custom entity types
    '''

    timestamp = 'evt_timestamp'

    def __init__(self, name, db, columns=None, constants=None, granularities=None, functions=None,
                 dimension_columns=None, generate_days=0, generate_entities=None, drop_existing=False, db_schema=None,
                 description=None, output_items_extended_metadata=None, **kwargs):

        if columns is None:
            columns = []
        if constants is None:
            constants = []
        if functions is None:
            functions = []
        if dimension_columns is None:
            dimension_columns = []
        if granularities is None:
            granularities = []
        if output_items_extended_metadata is None:
            output_items_extended_metadata = {}

        self._columns = columns
        self._constants = constants
        self._functions = functions
        self._dimension_columns = dimension_columns
        self._output_items_extended_metadata = output_items_extended_metadata

        args = []
        args.extend(self._columns)
        args.extend(granularities)
        args.extend(constants)

        if description is None:
            description = self.__doc__

        params = {'_timestamp': self.timestamp, '_db_schema': db_schema, 'description': description,
                  'drop_existing': drop_existing}

        kwargs = {**params, **kwargs}

        super().__init__(name, db, *args, **kwargs)

        self.make_dimension(None,  # auto build name
                            *self._dimension_columns)

        if generate_days > 0:
            # classify stages is adds entity metdata to the stages
            # need to run it before executing any stage
            self.classify_stages()
            generators = [x for x in self._functions if x.is_data_generator]
            start = dt.datetime.utcnow() - dt.timedelta(days=generate_days)
            for g in generators:
                logger.debug(('Running generator %s with start date %s.'
                              ' Drop existing %s'), g.__class__.__name__, start, drop_existing)
                g.execute(df=None, start_ts=start, entities=generate_entities)

    def publish_kpis(self, raise_error=True):

        '''
        Publish the function instances assigned to this entity type to the AS Server
        '''

        if self.db is None:
            msg = ('Entity type has no db connection. Local entity types'
                   ' are not allowed to publish kpis ')
            raise ValueError(msg)

        export = []
        self.db.register_functions(self._functions, raise_error=raise_error)

        for s in self._functions:
            try:
                name = s.name
            except AttributeError:
                name = s.__class__.__name__
                logger.debug(('Function class %s has no name property.'
                              ' Using the class name'), name)

            try:
                args = s._get_arg_metadata()
            except AttributeError:
                msg = ('Attempting to publish kpis for an entity type.'
                       ' Function %s has no _get_arg_spec() method.'
                       ' It cannot be published') % name
                raise NotImplementedError(msg)

            # the entity type may have extended metadata
            # find relevant extended metadata and add it to argument values

            output_meta = {}
            for (a, value) in list(args.items()):
                if not isinstance(value, list):
                    arg_values = [value]
                else:
                    arg_values = value
                for av in arg_values:
                    if isinstance(av, str):
                        extended = self._output_items_extended_metadata.get(av, None)
                        if extended is not None:
                            output_meta[av] = extended
            if output_meta:
                args['outputMeta'] = output_meta

            metadata = {'name': name, 'args': args}
            export.append(metadata)

        logger.debug('Published kpis to entity type')
        logger.debug(export)

        response = self.db.http_request(object_type='kpiFunctions', object_name=self.logical_name, request='POST',
                                        payload=export, raise_error=raise_error)

        logger.debug(response)

        return response


class Granularity(object):
    '''
    Describe granularity level in terms of the pandas grouper that is used
    to aggregate to this grain and any custom calendar object that may have
    been used to create it.

    Parameters:
    -----------
    name: str

    dimensions: list of data items used as dimension keys in group by

    entity_id: str. column name used to group by entity id. None if not
    an entity level summary

    freq = Pandas frequency string

    custom_calendar_keys: list of strs containing custom calendar data item
    names to be grouped on

    custom_calendar: function object

    grouper: pandas Grouper object. Optional. If not provided, the
    grouper will be built using other arguments

    '''

    is_granularity = True

    def __init__(self, name, dimensions, timestamp, freq, entity_name, entity_id=None, table_name=None, grouper=None,
                 custom_calendar_keys=None, custom_calendar=None, description=None):

        self.name = name
        self.entity_name = entity_name
        self.timestamp = timestamp

        if table_name is None:
            table_name = (('dm_%s_%s') % (self.entity_name, self.name)).lower()
        self.table_name = table_name

        if dimensions is None:
            dimensions = []
        if custom_calendar_keys is None:
            custom_calendar_keys = []
        if description is None:
            description = self.name

        self.dimensions = dimensions
        self.entity_id = entity_id
        self.freq = freq
        self.custom_calendar = custom_calendar
        self.custom_calendar_keys = custom_calendar_keys
        self.description = description

        if grouper is None:
            grouper = build_grouper(freq=self.freq, timestamp=self.timestamp, entity_id=self.entity_id,
                                    dimensions=self.dimensions, custom_calendar_keys=self.custom_calendar_keys)

        self.grouper = grouper

    def align_df_to_start_date(self, df, min_date=None):

        '''
        Align a dataframe to the granularity by filtering out times periods that
        are incomplete, ie: started before the earliest date in the dataframe.

        example: consider a daily grain with a dateframe containing data
        from 2019/08/05 8:09am. After alignment the dataframe will be filtered
        to exclude timestamps before the start of the first complete day (2019/08/06).
        '''

        if min_date is None:
            min_date = df[self.timestamp].min()

        period_end = self.get_period_end(min_date)
        df = df[(df[self.timestamp] > period_end)]

        return (df, period_end)

    def get_period_end(self, date):

        '''
        Get the start date of the next period after <date>
        '''

        if self.custom_calendar is not None:
            result = self.custom_calendar.get_period_end(date)
        else:
            series = pd.DatetimeIndex(data=[date])
            rounded = series.round(self.freq)
            result = rounded - dt.timedelta(microseconds=1)
            result = result.min()

        return result

    def __str__(self):

        return self.name


class Model(object):
    '''
    Predictive model
    '''

    def __init__(self, name, estimator, estimator_name, params, features, target, eval_metric_name,
                 eval_metric_train=None, eval_metric_test=None, shelf_life_days=None, col_name=None):

        self.name = name
        self.target = target
        self.features = features
        self.estimator = estimator
        self.estimator_name = estimator_name
        self.params = params
        self.eval_metric_name = eval_metric_name
        self.eval_metric_train = eval_metric_train
        self.eval_metric_test = eval_metric_test
        self.shelf_life_days = shelf_life_days

        if col_name is None:
            col_name = '%s_predicted' % self.target

        self.col_name = col_name

        if self.estimator is None:
            self.trained_date = None
        else:
            self.trained_date = dt.datetime.utcnow()
        if self.trained_date is not None and self.shelf_life_days is not None:
            self.expiry_date = self.trained_date + dt.timedelta(days=self.shelf_life_days)
        else:
            self.expiry_date = None
        self.viz = {}

    def fit(self, df):
        self.estimator = self.estimator.fit(df[self.features], df[self.target])
        self.trained_date = dt.datetime.utcnow()
        self.eval_metric_train = self.score(df)
        if self.shelf_life_days is not None:
            self.expiry_date = self.trained_date + dt.timedelta(days=self.shelf_life_days)
        msg = 'trained model %s with evaluation metric value %s' % (self.name, self.eval_metric_train)
        logger.info(msg)
        return self.estimator

    def predict(self, df):
        result = self.estimator.predict(df[self.features])
        msg = 'predicted using model %s' % (self.name)
        logger.info(msg)
        return result

    def score(self, df):
        result = self.estimator.score(df[self.features], df[self.target])
        return result

    def test(self, df):
        self.eval_metric_test = self.score(df)
        msg = 'evaluated model %s with evaluation metric value %s' % (self.name, self.eval_metric_test)
        logger.info(msg)
        return self.eval_metric_test

    def __str__(self):
        out = {}
        output = ['name', 'target', 'features', 'estimator_name', 'eval_metric_name', 'eval_metric_train',
                  'eval_metric_test', 'trained_date', 'expiry_date']
        for o in output:
            try:
                out[o] = getattr(self, o)
            except AttributeError:
                out[o] = '_missing_'
        if out['trained_date'] is not None:
            out['trained_date'] = out['trained_date'].isoformat()
        if out['expiry_date'] is not None:
            out['expiry_date'] = out['expiry_date'].isoformat()
        return json.dumps(out, indent=1)
