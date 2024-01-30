# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import re
import logging
from collections import defaultdict

import pandas as pd
import numpy as np
import ibm_db
import datetime as dt

import iotfunctions.metadata as md
from iotfunctions.base import (BaseAggregator, BaseFunction)
from iotfunctions.util import log_data_frame, rollback_to_interval_boundary, UNIQUE_EXTENSION_LABEL
from iotfunctions import dbhelper


def asList(x):
    if not isinstance(x, list):
        x = [x]
    return x


def add_simple_aggregator_execute(cls, func_name):
    def fn(self, group):
        return self.execute(group)

    setattr(cls, func_name, fn)
    fn.__name__ = func_name


class Aggregation(BaseFunction):

    """
    Keyword arguments:
    ids -- the names of entity id columns, can be a string or a list of
           string. aggregation is always done first by entities unless
           this is not given (None) and it is not allowed to include any
           of entity id columns in sources
    """
    def __init__(self, dms, ids=None, timestamp=None, granularity=None, columns_with_scope=None, simple_aggregators=None,
                 complex_aggregators=None, direct_aggregators=None):

        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        self.dms = dms

        self.simple_aggregators = simple_aggregators if simple_aggregators is not None else []
        self.complex_aggregators = complex_aggregators if complex_aggregators is not None else []
        self.direct_aggregators = direct_aggregators if direct_aggregators is not None else []

        self.ids = ids
        if self.ids is not None:
           self.ids = asList(self.ids)
        else:
            self.ids = list()

        self.timestamp = timestamp

        self.logger.debug('aggregation_input_granularity=%s' % str(granularity))

        if granularity is not None and not isinstance(granularity, tuple):
            raise RuntimeError('argument granularity must be a tuple')

        self.frequency = None
        self.groupby = None
        self.entityFirst = True
        if granularity is not None:
            self.frequency, self.groupby, self.entityFirst, dummy = granularity

        if self.groupby is None:
            self.groupby = ()

        if self.groupby is not None and not isinstance(self.groupby, tuple):
            raise RuntimeError('argument granularity[1] must be a tuple')

        self.columns_with_scope = columns_with_scope if columns_with_scope is not None else {}

        self.logger.debug(f'aggregation_ids={str(self.ids)}, aggregation_timestamp={str(self.timestamp)}, '
                          f'simple_aggregators={str(self.simple_aggregators)}, '
                          f'complex_aggregators={str(self.complex_aggregators)}, '
                          f'aggregation_direct_aggregators={str(self.direct_aggregators)}, '
                          f'aggregation_frequency={str(self.frequency)}, aggregation_groupby={str(self.groupby)}, '
                          f'aggregation_entityFirst={str(self.entityFirst)}, '
                          f'columns_with_scope={str(columns_with_scope)}')

    def _agg_dict(self, df, simple_aggregators):
        numerics = df.select_dtypes(include=[np.number, bool]).columns.values

        agg_dict = defaultdict(list)
        for srcs, scope_sources, agg, name, scope, filtered_sources in simple_aggregators:
            # kohlmann srcs = asList(srcs)
            for src in srcs:
                if src in numerics:
                    agg_dict[src].append(agg)
                else:
                    numeric_only_funcs = {'sum', 'mean', 'std', 'var', 'prod', Sum, Mean, StandardDeviation, Variance,
                                          Product}
                    if agg not in numeric_only_funcs:
                        agg_dict[src].append(agg)

        return agg_dict

    def execute(self, df, start_ts=None, end_ts=None, entities=None):

        df = df.reset_index()

        # The index has been moved to the columns and the index levels ('id', event timestamp, dimensions) can now be
        # used as a starting point of an aggregation. Provide index level 'id' - if it exists - as 'entity_id' as well.
        if 'id' in df.columns:
            df['entity_id'] = df['id']

        # Gather columns which are used to determine the groups
        group_base = []
        group_base_names = []
        if len(self.ids) > 0 and self.entityFirst:
            group_base.extend(self.ids)
            group_base_names.extend(self.ids)

        if self.timestamp is not None and self.frequency is not None:
            if self.frequency == 'W':
                # 'W' by default use right label and right closed
                group_base.append(pd.Grouper(key=self.timestamp, freq=self.frequency, label='left', closed='left'))
            else:
                # other alias seems to not needing to special handle
                group_base.append(pd.Grouper(key=self.timestamp, freq=self.frequency))
            group_base_names.append(self.timestamp)

        if self.groupby is not None and len(self.groupby) > 0:
            group_base.extend(self.groupby)
            group_base_names.extend(self.groupby)

        self.logger.debug(f'group_base={str(group_base)}, '
                          f'group_base_names={str(group_base_names)}')

        # Handle scopes: Duplicate source column and apply the scope to the duplicate
        log_messages = []
        for input_col_name, scope_and_new_col in self.columns_with_scope.items():
            for scope, new_input_col_name in scope_and_new_col:
                df[new_input_col_name] = self._apply_scope(df, input_col_name, scope)
                log_messages.append(f"({input_col_name} --> {new_input_col_name})")

        self.logger.info(f"A scope has been applied on the following columns: {str(log_messages)}")

        # Get list of all input columns (simple and complex aggregator only)
        all_input_col_names = set()
        for aggregators in [self.simple_aggregators, self.complex_aggregators]:
            for func, input_col_names, output_col_names in aggregators:
                all_input_col_names.update(input_col_names)

        self.logger.debug(
            f"The following columns are required for simple and complex aggregators: {str(all_input_col_names)}")

        # Check for missing columns
        missing_col_names = all_input_col_names.difference(df.columns)
        if len(missing_col_names) > 0:
            raise RuntimeError(f"The following columns are required as input for the simple and complex aggregation "
                               f"functions but are missing in the data frame: {str(list(missing_col_names))}. "
                               f"Available columns in data frame: {str(list(df.columns))}")

        # Split data frame into groups
        groups = df.groupby(group_base)

        all_dfs = []

        ###############################################################################
        # simple aggregators
        ###############################################################################

        # Define named aggregations
        named_aggregations = {}
        log_messages = []
        for func, input_col_names, output_col_names in self.simple_aggregators:
            for input_col_name, output_col_name in zip(input_col_names, output_col_names):
                named_aggregations[output_col_name] = pd.NamedAgg(column=input_col_name, aggfunc=func)
                log_messages.append(f"(input={input_col_name} --> agg_func={func.__name__}() --> output={output_col_name})")
        self.logger.info(f"input/output relationship for simple aggregators: {str(log_messages)}")

        # Aggregate
        if len(named_aggregations) > 0:
            agg_df_simple = groups.agg(**named_aggregations)
            log_data_frame('Data frame after application of simple aggregators', agg_df_simple)

            all_dfs.append(agg_df_simple)

        ###############################################################################
        # complex aggregators
        ###############################################################################

        for func, input_col_names, output_col_names in self.complex_aggregators:

            self.logger.info('executing complex aggregation function - sources %s' % str(input_col_names))
            self.logger.info('executing complex aggregation function - output %s' % str(output_col_names))
            df_apply = groups.apply(func)

            # Some aggregation functions return None instead of an empty data frame. Therefore the result of
            # groups.apply() can be an empty data frame without columns and without index when all function calls
            # returned None. We take corrective action and build a new empty dataframe with the expected columns and
            # the expected index including level names
            if df_apply.empty and df_apply.columns.empty:

                # Build empty index with correct level names
                if len(group_base_names) > 1:
                    tmp_array = [[] for i in range(len(group_base_names))]
                    new_index = pd.MultiIndex.from_arrays(tmp_array, names=group_base_names)
                else:
                    new_index = pd.Index([], name=group_base_names[0])

                # Build data frame with index and expected columns
                df_apply = pd.DataFrame([], columns=output_col_names, index=new_index)

                # Cast columns in data frame to the expected type
                column_types = {}
                for name in output_col_names:
                    source_metadata = self.dms.data_items.get(name)

                    if source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_NUMBER:
                        tmp_type = float
                    elif source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_BOOLEAN:
                        tmp_type = bool
                    elif source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_TIMESTAMP:
                        tmp_type = 'datetime64[ns]'
                    else:
                        tmp_type = str

                    column_types[name] = tmp_type

                df_apply = df_apply.astype(dtype=column_types, copy=False)

            all_dfs.append(df_apply)

            log_data_frame('func=%s, aggregation_df_apply' % str(func), df_apply)

        ###############################################################################
        # direct aggregators
        ###############################################################################
        for func, input_col_names, output_col_names in self.direct_aggregators:

            self.logger.info('executing direct aggregation function - sources %s' % str(input_col_names))
            self.logger.info('executing direct aggregation function - output %s' % str(output_col_names))

            df_direct = func(df=df, group_base=group_base, group_base_names=group_base_names, start_ts=start_ts, end_ts=end_ts, entities=entities)
            if df_direct.empty and df_direct.columns.empty:
                for name in output_col_names:
                    df_direct[name] = None

                    source_metadata = self.dms.data_items.get(name)
                    if source_metadata is None:
                        continue

                    if source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_NUMBER:
                        df_direct = df_direct.astype({name: float})
                    elif source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_BOOLEAN:
                        df_direct = df_direct.astype({name: bool})
                    elif source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_TIMESTAMP:
                        df_direct = df_direct.astype({name: 'datetime64[ns]'})
                    else:
                        df_direct = df_direct.astype({name: str})

            all_dfs.append(df_direct)

            log_data_frame('func=%s, aggregation_df_direct' % str(func), df_direct)

        ###############################################################################
        # concat all results
        ###############################################################################
        df = pd.concat(all_dfs, axis=1)

        # Corrective action: pd.concat() removes name from Index when we only have one level. There is no issue for
        # MultiIndex which is used for two and more levels
        if len(group_base_names) == 1:
            df.index.names = group_base_names

        log_data_frame('aggregation_final_df', df)

        return df

    def _apply_scope(self, df, input_col_name, scope):
        eval_expression = ""
        if scope.get('type') == 'DIMENSIONS':
            self.logger.debug('Applying Dimensions Scope')
            dimension_count = len(scope.get('dimensions'))
            for dimension_filter in scope.get('dimensions'):
                dimension_name = dimension_filter['name']
                dimension_value = dimension_filter['value']
                dimension_count -= 1
                if isinstance(dimension_value, str):
                    dimension_value = [dimension_value]
                else:
                    # Convert to list explicitly to guarantee subsequent 'str(dimension_value)' returns a proper
                    # string. Counter example: str(dict.values()) returns "dict_values([...])"
                    dimension_value = list(dimension_value)
                eval_expression += 'df[\'' + dimension_name + '\'].isin(' + str(dimension_value) + ')'
                eval_expression += ' & ' if dimension_count != 0 else ''
        else:
            self.logger.debug('Applying Expression Scope')
            eval_expression = scope.get('expression')
            if eval_expression is not None and '${' in eval_expression:
                eval_expression = re.sub(r"\$\{(\w+)\}", r"df['\1']", eval_expression)
        self.logger.debug('Final Scope Mask Expression {}'.format(eval_expression))

        scope_mask = eval(eval_expression)

        return df[input_col_name].where(scope_mask)


def _general_aggregator_input():
    return {'name': 'source', 'description': 'Select the data item that you want to use as input for your calculation.',
            'type': 'DATA_ITEM', 'required': True, }.copy()

'''
# How to run

# import stuff in this file
from iotfunctions.aggregate import (Aggregation, add_simple_aggregator_execute)

# choose aggrator function
func = bif.AggregateWithExpression

# set up parameter list
params_dict = {}
params_dict['input_items'] = 'score'  # input column
params_dict['output_items'] = 'score_out'  # output_column
params_dict['expression'] = 'x.max()'

# set name for the function pandas grouper should run
func_name = 'execute_AggregateWithExpression'
# add func_name as alias for the aggregate method
add_simple_aggregator_execute(func, func_name)
# turn func into a closure
func_clos = getattr(func(**params_dict), func_name)

# instatiate an Aggregation object with entity index, timestamp index,
# desired granularity and a (short) chain of aggregators
# granularity is a quadruple with frequency, list of columns to group, boolean whether to go by entity and
#   entity id (which seems to be ignored)
# simple aggregator is a list of triplets with column name, closure as above and a name
agg_fct = Aggregation(None, ids=['entity'], timestamp='timestamp', granularity=('T', ('score',), True, 16623),
                    simple_aggregators=[('score', func_clos, 'AggWithExpr')])

# Execute the aggregator
jobsettings = { 'db': db, '_db_schema': 'public', 'save_trace_to_file' : True}
et = agg_fct._build_entity_type(columns = [Column('score',Float())], **jobsettings)
df_agg = agg_fct.execute(df=df_input)


'''


def _number_aggregator_input():
    input_item = _general_aggregator_input()
    input_item['dataType'] = 'NUMBER'
    return input_item


def _no_datatype_aggregator_output():
    return {'name': 'name',
            'description': 'Enter a name for the data item that is produced as a result of this calculation.'}.copy()


def _general_aggregator_output():
    output_item = _no_datatype_aggregator_output()
    output_item['dataTypeFrom'] = 'source'
    return output_item


def _number_aggregator_output():
    output_item = _no_datatype_aggregator_output()
    output_item['dataType'] = 'NUMBER'
    return output_item


def _generate_metadata(cls, metadata):
    common_metadata = {'name': cls.__name__, 'moduleAndTargetName': '%s.%s' % (cls.__module__, cls.__name__),
                       'category': 'AGGREGATOR', 'input': [_general_aggregator_input()],
                       'output': [_general_aggregator_output()]}
    common_metadata.update(metadata)
    return common_metadata


class Aggregator(BaseAggregator):
    def __init__(self):
        super().__init__()


class SimpleAggregator(Aggregator):
    is_simple_aggregator = True

    def __init__(self):
        super().__init__()


class ComplexAggregator(Aggregator):
    is_complex_aggregator = True

    def __init__(self):
        super().__init__()

class DirectAggregator(Aggregator):
    is_direct_aggregator = True

    def __init__(self):
        super().__init__()

class Sum(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Calculates the sum of the values in your data set.',
                                        'input': [_number_aggregator_input(), {'name': 'min_count',
                                                                               'description': 'The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.',
                                                                               'type': 'CONSTANT', 'required': False,
                                                                               'dataType': 'NUMBER'}],
                                        'output': [_number_aggregator_output()]})

    def __init__(self, min_count=None):
        if min_count is None:
            self.min_count = 1
        else:
            self.min_count = min_count

    def execute(self, group):
        return group.sum(min_count=self.min_count)


class Minimum(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Identifies the minimum value in your data set.'})

    def __init__(self):
        pass

    def execute(self, group):
        return group.min(skipna=True)


class Maximum(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Identifies the maximum value in your data set.'})

    def __init__(self):
        pass

    def execute(self, group):
        return group.max()


class Mean(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Calculates the mean of the values in your data set.',
                                        'input': [_number_aggregator_input()], 'output': [_number_aggregator_output()]})

    def __init__(self):
        super().__init__()

    def execute(self, group):
        return group.mean()


class Median(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Indentifies the median of the values in your data set.'})

    def __init__(self):
        pass

    def execute(self, group):
        return group.median()


class Count(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Counts the number of values in your data set.',
                                        'input': [_general_aggregator_input(), {'name': 'min_count',
                                                                                'description': 'The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.',
                                                                                'type': 'CONSTANT', 'required': False,
                                                                                'dataType': 'NUMBER'}],
                                        'output': [_number_aggregator_output()], 'tags': ['EVENT']})

    def __init__(self, min_count=None):
        if min_count is None:
            self.min_count = 1
        else:
            self.min_count = min_count

    def execute(self, group):
        cnt = group.count()
        return cnt if cnt >= self.min_count else None


class DistinctCount(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Counts the number of distinct values in your data set.',
                                        'input': [_general_aggregator_input(), {'name': 'min_count',
                                                                                'description': 'The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.',
                                                                                'type': 'CONSTANT', 'required': False,
                                                                                'dataType': 'NUMBER'}],
                                        'output': [_number_aggregator_output()], 'tags': ['EVENT']})

    def __init__(self, min_count=None):
        if min_count is None:
            self.min_count = 1
        else:
            self.min_count = min_count

    def execute(self, group):
        cnt = len(group.dropna().unique())
        return cnt if cnt >= self.min_count else None


class StandardDeviation(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls,
                                  {'description': 'Calculates the standard deviation of the values in your data set.',
                                   'input': [_number_aggregator_input()], 'output': [_number_aggregator_output()]})

    def __init__(self):
        pass

    def execute(self, group):
        return group.std()


class Variance(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Calculates the variance of the values in your data set.',
                                        'input': [_number_aggregator_input()], 'output': [_number_aggregator_output()]})

    def __init__(self):
        pass

    def execute(self, group):
        return group.var()


class Product(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Calculates the product of the values in your data set.',
                                        'input': [_number_aggregator_input(), {'name': 'min_count',
                                                                               'description': 'The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.',
                                                                               'type': 'CONSTANT', 'required': False,
                                                                               'dataType': 'NUMBER'}],
                                        'output': [_number_aggregator_output()]})

    def __init__(self, min_count=None):
        if min_count is None:
            self.min_count = 1
        else:
            self.min_count = min_count

    def execute(self, group):
        return group.prod(min_count=self.min_count)


class First(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls,
                                  {'description': 'Identifies the first value in your data set.', 'tags': ['EVENT']})

    def __init__(self):
        pass

    def execute(self, group):

        first = None
        group = group.dropna()

        if not group.empty:
            if isinstance(group, pd.Series):
                first = group.iat[0]
            elif isinstance(group, pd.DataFrame):
                first = group.iat[0, 0]

        return first


class Last(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls,
                                  {'description': 'Identifies the last value in your data set.', 'tags': ['EVENT']})

    def __init__(self):
        pass

    def execute(self, group):

        last = None
        group = group.dropna()

        if not group.empty:
            if isinstance(group, pd.Series):
                last = group.iat[-1]
            elif isinstance(group, pd.DataFrame):
                last = group.iat[-1, 0]

        return last


class AggregateWithCalculation(SimpleAggregator):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Create aggregation using expression on a data item.',
                                        'input': [_general_aggregator_input(), {'name': 'expression',
                                                                                'description': 'The expression. Use ${GROUP} to reference the current grain. All Pandas Series methods can be used on the grain. For example, ${GROUP}.max() - ${GROUP}.min().',
                                                                                'type': 'CONSTANT', 'required': True,
                                                                                'dataType': 'LITERAL'}],
                                        'output': [_no_datatype_aggregator_output()], 'tags': ['EVENT', 'JUPYTER']})

    def __init__(self, expression=None):
        if expression is None or not isinstance(expression, str):
            raise RuntimeError("argument expression must be provided and must be a string")

        self.expression = expression

    def execute(self, group):
        return eval(re.sub(r"\${GROUP}", r"group", self.expression))


class CsaUserCount(DirectAggregator):

    KPI_FUNCTION_NAME="CiscoSpaceAdapter_UserCount"
    CSA_USER_PRESENCE_EVENT_TYPE = "UserPresenceEventType"
    CSA_USER_PRESENCE_EVENT_TYPE_USER_ENTRY = "USER_ENTRY_EVENT"
    CSA_USER_PRESENCE_EVENT_TYPE_USER_EXIT = "USER_EXIT_EVENT"
    CSA_USER_PRESENCE_EVENT_TYPE_IS_ACTIVE = "USER_IS_ACTIVE"
    CSA_USER_PRESENCE_EVENT_TYPE_IS_INACTIVE = "USER_IS_INACTIVE"

    def __init__(self, event_type, user_presence_event_type, was_active, start_of_calculation=None, name=None):
        super().__init__()
        if event_type is not None and len(event_type) > 0 and \
                user_presence_event_type is not None and len(user_presence_event_type) > 0 and \
                was_active is not None and len(was_active) > 0:
            self.event_type = event_type
            self.user_presence_event_type = user_presence_event_type
            self.was_active = was_active
        else:
            raise RuntimeError(f"Function {self.KPI_FUNCTION_NAME} requires the parameters event_type, "
                               f"user_presence_event_type and was_active but at least one is empty: "
                               f"event_type={event_type}, user_presence_event_type={user_presence_event_type}, "
                               f"was_active={was_active}")

        if start_of_calculation is not None:
            try:
                self.start_of_calculation = pd.Timestamp(start_of_calculation, unit='ms')
            except Exception as ex:
                raise RuntimeError(f"The optional parameter start_of_calculation of function {self.KPI_FUNCTION_NAME} "
                                   f"cannot be converted to a timestamp: start_of_calculation={start_of_calculation}") from ex
        else:
            self.start_of_calculation = None

        if name is not None and len(name) > 0:
            self.output_name = name
        else:
            raise RuntimeError(f"No name was provided for the metric which is calculated by function {self.KPI_FUNCTION_NAME}: name={name}")

    def execute(self, df, group_base, group_base_names, start_ts=None, end_ts=None, entities=None):

        # Schema name and table name of result table for sql statement
        sql_schema_name = self.dms.schema
        sql_quoted_schema_name = dbhelper.quotingSchemaName(sql_schema_name, self.dms.is_postgre_sql)

        sql_table_name = self.dms.entity_type_obj._data_items.get(self.output_name).get('sourceTableName')
        sql_quoted_table_name = dbhelper.quotingTableName(sql_table_name, self.dms.is_postgre_sql)

        # Find data item representing the result of this KPI function
        result_data_item = self.dms.entity_type_obj._data_items.get(self.output_name)

        # Find granularity_set/frequency of this aggregation
        granulatity_set_id = result_data_item.get('kpiFunctionDto').get('granularitySetId')
        granularity_set = None
        for granu in self.dms.granularities.values():
            if granu is not None and granu[3] == granulatity_set_id:
                granularity_set = granu
                break
        if granularity_set is None:
            raise RuntimeError(f"Granularity with id={granulatity_set_id} could not be found in configuration")

        agg_frequency = granularity_set[0]
        if agg_frequency is None:
            raise RuntimeError("Definition of granularity has no frequency")

        # Find time range covered by this pipeline run (align start and end date with grain boundaries)
        aligned_run_end = rollback_to_interval_boundary(self.dms.launch_date, agg_frequency)
        if self.dms.previous_launch_date is not None:
            aligned_run_start = rollback_to_interval_boundary(self.dms.previous_launch_date, agg_frequency)
        else:
            # No previous execution date available (first pipeline run for this resource type). Fall back to earliest
            # data event in input data frame
            time_min = df[group_base_names[1]].min()
            if pd.notna(time_min):
                aligned_run_start = rollback_to_interval_boundary(time_min, agg_frequency)
            else:
                # Empty data frame: Nothing to process, i.e. time range covered by this pipeline is zero
                aligned_run_start = aligned_run_end

        # Find any explicitly defined backtrack for this KPI function
        backtrack = result_data_item.get('kpiFunctionDto').get('backtrack')
        if backtrack is not None and len(backtrack) > 0:
            # backtrack = complete_backtrack_setting(backtrack)       # kohlmann todo: is this step required when rollback is applied lateron????
            backtrack_start = aligned_run_end - pd.tseries.offsets.DateOffset(**backtrack)
            #backtrack_start = rollback_to_interval_boundary(backtrack_start, agg_frequency)
        else:
            backtrack_start = None

        # Determine the following:
        #   1) Time range [aligned_cycle_start, aligned_cycle_end]: aligned start and end of this cycle
        #   2) aligned_calc_start: Point in time from which UserCount is supposed to be calculated (it will be aligned with grain boundaries).
        if self.dms.running_with_backtrack:
            # Pipeline runs in BackTrack mode.
            # Hint: start_ts is already aligned with grain boundaries
            #       end_ts is aligned with grain boundaries except for the last cycle
            aligned_cycle_start = rollback_to_interval_boundary(start_ts, agg_frequency)
            aligned_cycle_end = rollback_to_interval_boundary(end_ts, agg_frequency)
            if backtrack_start is None:
                aligned_calc_start = aligned_run_start
            else:
                aligned_calc_start = backtrack_start

        else:
            # Pipeline runs in CheckPoint mode (There is only one cycle)
            aligned_cycle_start = aligned_run_start
            aligned_cycle_end = aligned_run_end
            aligned_calc_start = aligned_cycle_start

        s_agg_result = None
        if aligned_cycle_start < aligned_cycle_end:
            # When aligned_calc_start <= aligned_cycle_start then we calculate all UserCount values in this cycle. But
            # when aligned_calc_start > aligned_cycle_start then we **do not** calculate all UserCount values in this
            # cycle. Fetch those non-calculated UserCount values from output table to complete the internal data frame
            # which might be used later on by other KPI functions
            s_missing_result_values = None
            if aligned_calc_start > aligned_cycle_start:
                # Load missing UserCount values from output table (this only happens in BackTrack mode)
                sql_statement = f'SELECT "VALUE_N", "TIMESTAMP", "ENTITY_ID" FROM {sql_quoted_schema_name}.{sql_quoted_table_name} ' \
                                f'WHERE KEY = ? AND TIMESTAMP >= ? AND TIMESTAMP < ? ORDER BY "ENTITY_ID", "TIMESTAMP" ASC'

                stmt = ibm_db.prepare(self.dms.db_connection, sql_statement)
                try:
                    ibm_db.bind_param(stmt, 1, self.output_name)
                    ibm_db.bind_param(stmt, 2, aligned_cycle_start)
                    ibm_db.bind_param(stmt, 3, min(aligned_calc_start, aligned_cycle_end))
                    ibm_db.execute(stmt)
                    row = ibm_db.fetch_tuple(stmt)
                    result_data = []
                    result_index = []
                    while row is not False:
                        result_data.append(row[0])
                        result_index.append((row[2], row[1]))
                        row = ibm_db.fetch_tuple(stmt)

                    if len(result_data) > 0:
                        s_missing_result_values = pd.Series(data=result_data, index=pd.MultiIndex.from_tuples(tuples=result_index, names=group_base_names), name=self.output_name)

                except Exception as ex:
                    raise Exception(
                        f'Retrieval of result values failed with sql statement "{sql_statement}"') from ex
                finally:
                    ibm_db.free_result(stmt)

            # Because UserCount is a cumulative measure we have to fetch the latest available UserCount values from
            # output table. This step is not required for cycles in which we do not calculate any UserCount values.
            if aligned_calc_start < aligned_cycle_end:
                s_start_result_values = None

                # Read just one result value per device right before aligned_calc_start.
                sql_statement = f'WITH PREVIOUS_VALUES AS ' \
                                    f'(SELECT "VALUE_N", "ENTITY_ID", ' \
                                    f'ROW_NUMBER() OVER ( PARTITION BY "ENTITY_ID" ORDER BY "TIMESTAMP" DESC) ROW_NUM ' \
                                    f'FROM {sql_quoted_schema_name}.{sql_quoted_table_name} ' \
                                    f'WHERE KEY = ? AND TIMESTAMP < ?) ' \
                                f'SELECT "VALUE_N", "ENTITY_ID" FROM PREVIOUS_VALUES WHERE ROW_NUM = 1'

                stmt = ibm_db.prepare(self.dms.db_connection, sql_statement)
                try:
                    ibm_db.bind_param(stmt, 1, self.output_name)
                    ibm_db.bind_param(stmt, 2, max(aligned_calc_start, aligned_cycle_start))
                    ibm_db.execute(stmt)
                    row = ibm_db.fetch_tuple(stmt)
                    result_data = []
                    result_index = []
                    while row is not False:
                        result_data.append(row[0])
                        result_index.append(row[1])
                        row = ibm_db.fetch_tuple(stmt)
                    if len(result_data) > 0:
                        s_start_result_values = pd.Series(data=result_data, index=pd.Index(result_index, name=group_base_names[0]))
                except Exception as ex:
                    raise Exception(
                        f'Retrieval of previous result value failed with sql statement "{sql_statement}"') from ex
                finally:
                    ibm_db.free_result(stmt)

                # Create new column 'self.output_name' in data frame df holding 1 for user-count up, -1 for user-count down else 0.
                # The logic behind user-count is as follows:
                #   User-count up:
                #           1) user-entry event with was_active==False
                #           2) user-is-active event with was_active==False
                #   User-count down:
                #           1) user-exit event with was_active==True
                #           2) user-is-inactive event with was_active==True
                #   All other combinations do not contribute to user-count:
                #           1) user-entry event with was_active==True
                #           2) user-is-active event with was_active==True
                #           3) user-exit event with was_active==False
                #           4) user-is-inactive event with was_active==False
                s_event_type = (df[self.event_type] == self.CSA_USER_PRESENCE_EVENT_TYPE)
                s_user_entry = (df[self.user_presence_event_type] == self.CSA_USER_PRESENCE_EVENT_TYPE_USER_ENTRY)
                s_user_exit = (df[self.user_presence_event_type] == self.CSA_USER_PRESENCE_EVENT_TYPE_USER_EXIT)
                s_is_active = (df[self.user_presence_event_type] == self.CSA_USER_PRESENCE_EVENT_TYPE_IS_ACTIVE)
                s_is_inactive = (df[self.user_presence_event_type] == self.CSA_USER_PRESENCE_EVENT_TYPE_IS_INACTIVE)

                # Careful processing is required for boolean input metric because of null-values in addition to
                # true/false. Make sure that entries with a null-value like 'None' and 'np.nan' do not lead to a
                # count event.
                # Hint: (s_tmp == True) also covers (s_tmp == np.True_) and (s_tmp == 1) properly
                #       (s_tmp == False) also covers (s_tmp == np.False_) and (s_tmp == 0) properly
                s_tmp = df[self.was_active]
                s_was_active = np.where((s_tmp == True) | (s_tmp == 'True'), True, False)
                s_was_inactive = np.where((s_tmp == False) | (s_tmp == 'False'), True, False)

                s_user_count_up = (s_event_type & (s_user_entry | s_is_active) & s_was_inactive)
                s_user_count_down = (s_event_type & (s_user_exit | s_is_inactive) & s_was_active)
                df[self.output_name] = 0
                df[self.output_name].mask(s_user_count_up, 1, inplace=True)
                df[self.output_name].mask(s_user_count_down, -1, inplace=True)

                # We only want to calculate UserCount between aligned_calc_start and aligned_cycle_end. Shrink data
                # frame to required time range.
                df_calc = df[[*group_base_names, self.output_name]]
                df_calc = df_calc[(df_calc[group_base_names[1]] >= max(aligned_calc_start, aligned_cycle_start)) & (df_calc[group_base_names[1]] < aligned_cycle_end)]

                # Aggregate new column to get result metric. Result metric has name self.output_name in data frame df_agg_result.
                # Columns in group_base_names go into index of df_agg_result
                df_agg_result = df_calc.groupby(group_base).sum()

                # Remove new column from data frame df again because df has global scope
                df.drop(columns=[self.output_name], inplace=True)

                # df_agg_result only holds values for aggregation intervals for which we had data events. Therefore,
                # create data frame with an index which holds entries for each aggregation interval between
                # aligned_calc_start (including) and aligned_cycle_end (excluding)
                time_index = pd.date_range(start=max(aligned_calc_start, aligned_cycle_start), end=(aligned_cycle_end - pd.Timedelta(value=1, unit='ns')), freq=agg_frequency)
                full_index = pd.MultiIndex.from_product([df_agg_result.index.get_level_values(level=group_base_names[0]).unique(), time_index], names=group_base_names)
                tmp_col_name = self.output_name + UNIQUE_EXTENSION_LABEL
                full_df = pd.DataFrame(data={tmp_col_name: 0}, index=full_index)
                df_agg_result = df_agg_result.join(full_df, how='right')
                df_agg_result[self.output_name].mask(df_agg_result[self.output_name].isna(), other=df_agg_result[tmp_col_name], inplace=True)
                df_agg_result.drop(columns=tmp_col_name, inplace=True)

                if s_start_result_values is not None:
                    # Add previous result(s) to first value(s) in df_agg_result because we want to calculate the cumulative sum
                    s_agg_result = df_agg_result[self.output_name].groupby(level=group_base_names[0], sort=False).transform(self.add_to_first, s_start_result_values)
                else:
                    # No previous values available (cumulative sum starts at 0)
                    s_agg_result = df_agg_result[self.output_name]

                s_agg_result = s_agg_result.groupby(level=group_base_names[0], sort=False).cumsum()

            # Add result values which has not been calculated in this run but were taken from output table
            if s_missing_result_values is not None:
                if s_agg_result is None or s_agg_result.empty:
                    s_agg_result = s_missing_result_values
                else:
                    s_agg_result = pd.concat([s_missing_result_values, s_agg_result], verify_integrity=True)

            if s_agg_result is not None:
                s_agg_result.sort_index(ascending=True, inplace=True)

        else:
            # Nothing to do because range in which UserCount must be calculated is zero or negative.
            pass

        if s_agg_result is None:
            s_agg_result = pd.Series([], index=pd.MultiIndex.from_arrays([[], []], names=group_base_names), dtype='int64')

        return s_agg_result

    def add_to_first(self, sub_s, value_map):
        if sub_s.name in value_map.index:
            sub_s.iat[0] = sub_s.iat[0] + value_map.at[sub_s.name]
        return sub_s

    # # We need one value of result metric which is not covered in this pipeline run but was calculated in previous
    # # pipeline run. We find this value in the corresponding Monitor output table
    # if df_agg_result.shape[0] > 0:
    #
    #     oldest_timestamp = df_agg_result.index.get_level_values(timestamp_name).values[0]
    #     oldest_timestamp = pd.Timestamp(oldest_timestamp).strftime('%Y-%m-%d-%H:%M:%S:%f')
    #
    #     # Database table contains columns "ENTITY_ID", "TIMESTAMP", "KEY", "VALUE_N"
    #     sql_statement = f'SELECT "VALUE_N", "ENTITY_ID", "TIMESTAMP", "KEY"  FROM {quoted_schema_name}.{quoted_table_name} ' \
    #                     f'WHERE KEY = ? AND TIMESTAMP < ? ORDER BY "TIMESTAMP" DESC FETCH FIRST 1 ROW ONLY'
    #
    #     stmt = ibm_db.prepare(self.dms.db_connection, sql_statement)
    #
    #     # Retrieve result from previous pipeline run for metric given in output_name
    #     missing_result_value = None
    #     try:
    #         ibm_db.bind_param(stmt, 1, self.output_name)
    #         ibm_db.bind_param(stmt, 2, oldest_timestamp)
    #         ibm_db.execute(stmt)
    #         row = ibm_db.fetch_tuple(stmt)
    #         if row is not False:
    #             missing_result_value = row[0]
    #     except Exception as ex:
    #         raise Exception(f'Retrieval of previous row failed with sql statement "{sql_statement}"') from ex
    #     finally:
    #         ibm_db.free_result(stmt)
    #
    #     # Add previous result to currently calculated values
    #     if missing_result_value is not None:
    #         df_agg_result[self.output_name].iat[0] = df_agg_result[self.output_name].iat[0] + missing_result_value
    #     df_agg_result[self.output_name] = df_agg_result[self.output_name].cumsum()
    #
    # # Drop the result value for the latest aggregation interval in this pipeline run (when it exists). The
    # # calculation for the latest interval is incomplete because we do not have - in general - all data events for
    # # this interval in data frame.
    # # First we have to determine the grain to find the timestamp of the latest interval
    # granulatity_set_id = self.dms.entity_type_obj._data_items.get(self.output_name).get('kpiFunctionDto').get('granularitySetId')
    # granularity_set = None
    # for granu in self.dms.granularities.values():
    #     if granu is not None and granu[3] == granulatity_set_id:
    #         granularity_set = granu
    #         break
    # if granularity_set is None:
    #     raise RuntimeError(f"Granularity with id={granulatity_set_id} could not be found in configuration")
    #
    # last_agg_interval_start_ts = rollback_to_interval_boundary(end_ts, granularity_set[0])
    # s_last_timestamp = (df_agg_result.index.get_level_values(timestamp_name) == last_agg_interval_start_ts)
    # df_agg_result[self.output_name].mask(s_last_timestamp, np.nan)
