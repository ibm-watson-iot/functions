# *****************************************************************************
# © Copyright IBM Corp. 2018, 2024  All Rights Reserved.
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
from pandas.api.types import is_string_dtype, is_numeric_dtype, is_bool_dtype, is_datetime64_ns_dtype, is_object_dtype
import numpy as np

import iotfunctions.metadata as md
from iotfunctions.base import (BaseAggregator, BaseFunction)
from iotfunctions.util import log_data_frame


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

    def execute(self, df):

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
            if agg_df_simple.empty:
                # Corrective action for unexpected behaviour of pandas: The user-defined (from the pandas perspective)
                # aggregation functions like our 'Count' function are never called when the corresponding dataframe is
                # empty. Although this behaviour (which was introduced in pandas 1.5.x) helps to avoid exceptions for
                # aggregation functions which are not designed to handle empty dataframes properly it causes issues
                # for function like 'Count':
                # The Count function returns a result of type 'int64' independent of the input type. This exchange of
                # type does not happen when our count function is never called. Therefore, pandas returns a (empty)
                # result column with the type of the input instead of type 'int64'. By the way, pandas build-in count()
                # function does not show this issue!
                # We take corrective action for all simple aggregators by enforcing the correct result type in case of
                # an empty dataframe

                # Determine the output metrics whose type must be changed. Then assign to the corresponding columns in
                # dataframe a dummy value of the expected type to enforce the conversion of type. This approach is much
                # simpler in case of an empty dataframe than using dataframe.astype() which throws an exception,
                # for example, when a datetime64[ns] is converted to a float64.
                for output_metric_name in agg_df_simple.columns:
                    output_metric = self.dms.data_items.get(output_metric_name)
                    if output_metric is not None:
                        output_metric_type = output_metric.get('columnType')
                        column_type = agg_df_simple[output_metric_name].dtype
                        new_type = None
                        if output_metric_type == "BOOLEAN":
                            if not is_bool_dtype(column_type):
                                new_type = 'boolean'
                                agg_df_simple[output_metric_name] = True
                        elif output_metric_type == "NUMBER":
                            if not is_numeric_dtype(column_type):
                                new_type = 'float64'
                                agg_df_simple[output_metric_name] = 1.0
                        elif output_metric_type == "LITERAL":
                            if not is_string_dtype(column_type):
                                new_type = 'str'
                                agg_df_simple[output_metric_name] = ""
                        elif output_metric_type == "TIMESTAMP":
                            if not is_datetime64_ns_dtype(column_type):
                                new_type = 'datetime64[ns]'
                                agg_df_simple[output_metric_name] = pd.TIMESTAMP(0)
                        elif output_metric_type == "JSON":
                            if not is_object_dtype(column_type):
                                new_type = 'object'
                                agg_df_simple[output_metric_name] = ""
                        else:
                            self.logger.warning(f"The output type could not be enforced for metric {output_metric_name} "
                                                f"because it has an unexpected type {output_metric_type}.")

                        if new_type is not None:
                            self.logger.debug(f"Type was changed from {column_type} to {new_type} for "
                                              f"output metric {output_metric_name}")
                    else:
                        self.logger.warning(f"The output type could not be enforced for metric {output_metric_name} "
                                            f"because it was not found in the list of defined metrics.")

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

            df_direct = func(df=df, group_base=group_base)
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


class DirectAggregator(Aggregator):
    is_direct_aggregator = True


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
        # group is expected to be a pandas series but in case of an empty dataframe groupby.agg() hands over an empty
        # data frame. This is an erroneous behaviour in pandas but we have to take it into account: Group.count()
        # returns a series instead of integer when group is a data frame instead of series. When cnt is a series the
        # subsequent cnt >= self.min_count raises an ambiguous truth exception - at least for some panda versions
        # like 1.4.3 but not for 1.3.4 and 1.5.4
        if not group.empty:
            cnt = group.count()
        else:
            cnt = 0
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
        # group is expected to be a pandas series but in case of an empty dataframe groupby.agg() hands over an empty
        # data frame. This is an erroneous behaviour in pandas but we have to take it into account: Do not deploy
        # unique() on group when it is a dataframe because unique() does only exist for series.
        if not group.empty:
            cnt = len(group.dropna().unique())
        else:
            cnt = 0
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
