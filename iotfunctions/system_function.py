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
import pandas as pd

from .ui import UIMultiItem, UISingle
from .exceptions import MergeException
from .util import get_index_names, reset_df_index
from collections import OrderedDict

logger = logging.getLogger(__name__)


class AggregateItems(object):
    '''
    Use common aggregation methods to aggregate one or more data items

    '''

    is_system_function = True
    _allow_empty_df = False
    produces_output_items = True
    is_simple_aggregator = True
    granularity = None
    _input_set = None

    def __init__(self, input_items, aggregation_function, output_items=None):

        super().__init__()

        self.input_items = input_items
        self.aggregation_function = aggregation_function

        if output_items is None:
            output_items = ['%s_%s' % (x, aggregation_function) for x in self.input_items]

        self.output_items = output_items
        self._output_list = []
        self._output_list.extend(self.output_items)

    def __str__(self):

        out = self.__class__.__name__
        try:
            out = out + ' at granularity ' + str(self.granularity)
        except AttributeError:
            out = out + ' unknown granularity'

        if self.input_items is not None:
            out = out + ' requires inputs %s' % self.input_items
        else:
            out = out + ' required inputs not evaluated yet'

        if self._output_list is not None:
            out = out + ' produces outputs %s' % self._output_list
        else:
            out = out + ' outputs produced not evaluated yet'

        try:
            out = out + ' on schedule ' + str(self.schedule)
        except AttributeError:
            out = out + ' unknown schedule'

        return out

    def get_aggregation_method(self):

        # Aggregation methods may either be strings like 'sum' or 'count' or class methods
        # get_available_methods returns a dictionary that converts aggregation method names to class names when needed

        methods = self.get_available_methods()
        out = methods.get(self.aggregation_function, None)
        if out is None:
            raise ValueError('Invalid aggregation function specified: %s' % self.aggregation_function)

        return out

    def get_input_set(self):

        out = set(self.input_items)
        gran = self.granularity
        if gran is not None:
            out |= set(gran.dimensions)
        else:
            raise ValueError('Aggregate function %s has no granularity' % self.aggregation_function)

        return out

    @classmethod
    def build_ui(cls):

        inputs = []
        inputs.append(UIMultiItem(name='input_items', datatype=None,
                                  description='Choose the data items that you would like to aggregate',
                                  output_item='output_items', is_output_datatype_derived=True))

        aggregate_names = list(cls.get_available_methods().keys())

        inputs.append(
            UISingle(name='aggregation_function', description='Choose aggregation function', values=aggregate_names))

        return (inputs, [])

    @classmethod
    def count_distinct(cls, series):

        return len(series.dropna().unique())

    @classmethod
    def get_available_methods(cls):

        return {'sum': 'sum', 'count': 'count', 'count_distinct': cls.count_distinct, 'min': 'min', 'max': 'max',
                'mean': 'mean', 'median': 'median', 'std': 'std', 'var': 'var', 'first': 'first', 'last': 'last',
                'product': 'product'}


class DataAggregator(object):
    '''
    Default simple aggregation stage.

    Parameters:
    -----------

    granularity: Granularity object

    agg_dict: dict
        Pandas aggregation dictionary

    complex_aggregators: list
        List of AS complex aggregation functions
        AS aggregation functions have an execute method that can be called
        inside of a pandas apply() on a groupby() to create a dataframe or series

    '''

    is_system_function = True
    _allow_empty_df = False
    _discard_prior_on_merge = True
    produces_output_items = True
    is_data_aggregator = True

    def __init__(self, name, granularity, agg_dict, input_items, output_items, complex_aggregators=None):

        self.name = name
        self._agg_dict = agg_dict
        self.granularity = granularity

        if complex_aggregators is None:
            complex_aggregators = []

        self._complex_aggregators = complex_aggregators
        self.input_items = input_items
        self.output_items = output_items

        self._input_set = set(self.input_items)
        self._output_list = self.output_items

    def __str__(self):

        msg = 'Aggregator: %s with granularity: %s. ' % (self.name, self.granularity.name)
        for key, value in list(self._agg_dict.items()):
            msg = msg + ' Aggregates %s using %s .' % (key, value)
        for s in self._complex_aggregators:
            msg = msg + ' Uses %s to produce %s .' % (s.name, s._output_list)

        return msg

    def execute(self, df=None):

        gfs = []
        group = df.groupby(self.granularity.grouper)

        if not self._agg_dict is None and self._agg_dict:
            gf = group.agg(self._agg_dict)
            gfs.append(gf)
        for s in self._complex_aggregators:
            gf = group.apply(s.execute)
            gfs.append(gf)
        df = pd.concat(gfs, axis=1)

        df.columns = self.output_items

        logger.info('Completed aggregation: %s', self.granularity.name)
        return df


class DataMerge(object):
    '''
    A DataMerge object combines the results of execution of a stage
    with the results of execution of the previous stages.

    By default, a DataMerge object initializes itself with an empty
    dataframe. Although the main purpose of the DataMerge object is
    maintaining a dataframe, it can also keep track of any constants and
    dimension lookups required added during job processing so that it can
    re-apply constants if needed.

    Use the execute method to combine a new incoming data object with
    whatever data is present in the DataMerge at the time.

    '''

    is_system_function = True
    r_suffix = '_new_'

    def __init__(self, name=None, df=None, **kwargs):

        if name is None:
            name = self.__class__.__name__
        self.name = name
        if df is None:
            df = pd.DataFrame()
        self.df = df
        self.constants = kwargs.get('constants', None)
        self.df_dimension = kwargs.get('df_dimension', None)
        if self.constants is None:
            self.constants = {}

    def __str__(self):

        out = ('DataMerge object has data structures: dataframe with %s rows'
               ' and %s constants ' % (len(self.df.index), len(self.constants)))

        return out

    def add_constant(self, name, value):
        '''
        Register a constant provide a value.
        Apply the constant to the dataframe.
        '''

        self.constants[name] = value
        self.df[name] = value

    def apply_constants(self):
        '''
        Apply the values of all constants to the dataframe.
        '''

        for name, value in list(self.constants.items()):
            self.df[name] = value

    def clear_data(self):
        '''
        Clear dataframe and constants
        '''

        self.constants = {}
        self.df = pd.DataFrame()

    def coalesce_cols(self, df, suffix):
        '''
        Combine two variants of the same column into a single. Variants are
        distinguished using a suffix, e.g. 'x' and 'x_new_' will be combined
        if the suffix of '_new_' is used. The coalesced result will be
        placed in column 'x' and will contain 'x_new' where a value of 'x_new'
        was provided and 'x' where the value of 'x_new' was null.
        '''

        altered = []
        for i, o in enumerate(df.columns):
            try:
                drop = "%s%s" % (o, suffix)
                df[o] = df[o].fillna(df[drop])
                altered.append(drop)
            except KeyError:
                pass
        if len(altered) > 0:
            cols = [x for x in list(df.columns) if x not in altered]
            df = df[cols]

        return df

    def convert_to_df(self, obj, col_names, index):

        df = pd.DataFrame(data=obj, columns=col_names)
        df.index = index
        return df

    def get_index_names(self, df=None):

        '''
        Get a list of index names from a dataframe with a single index
        or multi-index.
        '''

        if df is None:
            df = self.df

        if df is None:
            df = pd.DataFrame()

        df_index_names = get_index_names(df)

        return df_index_names

    def get_cols(self, df=None):

        '''
        Get a full set of column names from df and index. Return set.
        '''

        if df is None:
            df = self.df

        cols = set(self.get_index_names(df))
        cols |= set(df.columns)

        return cols

    def execute(self, obj, col_names, force_overwrite=False, col_map=None):
        '''
        Perform a smart merge between a dataframe and another object. The other
        object may be a dataframe, series, numpy array, list or scalar.

        Auto merge will choose between one of 3 merge strategies:
        1) slice when indexes are indentical
        2) full outer join when indexes are structured the same way
        3) lookup when obj has a single part index that matches one of the df cols

        col_names is a list of string data item names. This list of columns
        will be added or replaced during merge. When obj is a constant,
        tabular array, or series these column names these column names are
        necessary to identify the contents. If the obj is a dataframe, if
        col_names are provided they will be used to rename the obj columns
        before merging.

        '''
        if obj is None:
            raise MergeException(('DataMerge is attempting to merge a null object with a dataframe. Object to be '
                                  ' merged must be a dataframe, series, constant or numpy array. Unable to merge None'))

        if self.df is None:
            self.df = pd.DataFrame()

        if len(self.df.index) > 0:
            logger.debug('Input dataframe has columns %s and index %s', list(self.df.columns), self.get_index_names())
            existing = 'df'
        else:
            existing = 'empty df'

        job_constants = list(self.constants.keys())

        if len(job_constants) > 0:
            logger.debug(('The job has constant output items %s'), [x for x in job_constants])

        if isinstance(obj, (dict, OrderedDict)):
            raise MergeException(('Function error. A failure occured when attempting to merge a dictionary with a '
                                  'dataframe. Convert the dictionary to a dataframe or series and provide appropriate'
                                  ' index names.'))

        # if the object is a 2d array, convert to dataframe
        if len(col_names) > 1 and obj is not None and not isinstance(obj, (pd.DataFrame, pd.Series)):
            try:
                obj = self.convert_to_df(obj, col_names=col_names, index=self.df.index)
            except Exception:
                raise

        if isinstance(obj, (pd.DataFrame, pd.Series)):
            self.merge_dataframe(df=obj, col_names=col_names, force_overwrite=force_overwrite, col_map=col_map)

        else:
            logger.debug(('Merging dataframe with object of type %s'), type(obj))
            self.merge_non_dataframe(obj, col_names=col_names)

        # test that df has expected columns
        df_cols = self.get_cols()
        if not self.df.empty and not set(col_names).issubset(df_cols):
            missing_cols = set(col_names) - df_cols
            raise MergeException(('Error in auto merge. Missing columns %s Post merge dataframe does not'
                                  ' contain the expected output columns %s that should have'
                                  ' been delivered through merge. It has columns %s' % (
                                      missing_cols, col_names, df_cols)))
        if len(self.df.index) > 0:
            id_index = self.df.index.get_level_values(0)
            ts_index = self.df.index.get_level_values(1)
            usage = len(self.df.index) * len(col_names)
        else:
            usage = 0

        merge_result = 'existing %s with new %s' % (existing, obj.__class__.__name__)

        return merge_result, usage

    def merge_dataframe(self, df, col_names, force_overwrite=True, col_map=None):

        if col_map is None:
            col_map = {}

        # convert series to dataframe
        # rename columns as appropriate using supplied col_names
        if isinstance(df, pd.Series):
            if col_names is not None:
                df.name = col_names[0]
            else:
                col_names = [df.name]
            df = df.to_frame()
        else:
            if (col_names is None):
                col_names = list(df.columns)
            if col_map:
                df = df.rename(col_map)
        if len(df.index) > 0:
            logger.debug('Merging dataframe with columns %s and index %s', list(self.df.columns),
                         self.get_index_names())

        # profile incoming df to understand its structure
        # and determine merge strategy
        obj_index_names = self.get_index_names(df)
        merge_strategy = None
        if len(df.index) == 0:
            merge_strategy = 'skip'
            logger.debug('Skipping empty dataframe received as merge input')
        elif len(self.df.index) == 0:
            merge_strategy = 'replace'
        elif df.index.equals(self.df.index):
            if set(col_names).issubset(set(self.df.columns)) and (not force_overwrite):
                merge_strategy = 'skip'
                logger.debug('Skipping df merge as it looks like the merge has already taken place.'
                             ' To bypass this check and merge set force_overwrite = True')
            else:
                merge_strategy = 'slice'
            logger.debug('Merging dataframe with the same index')
        elif obj_index_names == self.get_index_names():
            logger.debug('Merging dataframe with the same index names')
            merge_strategy = 'outer'
        elif len(obj_index_names) == 1:
            logger.debug('Merging a dataframe with single index key')
            merge_strategy = 'lookup'
            # validate index for lookup
            df_valid_names = set(self.get_index_names())
            df_valid_names.update(set(self.df.columns))
            if not set(obj_index_names).issubset(df_valid_names):
                raise ValueError(('Function error. Attempting to merge a dataframe that has an'
                                  ' invalid name in the index %s' % (set(obj_index_names) - df_valid_names)))

        # carry out merge operation based on chosen merge strategy
        if merge_strategy == 'skip':
            # Add a null column for anything that should have been delivered
            missing_cols = [x for x in col_names if x not in self.df.columns]
            for c in missing_cols:
                self.df[c] = None
        elif merge_strategy == 'replace':
            self.df = df
            self.apply_constants()
        elif merge_strategy == 'slice':
            for c in list(df.columns):
                self.df[c] = df[c]
        elif merge_strategy == 'outer':
            self.df = self.df.merge(df, 'outer', left_index=True, right_index=True, suffixes=('', self.r_suffix))
            self.df = self.coalesce_cols(self.df, suffix=self.r_suffix)
            # A full outer join can add rows to the dataframe
            # Apply the constants to fill in the values of these new rows
            self.apply_constants()
        elif merge_strategy == 'lookup':
            try:
                df_index_names = self.get_index_names()
                self.df = reset_df_index(self.df, auto_index_name=self.auto_index_name)
                self.df = self.df.merge(df, 'left', on=df.index.name, suffixes=('', self.r_suffix))
            except Exception:
                logger.error(('Function error when attempting to auto merge a dataframe. The merge object is not'
                              ' a slice; or another dataframe with a compatible index; or a lookup with a'
                              ' single index that matches on the the source columns. Modify the index of the'
                              ' merge object to get it to automerge or explicitly merge'
                              ' inside the function and return a merged  result.'))
                raise
            else:
                self.df = self.df.set_index(df_index_names)
                self.df = self.coalesce_cols(self.df, suffix=self.r_suffix)
        else:
            logger.debug('Function error. Could not auto merge')
            if len(obj_index_names) == 0:
                df_valid_names = set(self.get_index_names())
                df_valid_names.update(set(self.df.columns))
                raise ValueError(('Function error.'
                                  'Attempting to merge a dataframe that has'
                                  ' an un-named index. Set the index name.'
                                  ' Index name/s may include any of the following'
                                  ' columns: %s' % (df_valid_names)))
            raise ValueError(('Function error.'
                              ' Auto merge encountered a dataframe that could not'
                              ' be automatically merged.'
                              ' The most likely reason for this is an invalid index'
                              ' When returning a dataframe from a function, the index'
                              ' names should either match the index of the dataframe that'
                              ' was provided as input, or should be a single lookup key.'
                              ' When using a lookup key, it must exist in the columns'
                              ' or index of the input dataframe'
                              ' Output dataframe index is %s.'
                              ' Input dataframe index is %s.'
                              ' Input dataframe columns are %s.' % (
                                  obj_index_names, self.get_index_names(), list(self.df.columns))))

    def merge_non_dataframe(self, obj, col_names):
        '''
        Merge a non-dataframe object into the DataMerge dataframe object.
        '''
        if len(col_names) == 1:
            # if the source dataframe is empty, it has no index
            # the data merge object can only accept a constant
            if len(self.df.index) == 0:
                self.add_constant(col_names[0], obj)
            else:
                try:
                    self.df[col_names[0]] = obj
                except ValueError:
                    raise ValueError(('Auto merge encounterd an object %s that could'
                                      ' not be automatically merged. Auto merge works best'
                                      ' when supplied with time series data indexed the'
                                      ' same way as subject of the merge or not time '
                                      ' series data with a single part index that is '
                                      ' readily identifyable as a source column' % obj))
        else:
            raise ValueError(('Auto merge encountered an object %s that could not'
                              ' be automatically merged. When the object is not a'
                              ' dataframe or numpy array, it should only deliver a'
                              ' single column. This merge operation has columns '
                              ' %s' % (obj, col_names)))


class DropNull(object):
    '''
    System function that drops null data
    '''

    is_system_function = True
    produces_output_items = False
    requires_input_items = False
    _allow_empty_df = False
    name = 'drop_null'

    def __init__(self, exclude_cols=None):

        if exclude_cols is None:
            exclude_cols = []
        self.exclude_cols = exclude_cols

    def __str__(self):

        return 'System generated DropNull stage'

    def execute(self, df):

        if len(df.index) > 0:

            msg = 'columns excluded when dropping null rows %s' % self.exclude_cols
            logger.debug(msg)
            subset = [x for x in df.columns if x not in self.exclude_cols]
            msg = 'columns considered when dropping null rows %s' % subset
            logger.debug(msg)
            for col in subset:
                count = df[col].count()
                msg = '%s count not null: %s' % (col, count)
                logger.debug(msg)
            df = df.dropna(how='all', subset=subset)

        return df
