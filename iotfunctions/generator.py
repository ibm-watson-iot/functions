# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

"""
The Built In Functions module contains preinstalled functions
"""

import logging

import numpy as np

from .base import (BaseTransformer)
from .ui import (UISingle, UIFunctionOutSingle, UISingleItem)

# import warnings
# from sqlalchemy import String

logger = logging.getLogger(__name__)
PACKAGE_URL = 'git+https://github.com/ibm-watson-iot/functions.git@'
_IS_PREINSTALLED = True


#
#                       | first |           main            | last|
#
# input array     A + + | + + + A + + + + A + + + + A ..... A + + | +
#
#        previous run   |   next run                              |  future run
#
#                 offset|               where to start with the next anomaly
#
#                       | remainder       what's left of width to be filled
#  ToDo
#  - save filler used in previous run for correct flatline generation (done)
#  - save timestamp to support overlapping data/backtrack
#

class AnomalyGenerator(BaseTransformer):

    def __init__(self):
        self.count = None  # allow to set count != 0 for unit testing
        self.key = None
        self.factor = 1
        self.width = 0
        self.size = 1
        self.counts_by_entity_id = None
        super().__init__()

    def injectAnomaly(self, input_array, offset=None, remainder=None, flatline=None, entity_name='', filler=None,
                      anomaly_extreme=None):

        output_array = input_array.copy()
        logger.debug(
            'InjectAnomaly: <<<entity ' + entity_name + ', Size: ' + str(input_array.size) + ', Offset: ' + str(
                offset) + ', Remainder: ' + str(remainder) + ', Flatline: ' + str(flatline))

        # start with part before the first anomaly
        if not anomaly_extreme:
            if filler is None:
                output_array[0:min(input_array.size, remainder)] = flatline
            else:
                output_array[0:min(input_array.size, remainder)] = filler
        remainder -= min(input_array.size, remainder)

        # just move the offset a bit
        if input_array.size < offset:
            logger.info('Not enough new data points to generate more anomalies - ' + str(input_array.shape))
            offset -= input_array.size
            logger.debug(
                'InjectAnomaly: >>>entity ' + entity_name + ', Size: ' + str(input_array.size) + ', Offset: ' + str(
                    offset) + ', Remainder: ' + str(remainder) + ', Flatline: ' + str(flatline))
            return offset, remainder, flatline, output_array

        # now treat the longer part of the array first (starting from offset)
        a = input_array[offset:]

        idx = offset

        if a.size >= self.factor:
            lim_size = a.size - a.size % self.factor
            logger.debug('InjectAnomaly:  Main:   entity ' + entity_name + ', a-Size: ' + str(a.size) + ', Lim: ' + str(
                lim_size) + ', Factor: ' + str(self.factor) + ', Width: ' + str(self.width))
            a_reshape_arr = a[:lim_size].copy()

            # Final numpy array to be transformed into 2d array
            try:
                a1 = np.reshape(a_reshape_arr, (-1, self.factor)).T
            except Exception as e:
                logger.error('InjectAnomaly: reshape failed with ' + str(e) + ' ' + str(input_array.shape) + ',' + str(
                    lim_size) + ',' + str(offset))

            if anomaly_extreme:
                # Calculate 'local' standard deviation if it exceeds 1 to generate anomalies
                std = np.std(a1, axis=0)
                stdvec = np.maximum(np.where(np.isnan(std), 1, std), np.ones(a1[0].size))

                # Mark Extreme anomalies
                a1[0] = np.multiply(a1[0], np.multiply(np.random.choice([-1, 1], a1.shape[1]), stdvec * self.size))
            else:
                for i in range(1, self.width):
                    if filler is not None:
                        a1[i] = filler
                    else:
                        a1[i] = a1[0]

            # Flattening back to 1D array
            output_array[offset:lim_size + offset] = a1.T.flatten()
            idx = lim_size + offset

        # handle the rest of the array
        if idx < output_array.size:
            logger.info('InjectAnomaly at the end - at ' + str(idx))
            flatline = input_array[idx]

            if not anomaly_extreme:
                if filler is None:
                    filler = flatline

                # this is not correct - a correct implementation would have to keep track of the filler on disk unless it's NaN
                try:
                    output_array[idx:idx + min(output_array.size - idx, self.width)] = filler
                except Exception as e1:
                    logger.error('InjectAnomaly filling up fails with ' + str(e1) + ' for ' + str(filler))

                remainder = self.width - min(output_array.size - idx, self.width)
            else:
                output_array[idx] = max(abs(np.mean(input_array[idx:])), 1) * self.size  # todo randomness

        offset = input_array.size - idx

        logger.debug(
            'InjectAnomaly: >>>entity ' + entity_name + ', Size: ' + str(input_array.size) + ', Offset: ' + str(
                offset) + ', Remainder: ' + str(remainder) + ', Flatline: ' + str(flatline))

        return offset, remainder, flatline, output_array

    def execute(self, df):
        logger.debug('AnomalyGenerator class')
        return df

    def check_and_init_key(self, entity_type):

        derived_metric_table_name = 'DM_' + entity_type.logical_name
        schema = entity_type._db_schema

        # store and initialize the counts by entity id
        db = self._entity_type.db

        raw_dataframe = None

        try:
            query, table = db.query(derived_metric_table_name, schema, column_names='KEY',
                                    filters={'KEY': self.output_item})
            raw_dataframe = db.read_sql_query(query)
            self.key = '_'.join([derived_metric_table_name, self.output_item])
            logger.debug('Check for key {} in derived metric table {}'.format(self.output_item, raw_dataframe.shape))
        except Exception as e:
            logger.error('Checking for derived metric table %s failed with %s.' % (str(self.output_item), str(e)))
            self.key = str(derived_metric_table_name) + str(self.output_item)
            pass

        if raw_dataframe is not None and raw_dataframe.empty:
            # delete old counts if present
            db.model_store.delete_model(self.key)
            logger.debug('Reinitialize count')

        self.counts_by_entity_id = None
        try:
            self.counts_by_entity_id = db.model_store.retrieve_model(self.key)
        except Exception as e2:
            self.counts_by_entity_id = self.count
            logger.error('Counts by entity id not yet initialized - error: ' + str(e2))
            pass

    def save_key(self):
        db = self._entity_type.db
        try:
            db.model_store.store_model(self.key, self.counts_by_entity_id)
        except Exception as e3:
            logger.error('Counts by entity id cannot be stored - error: ' + str(e3))
            pass
        return

    def extractOffset(self, entity_grp_id):
        offset = 0
        remainder = 0
        flatline = 0

        if self.counts_by_entity_id is None:
            self.counts_by_entity_id = {}

        if entity_grp_id in self.counts_by_entity_id:
            try:
                offset = self.counts_by_entity_id[entity_grp_id][0]
                remainder = self.counts_by_entity_id[entity_grp_id][1]
                flatline = self.counts_by_entity_id[entity_grp_id][2]
            except Exception as e:
                logger.info('No proper offset and remainder ' + str(e))
                pass
        return offset, remainder, flatline


class AnomalyGeneratorExtremeValue(AnomalyGenerator):
    """
    This function generates extreme anomaly.
    """

    def __init__(self, input_item, factor, size, output_item):
        super().__init__()
        self.input_item = input_item
        self.output_item = output_item
        self.factor = int(factor)
        self.size = int(size)
        self.count = None  # allow to set count != 0 for unit testing

    def execute(self, df):

        logger.debug('Dataframe shape {}'.format(df.shape))

        # initialize per entity offset and remainder
        entity_type = self.get_entity_type()
        self.check_and_init_key(entity_type)

        timeseries = df.reset_index()
        timeseries[self.output_item] = timeseries[self.input_item]

        df_grpby = timeseries.groupby('id')
        for grp in df_grpby.__iter__():

            entity_grp_id = grp[0]
            df_entity_grp = grp[1]

            # Initialize group counts, counts contain an offset and a remainder
            #  to determine where to start and how (and whether) to fill the offset
            offset, remainder, flatline = self.extractOffset(entity_grp_id)

            logger.debug('Initial Grp Counts {}'.format(self.counts_by_entity_id))

            # Prepare numpy array for marking anomalies
            actual = df_entity_grp[self.output_item].values
            offset, remainder, flatline, output_array = self.injectAnomaly(actual, offset=offset, remainder=remainder,
                                                                           flatline=flatline, entity_name=entity_grp_id,
                                                                           anomaly_extreme=True)

            # Update group counts for storage
            self.counts_by_entity_id[entity_grp_id] = (offset, remainder, flatline)
            logger.debug('Final Grp Counts {}'.format(self.counts_by_entity_id))

            # Adding the missing elements to create final array
            # final = np.append(actual[:strt_idx], a2)
            # Set values in the original dataframe
            try:
                timeseries.loc[df_entity_grp.index, self.output_item] = output_array
            except Exception as ee:
                logger.error('Could not set anomaly because of ' + str(ee) + '\nSizes are ' + str(output_array.shape))
                pass

        logger.debug('Final Grp Counts {}'.format(self.counts_by_entity_id))

        # save the group counts to db
        self.save_key()

        timeseries.set_index(df.index.names, inplace=True)
        return timeseries

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Item to base anomaly on'))

        inputs.append(UISingle(name='factor', datatype=int,
                               description='Frequency of anomaly e.g. A value of 3 will create anomaly every 3 datapoints',
                               default=5))

        inputs.append(UISingle(name='size', datatype=int, description='Size of extreme anomalies to be created. e.g. 10 will create 10x size extreme \
                             anomaly compared to the normal variance', default=10))

        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float,
                                           description='Generated Item With Extreme anomalies'))
        return (inputs, outputs)


class AnomalyGeneratorNoData(AnomalyGenerator):
    """
    This function generates nodata anomaly.
    """

    def __init__(self, input_item, width, factor, output_item):
        super().__init__()
        self.input_item = input_item
        self.output_item = output_item
        self.width = int(width)
        self.factor = int(factor) + int(width)
        self.count = None  # allow to set count != 0 for unit testing

    def execute(self, df):

        logger.debug('Dataframe shape {}'.format(df.shape))

        entity_type = self.get_entity_type()
        self.check_and_init_key(entity_type)

        # mark Anomalies
        timeseries = df.reset_index()
        timeseries[self.output_item] = timeseries[self.input_item]
        df_grpby = timeseries.groupby('id')
        for grp in df_grpby.__iter__():

            entity_grp_id = grp[0]
            df_entity_grp = grp[1]

            # Initialize group counts, counts contain an offset and a remainder
            #  to determine where to start and how (and whether) to fill the offset
            offset, remainder, flatline = self.extractOffset(entity_grp_id)

            logger.debug('Group {} Indexes {}'.format(grp[0], df_entity_grp.index))

            # Prepare numpy array for marking anomalies
            actual = df_entity_grp[self.output_item].values
            offset, remainder, flatline, output_array = self.injectAnomaly(actual, offset=offset, remainder=remainder,
                                                                           flatline=flatline, entity_name=entity_grp_id,
                                                                           filler=np.nan, anomaly_extreme=False)

            self.counts_by_entity_id[entity_grp_id] = (offset, remainder, flatline)

            # Adding the missing elements to create final array
            # final = np.append(actual[:strt_idx], a2)
            # Set values in the original dataframe
            try:
                timeseries.loc[df_entity_grp.index, self.output_item] = output_array
            except Exception as ee:
                logger.error('Could not set anomaly because of ' + str(ee) + '\nSizes are ' + str(output_array.shape))
                pass

        logger.debug('Final Grp Counts {}'.format(self.counts_by_entity_id))

        # save the group counts to db
        self.save_key()

        timeseries.set_index(df.index.names, inplace=True)
        return timeseries

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Item to base anomaly on'))

        inputs.append(UISingle(name='factor', datatype=int,
                               description='Frequency of anomaly e.g. A value of 3 will create anomaly every 3 datapoints',
                               default=10))

        inputs.append(UISingle(name='width', datatype=int, description='Width of the anomaly created', default=5))

        outputs = []
        outputs.append(
            UIFunctionOutSingle(name='output_item', datatype=float, description='Generated Item With NoData anomalies'))
        return (inputs, outputs)


class AnomalyGeneratorFlatline(AnomalyGenerator):
    """
    This function generates flatline anomaly.
    """

    def __init__(self, input_item, width, factor, output_item):
        super().__init__()
        self.input_item = input_item
        self.output_item = output_item
        self.width = int(width)
        self.factor = int(factor) + int(width)
        self.count = None  # allow to set count != 0 for unit testing

    def execute(self, df):

        logger.debug('Dataframe shape {}'.format(df.shape))

        entity_type = self.get_entity_type()
        self.check_and_init_key(entity_type)
        logger.debug('Initial Grp Counts {}'.format(self.counts_by_entity_id))

        # mark Anomalies
        timeseries = df.reset_index()
        timeseries[self.output_item] = timeseries[self.input_item]
        df_grpby = timeseries.groupby('id')
        for grp in df_grpby.__iter__():

            entity_grp_id = grp[0]
            df_entity_grp = grp[1]

            # Initialize group counts, counts contain an offset and a remainder
            #  to determine where to start and how (and whether) to fill the offset
            offset, remainder, flatline = self.extractOffset(entity_grp_id)

            logger.debug('Initial Grp Counts {}'.format(self.counts_by_entity_id))

            # Prepare numpy array for marking anomalies
            actual = df_entity_grp[self.output_item].values
            remainder, offset, flatline, output_array = self.injectAnomaly(actual, offset=offset, remainder=remainder,
                                                                           flatline=flatline, entity_name=entity_grp_id,
                                                                           filler=None, anomaly_extreme=False)

            # Update group counts for storage
            self.counts_by_entity_id[entity_grp_id] = (offset, remainder, flatline)

            # Adding the missing elements to create final array
            # final = np.append(actual[:strt_idx], a2)
            # Set values in the original dataframe
            try:
                timeseries.loc[df_entity_grp.index, self.output_item] = output_array
            except Exception as ee:
                logger.error('Could not set anomaly because of ' + str(ee) + '\nSizes are ' + str(output_array.shape))
                pass

        logger.debug('Final Grp Counts {}'.format(self.counts_by_entity_id))

        # save the group counts to db
        self.save_key()

        timeseries.set_index(df.index.names, inplace=True)
        return timeseries

    @classmethod
    def build_ui(cls):
        inputs = []
        inputs.append(UISingleItem(name='input_item', datatype=float, description='Item to base anomaly on'))

        inputs.append(UISingle(name='factor', datatype=int,
                               description='Frequency of anomaly e.g. A value of 3 will create anomaly every 3 datapoints',
                               default=10))

        inputs.append(UISingle(name='width', datatype=int, description='Width of the anomaly created', default=5))

        outputs = []
        outputs.append(UIFunctionOutSingle(name='output_item', datatype=float,
                                           description='Generated Item With Flatline anomalies'))
        return (inputs, outputs)
