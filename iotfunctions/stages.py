# *****************************************************************************
# Â© Copyright IBM Corp. 2018.  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************


from .util import MessageHub, asList
import logging

import json
import datetime as dt
import numpy as np

from .exceptions import StageException, DataWriterException

import pandas as pd
from sqlalchemy import (MetaData, Table)

# Kohlmann verify location of the following constants
DATA_ITEM_TYPE_BOOLEAN = 'BOOLEAN'
DATA_ITEM_TYPE_NUMBER = 'NUMBER'
DATA_ITEM_TYPE_LITERAL = 'LITERAL'
DATA_ITEM_TYPE_TIMESTAMP = 'TIMESTAMP'

DATA_ITEM_COLUMN_TYPE_KEY = 'columnType'
DATA_ITEM_TRANSIENT_KEY = 'transient'
DATA_ITEM_SOURCETABLE_KEY = 'sourceTableName'
DATA_ITEM_KPI_FUNCTION_DTO_KEY = 'kpiFunctionDto'

METADATA_TYPE_KEY = 'type'

DATA_ITEM_TAG_ALERT = 'ALERT'
DATA_ITEM_TAGS_KEY = 'tags'

logger = logging.getLogger(__name__)


class ProduceAlerts:

    def __init__(self, dms, alerts=None):

        if dms is None:
            raise RuntimeError("argument dms must be provided")
        if alerts is None:
            raise RuntimeError("argument alerts must be provided")
        self.dms = dms
        self.alerts = []
        self.messagehub = MessageHub()

        for alert in asList(alerts):
            # self.alerts.append(alert)
            metadata = self.dms.data_items.get(alert)
            if metadata is not None:
                if DATA_ITEM_TAG_ALERT in metadata.get(DATA_ITEM_TAGS_KEY, []):
                    self.alerts.append(alert)

    def execute(self, df):
        if self.dms.production_mode:
            logger.debug('alerts_to_produce=%s, df_columns=%s' % (str(self.alerts), str(df.dtypes.to_dict())))

            # TODO it may be worth pre-filtering the data frame to be just those rows with True alert column values,
            # because iterating through the whole data frame is a slow process

            t1 = dt.datetime.now()

            msg_and_keys = []
            if len(self.alerts) > 0:  # no alert, do iterate which is slow
                for ix, row in df.iterrows():
                    payload = row.to_dict()

                    # TODO there is an issue for the json.dumps below which cannot deal all types

                    for alert, value in payload.items():
                        # Skip missing values and non-True values
                        if alert in self.alerts and pd.notna(value) and value:
                            # publish alert format
                            # key: <tenant-id>|<entity-type>|<entity-id>|<alert-name>|<timestamp>
                            # value: json document containing all metrics at the same time / same device / same grain
                            msg_and_keys.append((json.dumps(payload, default=self._serialize_converter),
                                                 '%s|%s|%s|%s|%s' % (
                                                     self.dms.tenant_id, self.dms.entity_type, ix[0], alert, ix[1])))

            logger.debug("total_alerts_produced=%d" % len(msg_and_keys))

            if len(msg_and_keys) > 0:
                self.messagehub.produce_batch(msg_and_keys=msg_and_keys)

            t2 = dt.datetime.now()
            logger.info("produce_alert_time_seconds=%s" % (t2 - t1).total_seconds())

        return df

    '''
    Timestamp is not serialized by default by json.dumps()
    It is necessary to convert the object to string
    '''

    def _serialize_converter(self, o):
        if isinstance(o, dt.datetime):
            return o.__str__()


class DataWriter(object):
    ITEM_NAME_TIMESTAMP_MIN = 'TIMESTAMP_MIN'
    ITEM_NAME_TIMESTAMP_MAX = 'TIMESTAMP_MAX'


class DataWriterFile(DataWriter):
    '''
    Default data write stage. Writes to the file system.
    '''

    is_system_function = True
    requires_input_items = False
    produces_output_items = False

    def __init__(self, name, **params):
        self.name = name
        self.set_params(**params)

    def __str__(self):
        return 'System generated FileDataWriter stage: %s' % self.name

    def execute(self, df=None, start_ts=None, end_ts=None, entities=None):
        filename = 'data_writer_%s.csv' % self.name
        df.to_csv(filename)
        logger.debug('Wrote data to filename %s', filename)
        return df

    def set_params(self, **params):
        '''
        Set parameters based using supplied dictionary
        '''
        for key, value in list(params.items()):
            setattr(self, key, value)
        return self


class DataReader(object):
    '''
    Default data reader stage. Calls get_data method on the object.
    '''
    is_system_function = True
    is_data_source = True
    requires_input_items = False
    produces_output_items = True
    # will be added by job controller
    _projection_list = None

    def __init__(self, name, obj):

        self.name = name
        self.obj = obj

        self._input_set = set()
        self._output_list = self.get_output_list()

    def __str__(self):

        try:
            obj_name = self.obj.name
        except AttributeError:
            obj_name = self.obj.__class__.__name__

        out = ('System generated DataReader stage: %s. Reads data from'
               ' objects: %s' % (self.name, obj_name))

        return out

    def execute(self, df=None, start_ts=None, end_ts=None, entities=None):

        return self.obj.get_data(start_ts=start_ts, end_ts=end_ts, entities=entities, columns=self._projection_list)

    def get_output_list(self):

        if not self._projection_list is None:
            outputs = self._projection_list
            logger.debug(('The get_data() method of the payload will return'
                          ' data items %s using a projection list set by'
                          ' the job controller'), outputs)

        else:
            try:
                outputs = self.obj.get_output_items()
            except AttributeError:
                raise StageException(('The payload is missing a mandatory method'
                                      ' get_data_items_list() returns a list of'
                                      ' data items delivered by the get_data method'
                                      ' of the payload. If the get_data method of the'
                                      ' payload was not supposed to be called, set the'
                                      ' payloads _auto_read_from_ts_table property to False'), self)
            else:
                logger.debug(('The payload has candidate'
                              ' data items %s. The DataReader has no projection list'), outputs)

        if len(outputs) == 0:
            raise StageException(('The data reader get_data_items_list() method returned no'
                                  ' data items'), self)

        return outputs


class DataWriterSqlAlchemy(DataWriter):
    '''
    Stage that writes the calculated data items to database.
    '''
    is_system_function = True
    MAX_NUMBER_OF_ROWS_FOR_SQL = 5000
    produces_output_items = False

    # Fixed column names for the output tables
    COLUMN_NAME_KEY = 'key'
    COLUMN_NAME_VALUE_NUMERIC = 'value_n'
    COLUMN_NAME_VALUE_STRING = 'value_s'
    COLUMN_NAME_VALUE_BOOLEAN = 'value_b'
    COLUMN_NAME_VALUE_TIMESTAMP = 'value_t'
    COLUMN_NAME_TIMESTAMP = 'timestamp'
    COLUMN_NAME_TIMESTAMP_MIN = 'timestamp_min'
    COLUMN_NAME_TIMESTAMP_MAX = 'timestamp_max'
    COLUMN_NAME_ENTITY_ID = 'entity_id'

    def __init__(self, name, data_item_metadata, db_connection, schema_name, grains_metadata, **kwargs):
        self.name = name
        self.data_item_metadata = data_item_metadata
        self.db_connection = db_connection
        self.db_metadata = MetaData()
        self.schema_name = schema_name
        self.grains_metadata = grains_metadata
        self.kwargs = kwargs

    def __str__(self):

        return 'System generated DataWriterSqlAlchemy stage: %s' % self.name

    def execute(self, df=None, start_ts=None, end_ts=None, entities=None):

        if df is not None:
            logger.debug('Data items will be written to database for interval (%s, %s)' % (str(start_ts), str(end_ts)))

            col_props, helper_cols_avail, grain = self._get_active_cols_properties(df)
            logger.info('The following data items will be written to the database: %s' % (', '.join(
                [('%s (%s, %s)' % (item_name, table_name, data_type)) for item_name, (data_type, table_name) in
                 col_props.items()])))

            table_props = self._get_table_properties(df, col_props, grain, helper_cols_avail)
            logger.info('The data items will be written into the following tables: %s' % (
                ', '.join([table_name for table_name, dummy in table_props.items()])))

            # Delete old data item values in database
            self._delete_old_data(start_ts, end_ts, table_props, col_props, helper_cols_avail)

            if len(col_props) > 0:
                # Insert new data into database
                self._persist_data(df, col_props, helper_cols_avail, table_props)
            else:
                logger.warning('There are no data items that have to be written to the database.')

        else:
            raise DataWriterException('The data frame is None.')

        return df

    def _delete_old_data(self, start_ts, end_ts, table_props, col_props, helper_cols_avail):

        for table_name, (
                table_object, delete_object, insert_object, index_name_pos, map, row_list) in table_props.items():

            # Delete old data items in database
            try:
                col_list = col_props.keys()
                logger.debug('Deleting old data items %s from table %s for time range [%s, %s]' % (
                    col_list, table_name, start_ts, end_ts))

                start_time = dt.datetime.utcnow()

                # Restrict delete on interval [start_date, end_date]
                if helper_cols_avail:
                    timestamp_column_min = table_object.c.get(map[self.COLUMN_NAME_TIMESTAMP_MIN])
                    timestamp_column_max = table_object.c.get(map[self.COLUMN_NAME_TIMESTAMP_MAX])
                else:
                    timestamp_column_min = table_object.c.get(map[self.COLUMN_NAME_TIMESTAMP])
                    timestamp_column_max = timestamp_column_min

                if start_ts is not None:
                    delete_object = delete_object.where(timestamp_column_min >= start_ts)
                if end_ts is not None:
                    delete_object = delete_object.where(timestamp_column_max <= end_ts)

                # Restrict delete on KPIs that will be inserted in the next step
                key_column = table_object.c.get(map[self.COLUMN_NAME_KEY])
                delete_object = delete_object.where(key_column.in_(col_list))

                logger.debug('Executing delete statement: %s' % delete_object)
                result_object = self.db_connection.execute(delete_object)

                if result_object.supports_sane_rowcount():
                    txt = str(result_object.rowcount)
                else:
                    txt = 'Old'
                logger.info('%s data items have been deleted from table %s. Elapsed time in sec: %.3f' % (
                    txt, table_name, (dt.datetime.utcnow() - start_time) / dt.timedelta(microseconds=1000) / 1000))

            except Exception as exc:
                raise DataWriterException(
                    'Execution of the delete statement for table %s failed: %s' % (table_name, str(exc))) from exc

    def _persist_data(self, df, col_props, helper_cols_avail, table_props):

        col_props = col_props.items()

        counter = 0
        sql_alchemy_timedelta = dt.timedelta()
        start_time = dt.datetime.utcnow()
        # Loop over rows of data frame, loop over data item in rows
        for df_row in df.itertuples():
            ix = getattr(df_row, 'Index')
            for item_name, (item_type, table_name) in col_props:
                derived_value = getattr(df_row, item_name)
                if pd.isna(derived_value):
                    continue

                table_object, delete_object, insert_object, index_name_pos, map, row_list = table_props[table_name]

                # Collect data for new row in output table
                row = dict()
                row[map[self.COLUMN_NAME_KEY]] = item_name
                for index_name, position in index_name_pos:
                    row[index_name] = ix[position]

                if item_type == DATA_ITEM_TYPE_BOOLEAN:
                    row[map[self.COLUMN_NAME_VALUE_BOOLEAN]] = (1 if (bool(derived_value) is True) else 0)
                else:
                    row[map[self.COLUMN_NAME_VALUE_BOOLEAN]] = None

                if item_type == DATA_ITEM_TYPE_NUMBER:
                    my_float = float(derived_value)
                    row[map[self.COLUMN_NAME_VALUE_NUMERIC]] = (my_float if np.isfinite(my_float) else None)
                else:
                    row[map[self.COLUMN_NAME_VALUE_NUMERIC]] = None

                if item_type == DATA_ITEM_TYPE_LITERAL:
                    row[map[self.COLUMN_NAME_VALUE_STRING]] = str(derived_value)
                else:
                    row[map[self.COLUMN_NAME_VALUE_STRING]] = None

                if item_type == DATA_ITEM_TYPE_TIMESTAMP:
                    row[map[self.COLUMN_NAME_VALUE_TIMESTAMP]] = derived_value
                else:
                    row[map[self.COLUMN_NAME_VALUE_TIMESTAMP]] = None

                if helper_cols_avail:
                    row[map[self.COLUMN_NAME_TIMESTAMP_MIN]] = getattr(df_row, DataWriter.ITEM_NAME_TIMESTAMP_MIN)
                    row[map[self.COLUMN_NAME_TIMESTAMP_MAX]] = getattr(df_row, DataWriter.ITEM_NAME_TIMESTAMP_MAX)

                # Add new row to the corresponding row list
                row_list.append(row)

                # Write data to database when we have reached the max number per bulk
                if len(row_list) >= self.MAX_NUMBER_OF_ROWS_FOR_SQL:
                    sql_alchemy_timedelta += self._persist_row_list(table_name, insert_object, row_list)
                    counter += len(row_list)
                    logger.info('Number of data item values persisted so far: %d (%s)' % (counter, table_name))
                    row_list.clear()

        # Write remaining data (final bulk for each table)) to database
        for table_name, (
                table_object, delete_object, insert_object, index_name_pos, map, row_list) in table_props.items():
            if len(row_list) > 0:
                sql_alchemy_timedelta += self._persist_row_list(table_name, insert_object, row_list)
                counter += len(row_list)
                logger.info('Number of data item values persisted so far: %d (%s)' % (counter, table_name))
                row_list.clear()

        logger.info('Total number of persisted data item values: %d, Elapsed time in sec: %.3f, '
                    'SqlAlchemy time in sec: %.3f' % (
                        counter, (dt.datetime.utcnow() - start_time) / dt.timedelta(microseconds=1000) / 1000,
                        sql_alchemy_timedelta / dt.timedelta(microseconds=1000) / 1000))

    def _persist_row_list(self, table_name, insert_object, row_list):
        try:
            start_time = dt.datetime.utcnow()
            self.db_connection.execute(insert_object, row_list)
        except Exception as exc:
            raise DataWriterException(
                'Persisting data item values to table %s failed: %s' % (table_name, str(exc))) from exc
        return (dt.datetime.utcnow() - start_time)

    def _get_active_cols_properties(self, df):

        # Return a dict with all columns(=data items) that are relevant for data persistence.
        # Values of dict col_props hold the corresponding data type and table name.
        # Values of dict helper_col_props hold the corresponding data type only because those columns apply to all tables.
        #
        # Sort out all columns of data frame that
        # 1) do not correspond to a data item or
        # 2) do correspond to a transient data item
        # 3) have an inconsistent definition (table name or type of the corresponding data item is missing)

        col_props = dict()
        grain_name = None
        first_loop_cycle = True
        for col_name, col_type in df.dtypes.iteritems():
            metadata = self.data_item_metadata.get(col_name)
            if metadata is not None and metadata.get(METADATA_TYPE_KEY).upper() == 'DERIVED_METRIC':
                if metadata.get(DATA_ITEM_TRANSIENT_KEY, False) is False:
                    table_name = metadata.get(DATA_ITEM_SOURCETABLE_KEY)
                    data_item_type = metadata.get(DATA_ITEM_COLUMN_TYPE_KEY)
                    kpi_func_dto = metadata.get(DATA_ITEM_KPI_FUNCTION_DTO_KEY)
                    if table_name is None:
                        logger.warning(
                            'No table name defined for data item ' + col_name + '. The data item will not been written to the database.')
                    elif data_item_type is None:
                        logger.warning(
                            'No data type defined for data item ' + col_name + '. The data item will not been written to the database.')
                    elif kpi_func_dto is None:
                        logger.warning('No function definition defined for data item ' + col_name + '.')
                    else:
                        if (
                                data_item_type != DATA_ITEM_TYPE_BOOLEAN and data_item_type != DATA_ITEM_TYPE_NUMBER and data_item_type != DATA_ITEM_TYPE_LITERAL and data_item_type != DATA_ITEM_TYPE_TIMESTAMP):
                            logger.warning((
                                               'Data item %s has the unknown type %s. The data item will be written as %s into the database.') % (
                                               col_name, data_item_type, DATA_ITEM_TYPE_LITERAL))
                            data_item_type = DATA_ITEM_TYPE_LITERAL

                        col_props[col_name] = (data_item_type, table_name)
                        if first_loop_cycle:
                            grain_name = kpi_func_dto.get('granularity')
                            first_loop_cycle = False
                        else:
                            if grain_name != kpi_func_dto.get('granularity'):
                                raise Exception('Mismatch of grains. Only data items of same grain type can be '
                                                'handled together')

                else:
                    logger.info(
                        'Data item ' + col_name + ' is not written to database because it is marked as transient.')
            else:
                logger.info('The column ' + col_name + ' in data frame does not correspond to a data item. '
                                                       'Therefore it is not written to the database.')

        # Get definition of grain for corresponding grain name if defined
        grain = None
        if grain_name is not None:
            grain = self.grains_metadata.get(grain_name)

        # Add helper columns for aggregated data without (explicit) TimeSeries information
        helper_cols_avail = False
        if not self._time_series_avail(grain):
            # TimeSeries of index was not involved in aggregation, i.e. the time information is hidden in one or
            # more index columns we do not know now; in this case, we have two
            # additional helper columns in data frame to track the lower ond upper bound of the timestamps
            # of the aggregated records. Add those columns to the output table.
            helper_cols_avail = True

        return col_props, helper_cols_avail, grain

    def _get_table_properties(self, df, col_props, grain, helper_cols_avail):

        # Set up a map for the relation index name and index position
        map_index_name_pos = {name: pos for pos, name in enumerate(df.index.names)}
        logger.debug('Mapping between index name and index position: %s' % (
            ', '.join([('%s -> %d' % (name, pos)) for name, pos in map_index_name_pos.items()])))

        # Assemble the sql statements and the required index elements for each table referenced in col_props
        table_props = dict()
        for item_name, (data_type, table_name) in col_props.items():
            table_prop = table_props.get(table_name)
            if table_prop is None:
                table_object = self.get_table_object(table_name)
                delete_object = self.get_delete_object(table_object)
                insert_object = self.get_insert_object(table_object)
                logger.debug('For table %s: delete statement: %s, insert statement: %s' % (
                    table_name, delete_object, insert_object))

                # Setup mapping for column names that are quoted in sql statements by SqlAlchemy because they
                # are keywords in SQL
                output_col_names = []
                output_col_names.append(self.COLUMN_NAME_KEY)
                output_col_names.append(self.COLUMN_NAME_VALUE_NUMERIC)
                output_col_names.append(self.COLUMN_NAME_VALUE_STRING)
                output_col_names.append(self.COLUMN_NAME_VALUE_BOOLEAN)
                output_col_names.append(self.COLUMN_NAME_VALUE_TIMESTAMP)
                if helper_cols_avail:
                    output_col_names.append(self.COLUMN_NAME_TIMESTAMP_MIN)
                    output_col_names.append(self.COLUMN_NAME_TIMESTAMP_MAX)
                else:
                    output_col_names.append(self.COLUMN_NAME_TIMESTAMP)
                if grain is None or grain.entity_id is not None:
                    output_col_names.append(self.COLUMN_NAME_ENTITY_ID)
                if grain is not None and grain.dimensions is not None:
                    output_col_names.extend(grain.dimensions)

                map = self.get_col_name_map_for_sa(required_col_names=output_col_names, table_object=table_object)

                # Determine mapping between index fields (entity_id/timestamp) and column names
                index_name_pos = list()
                if not isinstance(df.index, pd.MultiIndex):
                    # only one element in the grain, index is not an array, just append it assuming 'timestamp'
                    index_name_pos.append((map[self.COLUMN_NAME_TIMESTAMP], 0))
                elif grain is None:
                    # no grain, the index must be an array of (id, timestamp)
                    index_name_pos.append((map[self.COLUMN_NAME_ENTITY_ID], 0))
                    index_name_pos.append((map[self.COLUMN_NAME_TIMESTAMP], 1))
                else:
                    if grain.entity_id is not None:
                        # entity_first, the first level index must be the entity id
                        index_name_pos.append((map[self.COLUMN_NAME_ENTITY_ID], 0))

                    if grain.freq is not None:
                        if grain.entity_id is not None:
                            # if both id and time are included in the grain, time must be at pos 1
                            index_name_pos.append((map[self.COLUMN_NAME_TIMESTAMP], 1))
                        else:
                            # if only time is included, time must be at pos 0
                            index_name_pos.append((map[self.COLUMN_NAME_TIMESTAMP], 0))

                    if grain.dimensions is not None:
                        for pos, dimension in enumerate(grain.dimensions, start=len(index_name_pos)):
                            index_name_pos.append((map[dimension], pos))

                logger.debug('For table %s: Mapping between column name and dataframe index position: %s' % (
                    table_object.name, ', '.join([col_name + ' ==> ' + str(pos) for col_name, pos in index_name_pos])))

                table_props[table_name] = (table_object, delete_object, insert_object, index_name_pos, map, list())

        return table_props

    def _time_series_avail(self, grain):
        time_series_avail = True
        if (grain is not None) and (grain.freq is None):
            time_series_avail = False
        return time_series_avail

    def get_col_name_map_for_sa(self, required_col_names, table_object):

        avail_col_names = set()
        for col in table_object.c:
            avail_col_names.add(col.name)
        logger.debug('Columns of table %s: %s' % (table_object.name, ', '.join(avail_col_names)))

        result_map = {}
        mapped_col = None
        for required_col in required_col_names:
            if required_col in avail_col_names:
                mapped_col = required_col
            else:
                required_col_upper = required_col.upper()
                if required_col_upper in avail_col_names:
                    mapped_col = required_col_upper
                else:
                    required_col_lower = required_col.lower()
                    if required_col_lower in avail_col_names:
                        mapped_col = required_col_lower
                    else:
                        raise ValueError('Column %s/%s/%s could not be found in table %s ' % (
                            required_col, required_col_upper, required_col_lower, table_object.name))
            result_map[required_col] = mapped_col

        logger.debug('Column name mapping for table %s: %s' % (
            table_object.name, ', '.join([col + ' ==> ' + mapped_col for col, mapped_col in result_map.items()])))
        return result_map

    def get_insert_object(self, table_object):
        insert_object = table_object.insert()
        return insert_object

    def get_delete_object(self, table_object):
        delete_object = table_object.delete()
        return delete_object

    def get_table_object(self, table_name):
        # kohlmann full_table_name ?????
        full_table_name = '%s.%s' % (self.schema_name, table_name)
        table_object = Table(table_name.lower(), self.db_metadata, autoload=True, autoload_with=self.db_connection)

        return table_object
