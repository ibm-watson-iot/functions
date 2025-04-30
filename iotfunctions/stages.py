# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import datetime as dt
import json
import logging
from collections import defaultdict
from enum import Enum, auto

import ibm_db
import numpy as np
import pandas as pd
from sqlalchemy import (MetaData, Table)

from . import dbhelper
from .dbhelper import check_sql_injection, check_sql_injection_extended
from .exceptions import StageException, DataWriterException
from .util import asList, UNIQUE_EXTENSION_LABEL, log_data_frame
from . import metadata as md

logger = logging.getLogger(__name__)

DATALAKE_BATCH_UPDATE_ROWS = 5000
KPI_ENTITY_ID_COLUMN = 'ENTITY_ID'


class MetricType(Enum):
    BOOLEAN = auto()
    STRING = auto()
    NUMBER = auto()
    TIMESTAMP = auto()
    JSON = auto()

METRICTYPE_TO_COLNAME = {MetricType.NUMBER: "VALUE_N",
                         MetricType.STRING: "VALUE_S",
                         MetricType.BOOLEAN: "VALUE_B",
                         MetricType.TIMESTAMP: "VALUE_T",
                         MetricType.JSON: "VALUE_C"}


class PersistColumns:

    def __init__(self, dms, sources=None, checkpoint=False):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if dms is None:
            raise RuntimeError("argument dms must be provided")
        if sources is None:
            raise RuntimeError("argument sources must be provided")
        self.dms = dms
        self.schema = self.dms.schema
        self.db_connection = self.dms.db_connection
        self.sources = asList(sources)
        self.checkpoint = checkpoint

    def execute(self, df):
        self.logger.debug('columns_to_persist=%s, df_columns=%s' % (str(self.sources), str(df.dtypes.to_dict())))
        if self.dms.production_mode:
            t1 = dt.datetime.now()
            self.store_derived_metrics_2(df[list(set(self.sources) & set(df.columns))])
            t2 = dt.datetime.now()
            self.logger.info("persist_data_time_seconds=%s" % (t2 - t1).total_seconds())
            if self.checkpoint is True:
                self.dms.create_checkpoint_entries(df)
                t3 = dt.datetime.now()
                self.logger.info("checkpoint_time_seconds=%s" % (t3 - t2).total_seconds())
        else:
            self.logger.info("***** The calculated metric data is not stored into the database. ***** ")

        return df

    def store_derived_metrics_2(self, dataFrame):

        if self.dms.production_mode:

            # Determine the metrics per table which must be persisted. Collect metric_table_name, metric_name and metric_type
            table_name_to_metrics = defaultdict(dict)
            for metric_name, dtype in dataFrame.dtypes.to_dict().items():
                metric_metadata = self.dms.data_items.get(metric_name)
                if metric_metadata is None:
                    continue

                # skip transient data items
                if metric_metadata.get(md.DATA_ITEM_TRANSIENT_KEY) is True:
                    self.logger.debug('skip persisting transient data_item=%s' % metric_name)
                    continue

                # Determine table name of current source
                try:
                    metric_table_name = metric_metadata.get(md.DATA_ITEM_SOURCETABLE_KEY)
                except Exception:
                    self.logger.warning('sourceTableName invalid for derived_metric=%s' % metric_name, exc_info=True)
                    continue

                if metric_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_NUMBER:
                    metric_type = MetricType.NUMBER
                elif metric_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_BOOLEAN:
                    metric_type = MetricType.BOOLEAN
                elif metric_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_TIMESTAMP:
                    metric_type = MetricType.TIMESTAMP
                elif metric_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_JSON:
                    metric_type = MetricType.JSON
                else:
                    metric_type = MetricType.STRING

                table_name_to_metrics[metric_table_name][metric_name] = metric_type

            self.logger.debug('table_name_to_metrics=%s' % str(table_name_to_metrics))

            # Create sql statements for each table
            table_name_to_has_clob = {}
            table_name_to_insert_stmt = {}
            for table_name, metric_name_to_metric_type in table_name_to_metrics.items():

                # Determine if this table has a column VALUE_C (CLOB containing json)
                has_clob_column = False
                for metric_name, metric_type in metric_name_to_metric_type.items():
                    if metric_type == MetricType.JSON:
                        has_clob_column = True
                        break
                table_name_to_has_clob[table_name] = has_clob_column

                # Determine grain for this table
                first_metric_name = None
                for metric_name in metric_name_to_metric_type.keys():
                    first_metric_name = metric_name
                    break
                grain = self.dms.target_grains[first_metric_name]

                # Create upsert statements for this table
                sql = self.create_upsert_statement(table_name, grain, has_clob_column)
                try:
                    stmt = ibm_db.prepare(self.db_connection, sql)
                except Exception:
                    raise RuntimeError('Error creating DB2 upsert statement for sql = %s' % sql, exc_info=True)
                table_name_to_insert_stmt[table_name] = (stmt, grain)

                self.logger.debug(f'SQL upsert statement derived_metrics_upsert_sql = {sql}')

            # Determine position of metric in dataframe. Position starts at 1.
            metric_name_to_position = {}
            for pos, metric_name in enumerate(dataFrame.columns, 1):
                metric_name_to_position[metric_name] = pos

            # Determine position of index levels in dataframe's index. Position starts at 0.
            index_name_to_position = {index_name: position for position, index_name in enumerate(dataFrame.index.names)}

            for table_name, metric_name_to_metric_type in table_name_to_metrics.items():
                stmt, grain = table_name_to_insert_stmt[table_name]
                table_has_clob = table_name_to_has_clob[table_name]

                existing_result_df = self.get_existing_result(table_name, grain, metric_name_to_metric_type, dataFrame)
                result_name_to_metric_name = {}
                metric_name_to_result_position = {}
                if existing_result_df is not None and not existing_result_df.empty:
                    for metric_name in dataFrame.columns:
                        if metric_name in existing_result_df.columns:
                            result_name_to_metric_name[metric_name + UNIQUE_EXTENSION_LABEL] = metric_name

                    dataFrame = dataFrame.join(other=existing_result_df, how='left', rsuffix=UNIQUE_EXTENSION_LABEL)

                    # Determine position of result columns in dataframe. Position starts at 1.
                    number_of_metrics = len(metric_name_to_position)
                    for pos, col_name in enumerate(dataFrame.columns, 1):
                        if pos <= number_of_metrics:
                            continue
                        else:
                            metric_name = result_name_to_metric_name.get(col_name)
                            if metric_name is not None:
                                metric_name_to_result_position[metric_name] = pos
                else:
                    logger.debug("No existing data was retrieved.")

                log_data_frame(f'Dataframe before loop to push data to DB2: ', dataFrame)
                logger.debug(f'Existing data available for the following metrics: '
                             f'{list(metric_name_to_result_position.keys())}')

                # Loop over rows of data frame
                # We do not use namedtuples in intertuples() (name=None) because of clashes of column names with python
                # keywords and column names starting with underscore; both lead to renaming of column names in df_rows.
                # Additionally, namedtuples are limited to 255 columns in itertuples(). We access the columns in df_row
                # by index. Position starts at 1 because 0 is reserved for the row index.
                value_list = []
                cnt = 0
                cnt_total = 0
                total_saved = 0
                precision_factor = 10 ** (9 - self.dms.db.precision_fractional_seconds)

                for df_row in dataFrame.itertuples(index=True, name=None):
                    ix = df_row[0]
                    for metric_name, metric_type in metric_name_to_metric_type.items():

                        metric_value = df_row[metric_name_to_position[metric_name]]

                        result_position = metric_name_to_result_position.get(metric_name)
                        if result_position is not None:
                            result_value = df_row[result_position]
                        else:
                            result_value = None

                        # Skip missing values and values which already exist in DB2
                        if pd.notna(metric_value):
                            cnt_total += 1

                            row_values = list()
                            row_values.append(metric_name)

                            if grain is None or len(grain) == 0:
                                # no grain, the index must be an array of (id, timestamp)
                                row_values.append(ix[0])
                                row_values.append(ix[1])
                            elif not isinstance(ix, list) and not isinstance(ix, tuple):
                                # only one element in the grain, ix is not an array, just append it anyway
                                row_values.append(ix)
                            else:
                                if grain[2]:
                                    # entity_first, the first level index must be the entity id
                                    row_values.append(ix[0])
                                if grain[0] is not None:
                                    if grain[2]:
                                        # if both id and time are included in the grain, time must be at pos 1
                                        row_values.append(ix[1])
                                    else:
                                        # if only time is included, time must be at pos 0
                                        row_values.append(ix[0])
                                if grain[1] is not None:
                                    for dimension in grain[1]:
                                        row_values.append(ix[index_name_to_position[dimension]])

                            # MetricType.BOOLEAN
                            if metric_type == MetricType.BOOLEAN:
                                if metric_value is False or metric_value == 0:
                                    metric_value = False
                                else:
                                    metric_value = True
                                if metric_value != result_value:
                                    row_values.append(0 if metric_value is False else 1)
                                else:
                                    # Calculated and existing value are identical. No need to update in DB2
                                    continue
                            else:
                                row_values.append(None)

                            # MetricType.NUMBER
                            if metric_type == MetricType.NUMBER:
                                float_value = float(metric_value)
                                if not np.isfinite(float_value):
                                    # Metric is infinite (np.inf). This can happen after a division by zero in the
                                    # calculation. We handle infinite values as None and we do not update any
                                    # existing result in DB2
                                    continue
                                if float_value != result_value:
                                    row_values.append(float_value)
                                else:
                                    # Calculated and existing value are identical. No need to update in DB2
                                    continue
                            else:
                                row_values.append(None)

                            # MetricType.STRING
                            if metric_type == MetricType.STRING:
                                metric_value = str(metric_value)
                                if metric_value != result_value:
                                    row_values.append(metric_value)
                                else:
                                    # Calculated and existing value are identical. No need to update in DB2
                                    continue
                            else:
                                row_values.append(None)

                            # MetricType.TIMESTAMP
                            if metric_type == MetricType.TIMESTAMP:
                                # Reduce precision of current metric value (it can be upto nanoseconds depending how
                                # the metric value is calculated) to the precision of result value from DB2 before
                                # comparing both. Otherwise the comparison metric_value != result_value returns True
                                # all the time.
                                nano_seconds = metric_value.microsecond * 1000 + metric_value.nanosecond
                                metric_value = metric_value - \
                                               pd.Timedelta(nano_seconds -
                                                            (nano_seconds // precision_factor * precision_factor))
                                if metric_value != result_value:
                                    row_values.append(metric_value)
                                else:
                                    # Calculated and existing value are identical. No need to update in DB2
                                    continue
                            else:
                                row_values.append(None)

                            # MetricType.JSON
                            if table_has_clob is True:
                                if metric_type == MetricType.JSON:
                                    json_str = json.dumps(metric_value)
                                    if json_str != result_value:
                                        row_values.append(json_str)
                                    else:
                                        # Calculated and existing value are identical. No need to update in DB2
                                        continue
                                else:
                                    row_values.append(None)

                            value_list.append(tuple(row_values))
                            cnt += 1

                        if cnt >= DATALAKE_BATCH_UPDATE_ROWS:
                            try:
                                # Bulk insert

                                res = ibm_db.execute_many(stmt, tuple(value_list))
                                saved = res if res is not None else ibm_db.num_rows(stmt)

                                total_saved += saved
                                self.logger.debug('Records saved so far = %d' % total_saved)
                            except Exception as ex:
                                raise Exception('Error persisting derived metrics, batch size = %s, value_list=%s' % (
                                    len(value_list), str(value_list))) from ex

                            value_list = []
                            cnt = 0

                if len(value_list) > 0:
                    try:
                        # Bulk insert
                        res = ibm_db.execute_many(stmt, tuple(value_list))
                        saved = res if res is not None else ibm_db.num_rows(stmt)

                        total_saved += saved
                    except Exception as ex:
                        raise Exception('Error persisting derived metrics, batch size = %s, value_list=%s' % (
                            len(value_list), str(value_list))) from ex

                self.logger.debug(f'derived_metrics_persisted: {total_saved} values out of {cnt_total} non-null values saved. ')

    def create_upsert_statement(self, tableName, grain, with_clob_column):
        dimensions = []
        if grain is None or len(grain) == 0:
            dimensions.append(KPI_ENTITY_ID_COLUMN)
            dimensions.append('TIMESTAMP')
        else:
            if grain[2]:
                dimensions.append(KPI_ENTITY_ID_COLUMN)
            if grain[0] is not None:
                dimensions.append('TIMESTAMP')
            if grain[1] is not None:
                # Map dimension names to dimension column names
                for dimension_name in grain[1]:
                    dimensions.append(self.dms.data_items.get(dimension_name)["columnName"])

        colExtension = ''
        parmExtension = ''
        joinExtension = ''
        sourceExtension = ''
        for dimension in dimensions:
            quoted_dimension = dbhelper.quotingColumnName(check_sql_injection(dimension))
            colExtension += ', ' + quoted_dimension
            parmExtension += ', ?'
            joinExtension += ' AND TARGET.' + quoted_dimension + ' = SOURCE.' + quoted_dimension
            sourceExtension += ', SOURCE.' + quoted_dimension

        return (f"MERGE INTO "
                f"{dbhelper.quotingSchemaName(check_sql_injection(self.schema))}.{dbhelper.quotingTableName(check_sql_injection(tableName))} "
                f"AS TARGET "
                f"USING (VALUES (?{parmExtension}, ?, ?, ?, ?, {'?, ' if with_clob_column else ''}CURRENT TIMESTAMP)) AS SOURCE "
                f"(KEY{colExtension}, VALUE_B, VALUE_N, VALUE_S, VALUE_T, {'VALUE_C, ' if with_clob_column else ''}LAST_UPDATE) "
                f"ON TARGET.KEY = SOURCE.KEY{joinExtension} "
                f"WHEN MATCHED THEN "
                f"UPDATE SET TARGET.VALUE_B = SOURCE.VALUE_B, TARGET.VALUE_N = SOURCE.VALUE_N, "
                f"TARGET.VALUE_S = SOURCE.VALUE_S, TARGET.VALUE_T = SOURCE.VALUE_T,"
                f"{ 'TARGET.VALUE_C = SOURCE.VALUE_C,' if with_clob_column else ''} TARGET.LAST_UPDATE = SOURCE.LAST_UPDATE "
                f"WHEN NOT MATCHED THEN "
                f"INSERT (KEY{colExtension}, VALUE_B, VALUE_N, VALUE_S, VALUE_T, {'VALUE_C, ' if with_clob_column else ''}LAST_UPDATE) "
                f"VALUES (SOURCE.KEY{sourceExtension}, SOURCE.VALUE_B, SOURCE.VALUE_N, SOURCE.VALUE_S, SOURCE.VALUE_T, "
                f"{'SOURCE.VALUE_C, ' if with_clob_column else ''}CURRENT TIMESTAMP)")

    def get_existing_result(self, table_name, grain, metric_name_to_metric_type, df):

        # Only retrieve any existing results when we have a timestamp-level in the index of the dataframe
        # and when the dataframe is not empty (both is required to determine the upper and lower boundary for the
        # timestamp in sql statement)
        if ((grain is None or len(grain) == 0) or grain[0] is not None) and df.empty is False:
            # Determine the required columns to build index of dataframe
            index_names = []
            index_column_names = []
            if grain is None or len(grain) == 0:
                index_column_names.append(KPI_ENTITY_ID_COLUMN)
                index_column_names.append('TIMESTAMP')
                index_names.append('id')
                index_names.append(self.dms.eventTimestampName)

            else:
                if grain[2]:
                    index_column_names.append(KPI_ENTITY_ID_COLUMN)
                    index_names.append('id')
                if grain[0] is not None:
                    index_column_names.append('TIMESTAMP')
                    index_names.append(self.dms.eventTimestampName)
                if grain[1] is not None:
                    # Map dimension names to dimension column names
                    for dimension_name in grain[1]:
                        index_column_names.append(self.dms.data_items.get(dimension_name)["columnName"])
                        index_names.append(dimension_name)

            quoted_index_column_names = []
            for column_name in index_column_names:
                quoted_index_column_names.append(dbhelper.quotingColumnName(check_sql_injection(column_name)))
            quoted_index_column_names_string = ", ".join(quoted_index_column_names)

            # Retrieve existing metric data per metric type. Limit the result by timestamp.
            tmp_time_index = df.index.get_level_values(self.dms.eventTimestampName)
            timestamp_min = tmp_time_index.min()
            timestamp_max = tmp_time_index.max()
            existing_result_dfs = []
            for value_column_type, value_column_name in METRICTYPE_TO_COLNAME.items():

                # Setup sql statement
                metric_names = []
                quoted_metric_names = []
                for metric_name, metric_type in metric_name_to_metric_type.items():
                    if metric_type == value_column_type:
                        metric_names.append(metric_name)
                        quoted_metric_names.append(dbhelper.quotingSqlString(check_sql_injection_extended(metric_name)))

                if len(metric_names) > 0:
                    sql_statement = f'SELECT  {quoted_index_column_names_string}, "KEY", "{value_column_name}" FROM ' \
                                    f'{dbhelper.quotingSchemaName(check_sql_injection(self.schema))}.{dbhelper.quotingTableName(check_sql_injection(table_name))} ' \
                                    f'WHERE KEY IN ({", ".join(quoted_metric_names)}) AND ' \
                                    f'TIMESTAMP >= \'{timestamp_min}\' AND TIMESTAMP <= \'{timestamp_max}\''
                    logger.debug(f"Sql statement to retrieve existing results of type {value_column_type.name}: {sql_statement}")

                    # Execute sql statement and get result as list of tuples
                    existing_result = []
                    try:
                        result_handle = ibm_db.exec_immediate(self.db_connection, sql_statement)

                        try:
                            row = ibm_db.fetch_tuple(result_handle)
                            while row is not False:
                                existing_result.append(row)
                                row = ibm_db.fetch_tuple(result_handle)
                        except Exception as ex:
                            raise RuntimeError(f"Collecting the result of sql statement to retrieve existing results "
                                               f"of type {value_column_type.name} failed: {ex}") from ex
                        finally:
                            ibm_db.free_result(result_handle)

                    except Exception as ex:
                        raise RuntimeError(f"Execution of sql statement to retrieve existing results of type "
                                           f"{value_column_type.name} failed: {ex}") from ex

                    # Convert existing_result to dataframe with metrics arranged as columns
                    if len(existing_result) > 0:
                        tmp_df = pd.DataFrame.from_records(data=existing_result,
                                                           columns=[*index_names, "KEY", value_column_name])
                        existing_result_dfs.append(tmp_df.pivot(index=index_names, columns="KEY",
                                                                values=value_column_name))
                        tmp_df = None
                        existing_result = None
                        logger.debug(f"Existing data for metrics {metric_names} of metric type {value_column_type.name} "
                                     f"retrieved.")
                    else:
                        logger.debug(f"No existing data available for metrics {metric_names} of "
                                     f"metric type {value_column_type.name}")

            # Join existing-result data frames to one data frame
            if len(existing_result_dfs) > 0:
                # Take first data frame
                existing_result_df = existing_result_dfs[0]
                if len(existing_result_dfs) > 1:
                    # Join remaining dataframes to first dataframe and sort index
                    existing_result_df = existing_result_df.join(other=existing_result_dfs[1:], how='outer', sort=True)
                else:
                    # Sort index
                    existing_result_df.sort_index(inplace=True)
            else:
                existing_result_df = None

        else:
            existing_result_df = None
            logger.debug("Existing result was not retrieved because data frame is empty or because no timestamp "
                         "column available.")

        return existing_result_df


def _timestamp_as_string(timestamp):
    return f"{timestamp.year:04}-{timestamp.month:02}-{timestamp.day:02} " \
           f"{timestamp.hour:02}:{timestamp.minute:02}:{timestamp.second:02}." \
           f"{timestamp.microsecond:06}{timestamp.nanosecond:03}"


class ProduceAlerts(object):
    is_system_function = True
    produces_output_items = False

    ALERT_TABLE_NAME = 'dm_wiot_as_alert'

    def __init__(self, dms, alerts=None, data_item_names=None, **kwargs):

        if dms is None:
            raise RuntimeError("argument dms must be provided")

        self.dms = dms

        try:
            self.entity_type_name = dms.logical_name
        except AttributeError:
            self.entity_type_name = dms.entity_type

        self.quoted_schema = dbhelper.quotingSchemaName(check_sql_injection(dms.default_db_schema))
        self.quoted_table_name = dbhelper.quotingTableName(check_sql_injection(self.ALERT_TABLE_NAME))
        self.alert_to_kpi_input_dict = dict()

        if alerts is not None:
            self.alerts_to_db = alerts
        elif data_item_names is not None:
            self.alerts_to_db = []
            alert_catalogs = dms.catalog.get_alerts()
            for data_item_name in asList(data_item_names):
                metadata = dms.data_items.get(data_item_name)
                kpi_func_dto = metadata.get(md.DATA_ITEM_KPI_FUNCTION_DTO_KEY, None)
                if kpi_func_dto is not None:
                    kpi_function_name = kpi_func_dto.get(md.DATA_ITEM_KPI_FUNCTION_DTO_FUNCTION_NAME, None)
                    alert_catalog = alert_catalogs.get(kpi_function_name, None)
                    if alert_catalog is not None:
                        self.alerts_to_db.append(data_item_name)
                        self.alert_to_kpi_input_dict[data_item_name] = kpi_func_dto.get('input')
        else:
            raise RuntimeError("Invalid combination of parameters: Either alerts or data_item_names must be provided.")

        logger.info('alerts going to database = %s ' % str(self.alerts_to_db))

        # Column names in database
        self.timestamp_col_name = 'timestamp'
        self.updated_ts_col_name = 'updated_ts'
        self.entity_id_col_name = 'entity_id'
        self.alert_id_col_name = 'alert_id'
        self.created_ts_col_name = 'created_ts'
        self.alert_col_name = 'data_item_name'

        # Column names in data frame
        self.timestamp_df_name = self.dms.eventTimestampName
        self.entity_id_df_name = 'id'
        self.alert_id_df_name = '###IBM###_alert_id'
        self.created_ts_df_name = '###IBM###_created_ts'
        self.alert_df_name = '###IBM###_alert_name'

    def __str__(self):

        return 'System generated ProduceAlerts stage'

    def execute(self, df, start_ts=None, end_ts=None):

        # Only do for an non-empty dataframe
        if self.dms.production_mode is True and df.shape[0] > 0:

            # Only do when alerts are defined
            if len(self.alerts_to_db) > 0:

                # Determine if index of dataframe comes with or without entity id.
                # Note: Indices containing dimensions are not supported and cause an exception
                index_has_entity_id = self._verify_index_shape(df)

                # Do for each alert separately
                new_alert_events = {}
                for alert_name in self.alerts_to_db:

                    # Extract rows from data frame which are alert events, i.e. column 'alert_name' is equal to True
                    calc_alert_events = df[(df[alert_name] == True)].copy()

                    # Corrective action for pandas' issue https://github.com/pandas-dev/pandas/issues/44786
                    # Convert MultiIndex of dataframe to numpy array. Then convert the numpy array back to a MultiIndex.
                    # The flag about duplicates in MultiIndex is correctly recalculated.
                    if calc_alert_events.index.size > 0 and calc_alert_events.index.nlevels > 1:
                        tmp_names = calc_alert_events.index.names
                        calc_alert_events.index = pd.MultiIndex.from_tuples(calc_alert_events.index.to_numpy())
                        calc_alert_events.index.names = tmp_names

                    # Dataframe calc_alert_events can contain duplicates with respect to its index
                    # (device id/ timestamp). Duplicates are a result of duplicated raw metrics or incorrect
                    # calculation in a kpi function. Duplicates can cause trouble later on when inserted into database
                    # because the upsert statement does not allow the modification of the same record twice in the
                    # same bulk statement. We take corrective action and take the first occurrence of a duplicate only.
                    # This does not make any difference for table DM_WIOT_AS_ALERT because no metrics are inserted
                    # into this table but it has a small impact for Message Hub because metrics are added
                    if calc_alert_events.index.has_duplicates:
                        number_events_before = calc_alert_events.shape[0]
                        calc_alert_events = calc_alert_events[(~calc_alert_events.index.duplicated(keep='first'))]
                        number_removed_events = number_events_before - calc_alert_events.shape[0]

                        logger.warning(f"Dataframe contains {number_removed_events} duplicates with respect to "
                                       f"device id/ timestamp for alert {alert_name}. Duplicates are removed.")

                    if calc_alert_events.index.size > 0:
                        # Get earliest and latest timestamp of all alert events
                        timestamp_level = calc_alert_events.index.get_level_values(self.dms.eventTimestampName)
                        earliest = timestamp_level.min()
                        latest = timestamp_level.max()

                        # Retrieve existing alert events from database (table DM_WIOT_AS_ALERT) as index object
                        existing_alert_events = self._get_alert_events_from_db(alert_name=alert_name,
                                                                               index_has_entity_id=index_has_entity_id,
                                                                               start_ts=earliest, end_ts=latest)

                        # Determine all alert events which have been calculated in this pipeline run but which do not
                        # exist in database yet
                        difference = calc_alert_events.index.difference(existing_alert_events.index)
                        new_alert_events[alert_name] = calc_alert_events.reindex(difference)

                        logger.info(f"{difference.size} out of {calc_alert_events.index.size} calculated alert events "
                                    f"for alert {alert_name} are new alert events.")
                    else:
                        new_alert_events[alert_name] = calc_alert_events
                        logger.info(f"There are no calculated alert events for alert {alert_name}")

                # Push new alert events to database
                self._push_alert_events_to_db(new_alert_events, index_has_entity_id)

            else:
                logger.info("No alerts have been defined for current grain.")

        else:
            logger.info("No alerts have to be processed because the dataframe is empty.")

        return df

    def _verify_index_shape(self, df):

        level_names = df.index.names
        if len(level_names) == 1 and level_names[0] == self.dms.eventTimestampName:
            index_has_entity_id = False
            logger.info("The grain is based on timestamp only. The entity ids were dropped for this grain.")
        elif len(level_names) == 2 and level_names[0] == 'id' and level_names[1] == self.dms.eventTimestampName:
            index_has_entity_id = True
            logger.info("The grain is based on entity ids and timestamps.")
        else:
            raise RuntimeError(f"The data frame refers to a grain with dimensions which is not supported for alerts. "
                               f"The index of the data frame contains the following levels: {str(level_names)}")

        return index_has_entity_id

    def _get_alert_events_from_db(self, alert_name, index_has_entity_id, start_ts=None, end_ts=None):

        # Important: Explicitly set lower-case alias for timestamp column. Otherwise the column name in data frame
        # will be in uppercase for DB2 because sqlalchemy attempts to avoid name clashes with the possibly reserved
        # keyword TIMESTAMP by quoting
        select_timestamp = f'{self.timestamp_col_name} as "{self.timestamp_col_name}"'

        if index_has_entity_id is True:
            select_entity_id = f', {self.entity_id_col_name} as "{self.entity_id_col_name}"'
            index_col_names = [self.entity_id_col_name, self.timestamp_col_name]
            requested_col_names = [self.timestamp_df_name, self.entity_id_df_name]
        else:
            select_entity_id = ''
            index_col_names = [self.timestamp_col_name]
            requested_col_names = [self.timestamp_df_name]

        sql_statement = f"SELECT {select_timestamp}{select_entity_id} " \
                        f"FROM {self.quoted_schema}.{self.quoted_table_name} " \
                        f"WHERE entity_type_id = {self.dms.entity_type_id} AND {self.alert_col_name} = '{alert_name}'"

        if start_ts is not None:
            sql_statement += f" AND {self.timestamp_col_name} >= '{str(start_ts)}'"
        if end_ts is not None:
            sql_statement += f" AND {self.timestamp_col_name} <= '{str(end_ts)}'"

        result_df = self.dms.db.read_sql_query(sql_statement, parse_dates=[self.timestamp_col_name],
                                               index_col=index_col_names,
                                               requested_col_names=requested_col_names,
                                               log_message=f"Sql statement for alert {alert_name}")

        logger.debug(f"{result_df.shape[0]} alert events have been read from database.")

        return result_df

    def _push_alert_events_to_db(self, alert_events, index_has_entity_id):

        total_count = 0
        rows = []
        start_time = dt.datetime.now()
        sql_statement = self._get_sql_statement()

        for alert_name, df_alert_events in alert_events.items():

            # Get attributes linked to this alert
            kpi_input = self.alert_to_kpi_input_dict.get(alert_name)
            severity = kpi_input.get('Severity')
            priority = kpi_input.get('Priority')
            domain_status = kpi_input.get('Status')

            # Loop over all alert events
            for index_values in df_alert_events.index:
                # Distinguish with/without entity id
                if index_has_entity_id is True:
                    tmp_entity_id = index_values[0]
                    tmp_timestamp = index_values[1]
                else:
                    tmp_entity_id = None
                    tmp_timestamp = index_values

                # Setup alert event for DB
                rows.append((self.dms.entity_type_id, alert_name, tmp_entity_id, tmp_timestamp, severity, priority,
                             domain_status, self.dms.device_name_to_uid.get(tmp_entity_id)))

                if len(rows) == DATALAKE_BATCH_UPDATE_ROWS:
                    # Push alert events in list 'rows' in chunks to alert table in database
                    total_count += self._push_rows_to_db(sql_statement, rows)
                    rows.clear()
                    logger.info(f"{total_count} alert events have been written to alert table so far.")

        # Push all remaining rows to database
        if len(rows) > 0:
            # Push all remaining alert events (= rows) to database
            total_count += self._push_rows_to_db(sql_statement, rows)

        logger.info(f"A total of {total_count} alert events have been written to alert table in "
                    f"{(dt.datetime.now() - start_time).total_seconds()} seconds.")

    def _get_sql_statement(self):

        available_columns = ['ENTITY_TYPE_ID', 'DATA_ITEM_NAME', 'ENTITY_ID', 'TIMESTAMP', 'SEVERITY', 'PRIORITY',
                             'DOMAIN_STATUS', 'DEVICE_UID']
        all_columns_list = ', '.join(available_columns)
        raw_statement = f"MERGE INTO {self.quoted_schema}.{self.quoted_table_name} AS TARGET USING " \
                        f"(VALUES ({', '.join(['?'] * len(available_columns))})) AS SOURCE ({all_columns_list}) " \
                        f"ON TARGET.ENTITY_TYPE_ID = SOURCE.ENTITY_TYPE_ID " \
                        f"AND TARGET.DATA_ITEM_NAME = SOURCE.DATA_ITEM_NAME " \
                        f"AND TARGET.ENTITY_ID = SOURCE.ENTITY_ID " \
                        f"AND TARGET.TIMESTAMP = SOURCE.TIMESTAMP " \
                        f"WHEN NOT MATCHED THEN " \
                        f"INSERT ({all_columns_list}) " \
                        f"VALUES (SOURCE.{', SOURCE.'.join(available_columns)})"

        try:
            statement = ibm_db.prepare(self.dms.db_connection, raw_statement)
        except Exception as ex:
            raise RuntimeError("Preparation of sql statement failed for DB2.") from ex

        return statement

    def _push_rows_to_db(self, sql_statement, rows):

        try:
            res = ibm_db.execute_many(sql_statement, tuple(rows))
            row_count = res if res is not None else ibm_db.num_rows(sql_statement)
        except Exception as ex:
            raise RuntimeError(f"The attempt to write {len(rows)} alert events to alert table in database "
                               f"failed.") from ex

        return row_count

    @staticmethod
    def _serialize_converter(obj):
        if isinstance(obj, dt.datetime):
            return obj.__str__()
        else:
            raise TypeError(f"Do not know how to convert object of class {obj.__class__.__name__} to JSON")


class DataWriter(object):
    ITEM_NAME_TIMESTAMP_MIN = 'TIMESTAMP_MIN'
    ITEM_NAME_TIMESTAMP_MAX = 'TIMESTAMP_MAX'


class DataWriterFile(DataWriter):
    """
    Default data write stage. Writes to the file system.
    """

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
        """
        Set parameters based using supplied dictionary
        """
        for key, value in list(params.items()):
            setattr(self, key, value)
        return self


class DataReader(object):
    """
    Default data reader stage. Calls get_data method on the object.
    """
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
    """
    Stage that writes the calculated data items to database.
    """
    is_system_function = True
    MAX_NUMBER_OF_ROWS_FOR_SQL = 5000
    produces_output_items = False

    # Fixed column names for the output tables
    COLUMN_NAME_KEY = 'key'
    COLUMN_NAME_VALUE_NUMERIC = 'value_n'
    COLUMN_NAME_VALUE_STRING = 'value_s'
    COLUMN_NAME_VALUE_BOOLEAN = 'value_b'
    COLUMN_NAME_VALUE_TIMESTAMP = 'value_t'
    COLUMN_NAME_VALUE_CLOB = 'value_c'
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
                    list(col_list), table_name, start_ts, end_ts))

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
        # Remember position of column in dataframe. Index starts at 1.
        col_position = {}
        for pos, col_name in enumerate(df.columns, 1):
            col_position[col_name] = pos
        # Loop over rows of data frame.
        # We do not use namedtuples in intertuples() (name=None) because of clashes of column names with python
        # keywords and column names starting with underscore; both lead to renaming of column names in df_rows.
        # Additionally, namedtuples are limited to 255 columns in itertuples(). We access the columns in df_row
        # by index. Position starts at 1 because 0 is reserved for the row index.
        for df_row in df.itertuples(index=True, name=None):
            # Get index that is always at position 0 in df_row
            ix = df_row[0]
            # Loop over data item in rows
            for item_name, (item_type, table_name) in col_props:
                derived_value = df_row[col_position[item_name]]
                if pd.isna(derived_value):
                    continue

                table_object, delete_object, insert_object, index_name_pos, map, row_list = table_props[table_name]

                # Collect data for new row in output table
                row = dict()
                row[map[self.COLUMN_NAME_KEY]] = item_name
                for index_name, position in index_name_pos:
                    row[index_name] = ix[position]

                if item_type == md.DATA_ITEM_TYPE_BOOLEAN:
                    row[map[self.COLUMN_NAME_VALUE_BOOLEAN]] = (1 if (bool(derived_value) is True) else 0)
                else:
                    row[map[self.COLUMN_NAME_VALUE_BOOLEAN]] = None

                if item_type == md.DATA_ITEM_TYPE_NUMBER:
                    my_float = float(derived_value)
                    row[map[self.COLUMN_NAME_VALUE_NUMERIC]] = (my_float if np.isfinite(my_float) else None)
                else:
                    row[map[self.COLUMN_NAME_VALUE_NUMERIC]] = None

                if item_type == md.DATA_ITEM_TYPE_LITERAL:
                    row[map[self.COLUMN_NAME_VALUE_STRING]] = str(derived_value)
                else:
                    row[map[self.COLUMN_NAME_VALUE_STRING]] = None

                if item_type == md.DATA_ITEM_TYPE_TIMESTAMP:
                    row[map[self.COLUMN_NAME_VALUE_TIMESTAMP]] = derived_value
                else:
                    row[map[self.COLUMN_NAME_VALUE_TIMESTAMP]] = None

                if helper_cols_avail:
                    row[map[self.COLUMN_NAME_TIMESTAMP_MIN]] = df_row[col_position[DataWriter.ITEM_NAME_TIMESTAMP_MIN]]
                    row[map[self.COLUMN_NAME_TIMESTAMP_MAX]] = df_row[col_position[DataWriter.ITEM_NAME_TIMESTAMP_MAX]]

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
        for col_name, col_type in df.dtypes.items():
            metadata = self.data_item_metadata.get(col_name)
            if metadata is not None and metadata.get(md.DATA_ITEM_TYPE_KEY).upper() == 'DERIVED_METRIC':
                if metadata.get(md.DATA_ITEM_TRANSIENT_KEY, False) is False:
                    table_name = metadata.get(md.DATA_ITEM_SOURCETABLE_KEY)
                    data_item_type = metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY)
                    kpi_func_dto = metadata.get(md.DATA_ITEM_KPI_FUNCTION_DTO_KEY)
                    if table_name is None:
                        logger.warning(
                            'No table name defined for data item ' + col_name + '. The data item will not been written to the database.')
                    elif data_item_type is None:
                        logger.warning(
                            'No data type defined for data item ' + col_name + '. The data item will not been written to the database.')
                    elif kpi_func_dto is None:
                        logger.warning('No function definition defined for data item ' + col_name + '.')
                    else:
                        if (data_item_type != md.DATA_ITEM_TYPE_BOOLEAN and
                                data_item_type != md.DATA_ITEM_TYPE_NUMBER and
                                data_item_type != md.DATA_ITEM_TYPE_LITERAL and
                                data_item_type != md.DATA_ITEM_TYPE_TIMESTAMP):
                            logger.warning(f'Data item {col_name} has the unknown type {data_item_type}. The data item '
                                           f'will be written as {md.DATA_ITEM_TYPE_LITERAL} into the database.')
                            data_item_type = md.DATA_ITEM_TYPE_LITERAL

                        col_props[col_name] = (data_item_type, table_name)
                        if first_loop_cycle:
                            grain_name = kpi_func_dto.get('granularityName')
                            first_loop_cycle = False
                        else:
                            if grain_name != kpi_func_dto.get('granularityName'):
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
        table_object = Table(table_name.lower(), self.db_metadata, autoload_with=self.db_connection)

        return table_object
