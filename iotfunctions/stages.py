# *****************************************************************************
# © Copyright IBM Corp. 2018.  All Rights Reserved.
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

import ibm_db
import numpy as np
import pandas as pd
from sqlalchemy import (MetaData, Table)

from . import dbhelper
from .exceptions import StageException, DataWriterException
from .util import MessageHub, asList
from . import metadata as md

try:
    from MAS_Data_Dictionary.MAS_Core_Types import EntryType
except ImportError:
    EntryType = None

logger = logging.getLogger(__name__)

DATALAKE_BATCH_UPDATE_ROWS = 5000
KPI_ENTITY_ID_COLUMN = 'ENTITY_ID'


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
        self.is_postgre_sql = dms.is_postgre_sql
        self.sources = asList(sources)
        self.checkpoint = checkpoint

    def execute(self, df):
        self.logger.debug('columns_to_persist=%s, df_columns=%s' % (str(self.sources), str(df.dtypes.to_dict())))
        if self.dms.production_mode:
            t1 = dt.datetime.now()
            self.store_derived_metrics(df[list(set(self.sources) & set(df.columns))])
            t2 = dt.datetime.now()
            self.logger.info("persist_data_time_seconds=%s" % (t2 - t1).total_seconds())
            if self.checkpoint is True:
                self.dms.create_checkpoint_entries(df)
                t3 = dt.datetime.now()
                self.logger.info("checkpoint_time_seconds=%s" % (t3 - t2).total_seconds())
        else:
            self.logger.info("***** The calculated metric data is not stored into the database. ***** ")

        return df

    def store_derived_metrics(self, dataFrame):
        if self.dms.production_mode:
            table_insert_stmt = {}
            table_metrics_to_persist = defaultdict(dict)

            for source, dtype in dataFrame.dtypes.to_dict().items():
                source_metadata = self.dms.data_items.get(source)
                if source_metadata is None:
                    continue

                # skip transient data items
                if source_metadata.get(md.DATA_ITEM_TRANSIENT_KEY) is True:
                    self.logger.debug('skip persisting transient data_item=%s' % source)
                    continue

                # if source not in (get_all_kpi_targets(self.get_pipeline()) & self.data_items.get_derived_metrics()):
                #     continue

                try:
                    tableName = source_metadata.get(md.DATA_ITEM_SOURCETABLE_KEY)
                except Exception:
                    self.logger.warning('sourceTableName invalid for derived_metric=%s' % source, exc_info=True)
                    continue

                if tableName not in table_insert_stmt:
                    grain = self.dms.target_grains[source]
                    sql = None
                    try:

                        if self.is_postgre_sql:
                            sql = self.create_upsert_statement_postgres_sql(tableName, grain)
                            table_insert_stmt[tableName] = (sql, grain)

                        else:
                            sql = self.create_upsert_statement(tableName, grain)
                            stmt = ibm_db.prepare(self.db_connection, sql)
                            table_insert_stmt[tableName] = (stmt, grain)

                        self.logger.debug('derived_metrics_upsert_sql = %s' % sql)
                    except Exception:
                        self.logger.warning('Error creating db upsert statement for sql = %s' % sql, exc_info=True)
                        continue

                value_bool = False
                value_number = False
                value_string = False
                value_timestamp = False

                dtype = dtype.name.lower()
                # if dtype.startswith('int') or dtype.startswith('float') or dtype.startswith('long') or dtype.startswith('complex'):
                if source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_NUMBER:
                    value_number = True
                # elif dtype.startswith('bool'):
                elif source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_BOOLEAN:
                    value_bool = True
                # elif dtype.startswith('datetime'):
                elif source_metadata.get(md.DATA_ITEM_COLUMN_TYPE_KEY) == md.DATA_ITEM_TYPE_TIMESTAMP:
                    value_timestamp = True
                else:
                    value_string = True

                table_metrics_to_persist[tableName][source] = [value_bool, value_number, value_string, value_timestamp]

            self.logger.debug('table_metrics_to_persist=%s' % str(table_metrics_to_persist))

            # Remember position of column in dataframe. Index starts at 1.
            col_position = {}
            for pos, col_name in enumerate(dataFrame.columns, 1):
                col_position[col_name] = pos

            index_name_pos = {name: idx for idx, name in enumerate(dataFrame.index.names)}
            for table, metric_and_type in table_metrics_to_persist.items():
                stmt, grain = table_insert_stmt[table]

                # Loop over rows of data frame
                # We do not use namedtuples in intertuples() (name=None) because of clashes of column names with python
                # keywords and column names starting with underscore; both lead to renaming of column names in df_rows.
                # Additionally, namedtuples are limited to 255 columns in itertuples(). We access the columns in df_row
                # by index. Position starts at 1 because 0 is reserved for the row index.
                valueList = []
                cnt = 0
                total_saved = 0

                for df_row in dataFrame.itertuples(index=True, name=None):
                    ix = df_row[0]
                    for metric, metric_type in metric_and_type.items():
                        derivedMetricVal = df_row[col_position[metric]]

                        # Skip missing values
                        if pd.notna(derivedMetricVal):
                            rowVals = list()
                            rowVals.append(metric)

                            if grain is None or len(grain) == 0:
                                # no grain, the index must be an array of (id, timestamp)
                                rowVals.append(ix[0])
                                rowVals.append(ix[1])
                            elif not isinstance(ix, list) and not isinstance(ix, tuple):
                                # only one element in the grain, ix is not an array, just append it anyway
                                rowVals.append(ix)
                            else:
                                if grain[2]:
                                    # entity_first, the first level index must be the entity id
                                    rowVals.append(ix[0])
                                if grain[0] is not None:
                                    if grain[2]:
                                        # if both id and time are included in the grain, time must be at pos 1
                                        rowVals.append(ix[1])
                                    else:
                                        # if only time is included, time must be at pos 0
                                        rowVals.append(ix[0])
                                if grain[1] is not None:
                                    for dimension in grain[1]:
                                        rowVals.append(ix[index_name_pos[dimension]])

                            if metric_type[0]:
                                if self.dms.is_postgre_sql:
                                    rowVals.append(
                                        False if (derivedMetricVal == False or derivedMetricVal == 0) else True)
                                else:
                                    rowVals.append(0 if (derivedMetricVal == False or derivedMetricVal == 0) else 1)
                            else:
                                rowVals.append(None)

                            if metric_type[1]:
                                myFloat = float(derivedMetricVal)
                                rowVals.append(myFloat if np.isfinite(myFloat) else None)
                            else:
                                rowVals.append(None)

                            rowVals.append(str(derivedMetricVal) if metric_type[2] else None)
                            rowVals.append(derivedMetricVal if metric_type[3] else None)

                            if metric_type[1] and float(derivedMetricVal) is np.nan or metric_type[2] and str(
                                    derivedMetricVal) == 'nan':
                                self.logger.debug('!!! weird case, derivedMetricVal=%s' % derivedMetricVal)
                                continue

                            valueList.append(tuple(rowVals))
                            cnt += 1

                        if cnt >= DATALAKE_BATCH_UPDATE_ROWS:
                            try:
                                # Bulk insert

                                if self.is_postgre_sql:
                                    dbhelper.execute_batch(self.db_connection, stmt, valueList,
                                                           DATALAKE_BATCH_UPDATE_ROWS)
                                    saved = cnt  # Work around because we don't receive row count from batch query.

                                else:
                                    res = ibm_db.execute_many(stmt, tuple(valueList))
                                    saved = res if res is not None else ibm_db.num_rows(stmt)

                                total_saved += saved
                                self.logger.debug('Records saved so far = %d' % total_saved)
                            except Exception as ex:
                                raise Exception('Error persisting derived metrics, batch size = %s, valueList=%s' % (
                                    len(valueList), str(valueList))) from ex

                            valueList = []
                            cnt = 0

                if len(valueList) > 0:
                    try:
                        # Bulk insert
                        if self.is_postgre_sql:
                            dbhelper.execute_batch(self.db_connection, stmt, valueList, DATALAKE_BATCH_UPDATE_ROWS)
                            saved = cnt  # Work around because we don't receive row count from batch query.
                        else:
                            res = ibm_db.execute_many(stmt, tuple(valueList))
                            saved = res if res is not None else ibm_db.num_rows(stmt)

                        total_saved += saved
                    except Exception as ex:
                        raise Exception('Error persisting derived metrics, batch size = %s, valueList=%s' % (
                            len(valueList), str(valueList))) from ex

                self.logger.debug('derived_metrics_persisted = %s' % str(total_saved))

    def create_upsert_statement(self, tableName, grain):
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
                dimensions.extend(grain[1])

        colExtension = ''
        parmExtension = ''
        joinExtension = ''
        sourceExtension = ''
        for dimension in dimensions:
            quoted_dimension = dbhelper.quotingColumnName(dimension)
            colExtension += ', ' + quoted_dimension
            parmExtension += ', ?'
            joinExtension += ' AND TARGET.' + quoted_dimension + ' = SOURCE.' + quoted_dimension
            sourceExtension += ', SOURCE.' + quoted_dimension

        return ("MERGE INTO %s.%s AS TARGET "
                "USING (VALUES (?%s, ?, ?, ?, ?, CURRENT TIMESTAMP)) AS SOURCE (KEY%s, VALUE_B, VALUE_N, VALUE_S, VALUE_T, LAST_UPDATE) "
                "ON TARGET.KEY = SOURCE.KEY%s "
                "WHEN MATCHED THEN "
                "UPDATE SET TARGET.VALUE_B = SOURCE.VALUE_B, TARGET.VALUE_N = SOURCE.VALUE_N, TARGET.VALUE_S = SOURCE.VALUE_S, TARGET.VALUE_T = SOURCE.VALUE_T, TARGET.LAST_UPDATE = SOURCE.LAST_UPDATE "
                "WHEN NOT MATCHED THEN "
                "INSERT (KEY%s, VALUE_B, VALUE_N, VALUE_S, VALUE_T, LAST_UPDATE) VALUES (SOURCE.KEY%s, SOURCE.VALUE_B, SOURCE.VALUE_N, SOURCE.VALUE_S, SOURCE.VALUE_T, CURRENT TIMESTAMP)") % (
                   dbhelper.quotingSchemaName(self.schema), dbhelper.quotingTableName(tableName), parmExtension,
                   colExtension, joinExtension, colExtension, sourceExtension)

    def create_upsert_statement_postgres_sql(self, tableName, grain):
        dimensions = []
        if grain is None or len(grain) == 0:
            dimensions.append(KPI_ENTITY_ID_COLUMN.lower())
            dimensions.append('timestamp')
        else:
            if grain[2]:
                dimensions.append(KPI_ENTITY_ID_COLUMN.lower())
            if grain[0] is not None:
                dimensions.append('timestamp')
            if grain[1] is not None:
                dimensions.extend(grain[1])

        colExtension = ''
        parmExtension = ''

        for dimension in dimensions:
            # Note: the dimension grain need to be in lower case since the table will be created with lowercase column.
            quoted_dimension = dbhelper.quotingColumnName(dimension.lower(), self.is_postgre_sql)
            colExtension += ', ' + quoted_dimension
            parmExtension += ', %s'

        sql = "insert into " + self.schema + "." + tableName + " (key " + colExtension + ",value_b,value_n,value_s,value_t,last_update) values (%s " + parmExtension + ", %s, %s, %s, %s, current_timestamp) on conflict on constraint uc_" + tableName + " do update set value_b = EXCLUDED.value_b, value_n = EXCLUDED.value_n, value_s = EXCLUDED.value_s, value_t = EXCLUDED.value_t, last_update = EXCLUDED.last_update"
        return sql


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

        self.quoted_schema = dbhelper.quotingSchemaName(dms.default_db_schema, self.dms.is_postgre_sql)
        self.quoted_table_name = dbhelper.quotingTableName(self.ALERT_TABLE_NAME, self.dms.is_postgre_sql)
        self.alert_to_kpi_input_dict = dict()
        self.alerts_to_msg_hub = []
        self.alerts_to_db = []
        self.alert_name_to_alert_id = {}

        if alerts is not None:
            self.alerts_to_msg_hub = alerts
        elif data_item_names is not None:
            alert_catalogs = dms.catalog.get_alerts()
            for data_item_name in asList(data_item_names):
                metadata = dms.data_items.get(data_item_name)
                kpi_func_dto = metadata.get(md.DATA_ITEM_KPI_FUNCTION_DTO_KEY, None)
                kpi_function_name = kpi_func_dto.get(md.DATA_ITEM_KPI_FUNCTION_DTO_FUNCTION_NAME, None)
                alert_catalog = alert_catalogs.get(kpi_function_name, None)
                if alert_catalog is not None:
                    self.alerts_to_db.append(data_item_name)
                    self.alert_to_kpi_input_dict[data_item_name] = kpi_func_dto.get('input')
                    if md.DATA_ITEM_TAG_ALERT in metadata.get(md.DATA_ITEM_TAGS_KEY, []):
                        self.alerts_to_msg_hub.append(data_item_name)
                self.alert_name_to_alert_id[data_item_name] = metadata.get(md.DATA_ITEM_ID);
        else:
            raise RuntimeError("Invalid combination of parameters: Either alerts or data_item_names must be provided.")

        self.message_hub = MessageHub()

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

                    # Extract alert column from data frame as series
                    alert_series = df[alert_name]

                    # Remove all values from series which are not equal to 'True' and only keep index of new series
                    calc_alert_events = alert_series[(alert_series == True)].index

                    if calc_alert_events.size > 0:
                        # Get earliest and latest timestamp of all alert events
                        timestamp_level = calc_alert_events.get_level_values(self.dms.eventTimestampName)
                        earliest = timestamp_level.min()
                        latest = timestamp_level.max()

                        # Retrieve existing alert events from database (table DM_WIOT_AS_ALERT) as index object
                        existing_alert_events = self._get_alert_events_from_db(alert_name=alert_name,
                                                                               index_has_entity_id=index_has_entity_id,
                                                                               start_ts=earliest, end_ts=latest)

                        # Determine all alert events which have been calculated in this pipeline run but which do not
                        # exist in database yet
                        tmp = calc_alert_events.difference(existing_alert_events)
                        new_alert_events[alert_name] = tmp

                        logger.info(f"{tmp.size} out of {calc_alert_events.size} calculated alert events for "
                                    f"alert {alert_name} are new alert events.")
                    else:
                        new_alert_events[alert_name] = calc_alert_events

                        logger.info(f"There are no calculated alert events for alert {alert_name}")

                # Push new alert events to database and Data Dictionary
                self._push_alert_events_to_db_dd(new_alert_events, index_has_entity_id)

                # Push new alerts events to message hub if required
                if len(self.alerts_to_msg_hub) > 0:
                    self._push_alert_events_to_msg_hub(df, new_alert_events)

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

        timestamp_col_name = 'timestamp'
        entity_id_col_name = 'entity_id'

        if index_has_entity_id is True:
            select_entity_id = f"{entity_id_col_name} as \"{entity_id_col_name}\", "
            index_col_names = [entity_id_col_name, timestamp_col_name]
            requested_col_names = ['id', self.dms.eventTimestampName]
        else:
            select_entity_id = ''
            index_col_names = [timestamp_col_name]
            requested_col_names = [self.dms.eventTimestampName]

        # Important: Explicitly set lower-case alias for timestamp column. Otherwise the column name in data frame
        # will be in uppercase for DB2 because sqlalchemy attempts to avoid name clashes with the possibly reserved
        # keyword TIMESTAMP by quoting
        sql_statement = f"SELECT {select_entity_id}{timestamp_col_name} as \"{timestamp_col_name}\" FROM " \
                        f"{self.quoted_schema}.{self.quoted_table_name} " \
                        f"WHERE entity_type_id = '{self.dms.entity_type_id}' AND data_item_name = '{alert_name}'"

        if start_ts is not None:
            sql_statement += f" AND {timestamp_col_name} >= '{str(start_ts)}'"
        if end_ts is not None:
            sql_statement += f" AND {timestamp_col_name} <= '{str(end_ts)}'"

        result_df = self.dms.db.read_sql_query(sql_statement, parse_dates=[timestamp_col_name],
                                               index_col=index_col_names,
                                               requested_col_names=requested_col_names,
                                               log_message=f"Sql statement for alert {alert_name}")

        logger.debug(f"{result_df.shape[0]} alert events have been read from database.")

        return result_df.index

    def _push_alert_events_to_db_dd(self, alert_events, index_has_entity_id):

        # Push alert events to DB and Data Dictionary in parallel to reduce computational effort
        dd_builder = None
        msg_dd = ""
        if self.dms.db.dd_client is not None:
            dd_builder = self.dms.db.dd_client.builder()
            msg_dd = "and Data Dictionary each "

        sql_statement = self._get_sql_statement()

        total_count = 0
        rows = []
        start_time = dt.datetime.now()
        entity_type_ddId = self.dms.entity_type.get("resourceDdId")
        is_device_type = (self.dms.entity_type.get("resourceType") == "DEVICE_TYPE")

        for alert_name, index in alert_events.items():

            # Get attributes dedicated to this alert
            kpi_input = self.alert_to_kpi_input_dict.get(alert_name)
            severity = kpi_input.get('Severity')
            priority = kpi_input.get('Priority')
            domain_status = kpi_input.get('Status')
            alert_id = self.alert_name_to_alert_id.get(alert_name)

            for index_values in index:
                # Distinguish with/without entity id
                if index_has_entity_id is True:
                    tmp_entity_id = index_values[0]
                    tmp_timestamp = index_values[1]
                else:
                    tmp_entity_id = None
                    tmp_timestamp = index_values

                # Setup alert event for DB
                rows.append((self.dms.entity_type_id, alert_name, tmp_entity_id, tmp_timestamp, severity, priority,
                             domain_status))

                if dd_builder is not None:
                    # Setup alert event for Data Dictionary
                    timestamp_nano_seconds = int(tmp_timestamp)
                    alert_attributes = {"dimensions": None, "alertId": alert_id, "name": alert_name,
                                        "assetId": tmp_entity_id, "assetAlias": None, "owner": None,
                                        "severity": severity, "status": domain_status, "priority": priority,
                                        "timestamp": timestamp_nano_seconds, "actions": None}
                    alert_event_name = f"{self.dms.entity_type_id}?{alert_id}?{timestamp_nano_seconds}"
                    if tmp_entity_id is not None:
                        alert_event_name = f"{alert_event_name}?{tmp_entity_id}"
                    alert_ddId = EntryType.compute_mas_key(EntryType.Alert.mas_key_prefix, alert_event_name)
                    if is_device_type is True:
                        dd_builder = dd_builder.mas_alert_type(entity_type_ddId, alert_ddId, alert_name)\
                            .set_p(alert_attributes).hasAlertType(entity_type_ddId, alert_ddId)\
                            .p("timestamp", timestamp_nano_seconds)
                    else:
                        dd_builder = dd_builder.mas_alert(entity_type_ddId, alert_ddId, EntryType.Alert.mas_type(),
                                                          alert_name).set_p(alert_attributes)

                if len(rows) == DATALAKE_BATCH_UPDATE_ROWS:
                    # Push alert events in list 'rows' in chunks to alert table in database
                    total_count += self._push_rows_to_db(sql_statement, rows)
                    rows.clear()
                    logger.info(f"{total_count} alert events have been written to alert table so far.")

                    if dd_builder is not None:
                        # Push alert events to Data Dictionary and create a new builder
                        dd_builder.send()
                        dd_builder = self.dms.db.dd_client.builder()
                        logger.info(f"{total_count} alert events have been written to Data Dictionary so far.")

        # Push all remaining rows to database
        if len(rows) > 0:
            # Push all remaining alert events (= rows) to database
            total_count += self._push_rows_to_db(sql_statement, rows)

            if dd_builder is not None:
                # Push all remaining alert events to Data Dictionary
                dd_builder.send()

        logger.info(f"A total of {total_count} alert events have been written to alert table {msg_dd}"
                    f"in {(dt.datetime.now() - start_time).total_seconds()} seconds.")

    def _get_sql_statement(self):

        if self.dms.is_postgre_sql:
            available_columns = ['entity_type_id', 'data_item_name', 'entity_id', 'timestamp', 'severity', 'priority',
                                 'domain_status']
            all_columns_list = ', '.join(available_columns)
            statement = f"insert into {self.quoted_schema}.{self.quoted_table_name} ({all_columns_list}) " \
                        f"values ({', '.join(['%s'] * len(available_columns))} ) " \
                        f"on conflict on constraint uc_{self.ALERT_TABLE_NAME} do nothing "
        else:
            available_columns = ['ENTITY_TYPE_ID', 'DATA_ITEM_NAME', 'ENTITY_ID', 'TIMESTAMP', 'SEVERITY', 'PRIORITY',
                                 'DOMAIN_STATUS']
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
            if self.dms.is_postgre_sql:
                dbhelper.execute_batch(self.dms.db_connection, sql=sql_statement, params_list=rows,
                                       page_size=DATALAKE_BATCH_UPDATE_ROWS)
                row_count = len(rows)
            else:
                res = ibm_db.execute_many(sql_statement, tuple(rows))
                row_count = res if res is not None else ibm_db.num_rows(sql_statement)
        except Exception as ex:
            raise RuntimeError(f"The attempt to write {len(rows)} alert events to alert table in database "
                               f"failed.") from ex

        return row_count

    def _push_alert_events_to_msg_hub(self, df, new_alert_events):

        key_and_msg = []

        for alert_name in self.alerts_to_msg_hub:
            index = new_alert_events[alert_name]
            df_alert_events = df.reindex(index)

            for df_row in df_alert_events.itertuples(index=True, name=None):
                # publish alert format
                # key: <tenant-id>|<entity-type-name>|<alert-name>
                # value: json document containing all metrics at the same time / same device / same grain
                key = f"{self.dms.tenant_id}|{self.entity_type_name}|{alert_name}"
                msg = self._get_json_values(index.names, df_alert_events.columns, df_row)
                key_and_msg.append((key, msg))

        logger.info(f"Pushing {len(key_and_msg)} alert events to Message Hub.")

        self.message_hub.produce_batch_alert_to_default_topic(key_and_msg=key_and_msg)

    def _get_json_values(self, index_names, col_names, row):

        # Create a json string with a list of index names and column names with their corresponding values

        index_json = {}
        if len(index_names) == 1:
            index_json[index_names[0]] = row[0]
        else:
            for index_name, index_value in zip(index_names, row[0]):
                index_json[index_name] = index_value

        values = {}
        for col_name, value in zip(col_names, row[1:]):
            values[col_name] = value
        values["index"] = index_json

        # Timestamp is not serialized by default by json.dumps(). Therefore timestamps must be explicitly
        # converted to string by _serialize_converter()
        return json.dumps(values, default=self._serialize_converter)

    @staticmethod
    def _serialize_converter(obj):
        if isinstance(obj, dt.datetime):
            return obj.__str__()
        else:
            raise TypeError(f"Do not know how to convert object of class {obj.__class__.__name__} to JSON")


class RecordUsage:

    def __init__(self, dms, function_kpi_generated, start_ts, completed=False):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if dms is None:
            raise RuntimeError("argument dms must be provided")
        if function_kpi_generated is None or not isinstance(function_kpi_generated, list):
            raise RuntimeError("argument function_kpi_generated must be provided as a list")
        if start_ts is None:
            raise RuntimeError("argument start_ts must be provided")

        self.dms = dms
        self.function_kpi_generated = function_kpi_generated
        self.start_ts = start_ts
        self.completed = completed

    def execute(self, df, *args, **kwargs):
        if self.dms.production_mode:
            end_ts = None
            if self.completed:
                end_ts = pd.Timestamp.utcnow().tz_convert(tz=None)

            usage = []
            for fname, kfname, kpis, kpiFunctionId in self.function_kpi_generated:
                total_records = None
                if self.completed:
                    total_records = 0
                    for kpi in kpis:
                        try:
                            records = df[kpi].dropna().shape[0]
                            total_records += records
                            self.logger.info('fname=%s, kpi=%s, records=%d' % (fname, kpi, records))
                        except KeyError:
                            self.logger.info('not able to calculate usage for the %s ' % kpi)
                usage.append({"entityTypeName": self.dms.entity_type, "kpiFunctionName": kfname,
                              "startTimestamp": self.start_ts.value // 1000000,
                              "endTimestamp": (end_ts.value // 1000000) if self.completed else None,
                              "numberOfResultsProcessed": total_records, })

            self.logger.info('usage_records=%s' % str(usage))

            if len(usage) > 0:
                # util.api_request(USAGE_REQUEST_TEMPLATE.format(self.dms.tenant_id), method='post', json=usage)
                try:
                    self.dms.db.http_request(object_type='usage', object_name='', request='POST', payload=usage)
                except BaseException as e:
                    msg = 'Unable to write usage. %s' % str(e)
                    self.logger.error(msg)
        else:
            self.logger.info("***** The RecordUsage is not stored into the database. ***** ")

        return df


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
        for col_name, col_type in df.dtypes.iteritems():
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
        # kohlmann full_table_name ?????
        full_table_name = '%s.%s' % (self.schema_name, table_name)
        table_object = Table(table_name.lower(), self.db_metadata, autoload=True, autoload_with=self.db_connection)

        return table_object
