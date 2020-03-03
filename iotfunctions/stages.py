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
import json
import ibm_db
import datetime as dt
import numpy as np
from collections import defaultdict
from sqlalchemy import (MetaData, Table)
from . import dbhelper
from .util import MessageHub, asList
from .exceptions import StageException, DataWriterException

# Kohlmann verify location of the following constants
DATA_ITEM_TYPE_BOOLEAN = 'BOOLEAN'
DATA_ITEM_TYPE_NUMBER = 'NUMBER'
DATA_ITEM_TYPE_LITERAL = 'LITERAL'
DATA_ITEM_TYPE_TIMESTAMP = 'TIMESTAMP'

DATA_ITEM_COLUMN_TYPE_KEY = 'columnType'
DATA_ITEM_TRANSIENT_KEY = 'transient'
DATA_ITEM_SOURCETABLE_KEY = 'sourceTableName'
DATA_ITEM_KPI_FUNCTION_DTO_KEY = 'kpiFunctionDto'
DATA_ITEM_KPI_FUNCTION_DTO_FUNCTION_NAME = 'functionName'
DATA_ITEM_TAG_ALERT = 'ALERT'
DATA_ITEM_TAGS_KEY = 'tags'
DATA_ITEM_TYPE_KEY = 'type'

logger = logging.getLogger(__name__)

DATALAKE_BATCH_UPDATE_ROWS = 5000
KPI_ENTITY_ID_COLUMN = 'ENTITY_ID'


class PersistColumns:

    def __init__(self, dms, sources=None):
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

    def execute(self, df):
        self.logger.debug('columns_to_persist=%s, df_columns=%s' % (str(self.sources), str(df.dtypes.to_dict())))

        t1 = dt.datetime.now()
        self.store_derived_metrics(df[list(set(self.sources) & set(df.columns))])
        t2 = dt.datetime.now()
        if self.dms.production_mode:
            self.logger.info("persist_data_time_seconds=%s" % (t2 - t1).total_seconds())
        self.dms.create_checkpoint_entries(df)
        t3 = dt.datetime.now()
        if self.dms.production_mode:
            self.logger.info("checkpoint_time_seconds=%s" % (t3 - t2).total_seconds())

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
                if source_metadata.get(DATA_ITEM_TRANSIENT_KEY) == True:
                    self.logger.debug('skip persisting transient data_item=%s' % source)
                    continue

                # if source not in (get_all_kpi_targets(self.get_pipeline()) & self.data_items.get_derived_metrics()):
                #     continue

                try:
                    tableName = source_metadata.get(DATA_ITEM_SOURCETABLE_KEY)
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
                if source_metadata.get(DATA_ITEM_COLUMN_TYPE_KEY) == DATA_ITEM_TYPE_NUMBER:
                    value_number = True
                # elif dtype.startswith('bool'):
                elif source_metadata.get(DATA_ITEM_COLUMN_TYPE_KEY) == DATA_ITEM_TYPE_BOOLEAN:
                    value_bool = True
                # elif dtype.startswith('datetime'):
                elif source_metadata.get(DATA_ITEM_COLUMN_TYPE_KEY) == DATA_ITEM_TYPE_TIMESTAMP:
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
                            except Exception:
                                self.logger.warning('Error persisting derived metrics, valueList=%s' % str(valueList),
                                                    exc_info=True)

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
                    except Exception:
                        self.logger.warning('Error persisting derived metrics, valueList = %s' % str(valueList),
                                            exc_info=True)

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

    def __init__(self, dms, alerts=None, all_cols=None, **kwargs):

        if dms is None:
            raise RuntimeError("argument dms must be provided")
        if alerts is None and all_cols is None:
            raise RuntimeError("either alerts argument or all_cols arguments must be provided")

        self.dms = dms

        try:
            self.entity_type_name = dms.logical_name
        except AttributeError:
            self.entity_type_name = dms.entity_type

        self.entity_type_id = dms.entityTypeId
        self.is_postgre_sql = dms.is_postgre_sql
        self.db_connection = dms.db_connection
        self.schema = dms.schema
        self.alert_to_kpi_input_dict = dict()
        self.alerts_to_message_hub = []
        self.alerts_to_database = []
        self.alert_catalogs = dms.catalog.get_alerts()
        if alerts is None:
            if all_cols is not None:
                for alert_data_item in asList(all_cols):
                    metadata = dms.data_items.get(alert_data_item)
                    if metadata is not None:
                        if DATA_ITEM_TAG_ALERT in metadata.get(DATA_ITEM_TAGS_KEY, []):
                            self.alerts_to_message_hub.append(alert_data_item)

                        kpi_func_dto = metadata.get(DATA_ITEM_KPI_FUNCTION_DTO_KEY, None)
                        kpi_function_name = kpi_func_dto.get(DATA_ITEM_KPI_FUNCTION_DTO_FUNCTION_NAME, None)
                        alert_catalog = self.alert_catalogs.get(kpi_function_name, None)
                        if alert_catalog is not None:
                            self.alerts_to_database.append(alert_data_item)
                            self.alert_to_kpi_input_dict[alert_data_item] = kpi_func_dto.get('input')

        else:
            self.alerts_to_message_hub = alerts
        self.message_hub = MessageHub()

    def __str__(self):

        return 'System generated ProduceAlerts stage'

    def execute(self, df, start_ts=None, end_ts=None):

        logger.info('alerts_to_produce into message hub = %s ' % str(self.alerts_to_message_hub))
        logger.info('alerts_to_produce into database = %s ' % str(self.alerts_to_database))

        key_and_msg = []
        key_and_msg_updated = []
        key_and_msg_and_db_parameter = []

        if len(self.alerts_to_message_hub) > 0:

            filtered_alerts = []
            alert_filter = None

            # pre-filtering the data frame to be just those rows with True alert column values.
            # Iterating through the whole data frame is a slow process.
            for alert_name in self.alerts_to_message_hub:
                if alert_name in df.columns:
                    if alert_filter is None:
                        alert_filter = (df[alert_name] == True)
                    else:
                        alert_filter = alert_filter | (df[alert_name] == True)
                    filtered_alerts.append(alert_name)

            filtered_df = df[alert_filter]
            index_names = filtered_df.index.names

            # for df_row in filtered_df.itertuples():
            for index_value, row in filtered_df.iterrows():
                for alert_name in filtered_alerts:
                    # derived_value = getattr(payload, alert)

                    if row[alert_name]:
                        # publish alert format
                        # key: <tenant-id>|<entity-type-name>|<alert-name>
                        # value: json document containing all metrics at the same time / same device / same grain
                        key = '%s|%s|%s' % (self.dms.tenant_id, self.entity_type_name, alert_name)
                        value = self.get_json_values(index_names, index_value, row)

                        if alert_name in self.alerts_to_database:
                            kpi_input = self.alert_to_kpi_input_dict.get(alert_name)
                            db_insert_parameter = (
                                index_value[0], index_value[1], self.entity_type_id, self.entity_type_name, alert_name,
                                kpi_input.get('Severity', None), kpi_input.get('Priority', None),
                                kpi_input.get('Status', None))
                            key_and_msg_and_db_parameter.append((key, value, db_insert_parameter))
                        else:
                            key_and_msg.append((key, value))

        else:
            logger.debug("No alerts to produce for %s." % self.entity_type_name)
            return df

        if len(key_and_msg_and_db_parameter) > 0:
            try:
                key_and_msg_updated = self.insert_data_into_alert_table(key_and_msg_and_db_parameter)
            except Exception as ex:
                # TODO:: Remove the exception once you create dm_wiot_as_alert table for all the tenant.
                self.logger.warning('Inserting data into alert table failed: %s' % str(ex))

        if len(key_and_msg) > 0 or len(key_and_msg_updated) > 0:
            # TODO:: Duplicate alert issue is still exist.
            key_and_msg_merged = []
            key_and_msg_merged.extend(key_and_msg)
            key_and_msg_merged.extend(key_and_msg_updated)
            self.message_hub.produce_batch_alert_to_default_topic(key_and_msg=key_and_msg_merged)

        return df

    '''
    Timestamp is not serialized by default by json.dumps()
    It is necessary to convert the object to string
    '''

    def _serialize_converter(self, object):
        if isinstance(object, dt.datetime):
            return object.__str__()

    '''
    This function creates a json string which consist of dataframe row records and index key, value .
    '''

    def get_json_values(self, index_names, index_values, row):

        index_json = {}
        for index_name, index_value in zip(index_names, index_values):
            index_json[index_name] = index_value

        value = row.to_json()
        updated_value = json.loads(value)
        updated_value["index"] = index_json
        return json.dumps(updated_value, default=self._serialize_converter)

    def insert_data_into_alert_table(self, key_and_msg_and_db_parameter=[]):
        logger.info("Processing %s alerts. This alert may contain duplicates, "
                    "so need to process the alert before inserting into Database." % len(key_and_msg_and_db_parameter))
        updated_key_and_msg = []
        postgres_sql = "insert into " + self.schema + ".dm_wiot_as_alert (entity_id, timestamp, entity_type_id, entity_type_name, data_item_name,  severity, priority,domain_status) values (%s, %s, %s, %s, %s, %s, %s, %s)"
        db2_sql = "insert into " + self.schema + ".DM_WIOT_AS_ALERT (ENTITY_ID, TIMESTAMP, ENTITY_TYPE_ID, ENTITY_TYPE_NAME, DATA_ITEM_NAME,  SEVERITY, PRIORITY,DOMAIN_STATUS) values (?, ?, ?, ?, ?, ?, ?, ?) "
        start_time = dt.datetime.now()

        for key, msg, db_params in key_and_msg_and_db_parameter:
            try:
                if self.is_postgre_sql:
                    dbhelper.execute_postgre_sql_query(self.db_connection, sql=postgres_sql, params=db_params)
                else:
                    stmt = ibm_db.prepare(self.db_connection, db2_sql)
                    for i, param in enumerate(iterable=db_params, start=1):
                        ibm_db.bind_param(stmt, i, param)

                    ibm_db.execute(stmt)

                updated_key_and_msg.append((key, msg))

            except Exception as ex:
                if self.is_postgre_sql:
                    if ex.pgcode == '23505':
                        continue
                else:
                    if "SQLSTATE=23505" in ex.args[0]:
                        continue

                raise ex

        end_time = dt.datetime.now()
        logger.info("Total alerts produced to database = %d " % len(updated_key_and_msg))
        logger.info(
            "Total time taken to insert the alert to database = %s seconds." % (end_time - start_time).total_seconds())

        return updated_key_and_msg


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
                end_ts = pd.Timestamp('today')

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

        return df


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
            if metadata is not None and metadata.get(DATA_ITEM_TYPE_KEY).upper() == 'DERIVED_METRIC':
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
