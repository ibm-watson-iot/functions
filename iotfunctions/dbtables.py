import logging
import ibm_db
from pathlib import Path
import pyarrow as pa
import pandas as pd
import os
import psycopg2
import dill as pickle

from iotfunctions import dbhelper

logger = logging.getLogger(__name__)


class DBDataCache:

    PARQUET_DIRECTORY = 'parquet'
    CACHE_TABLENAME = 'KPI_DATA_CACHE'
    CACHE_FILE_STEM = 'df_parquet'

    def __init__(self, tenant_id, entity_type_id, schema, db_connection, db_type):

        self.tenant_id = tenant_id
        self.entity_type_id = entity_type_id
        self.schema = schema
        self.db_connection = db_connection
        self.db_type = db_type

        if self.db_type == 'db2':
            self.is_postgre_sql = False
            self.schema = schema.upper()
            self.cache_tablename = DBDataCache.CACHE_TABLENAME.upper()
        elif self.db_type == 'postgresql':
            self.is_postgre_sql = True
            self.schema = schema.lower()
            self.cache_tablename = DBDataCache.CACHE_TABLENAME.lower()
        else:
            raise Exception('Initialization of %s failed because the database type %s is unknown.'
                            % (self.__class__.__name__, self.db_type))

        self.quoted_schema = dbhelper.quotingSchemaName(self.schema, self.is_postgre_sql)
        self.quoted_cache_tablename = dbhelper.quotingTableName(self.cache_tablename, self.is_postgre_sql)

        self._handle_cache_table()

    def _create_cache_table(self):

        if not self.is_postgre_sql:

            sql_statement = "CREATE TABLE %s.%s ( " \
                            "ENTITY_TYPE_ID BIGINT NOT NULL, " \
                            "PARQUET_NAME VARCHAR(2048) NOT NULL, " \
                            "PARQUET_FILE BLOB(2G), " \
                            "UPDATED_TS TIMESTAMP  NOT NULL DEFAULT CURRENT TIMESTAMP, " \
                            "CONSTRAINT %s UNIQUE(entity_type_id, parquet_name)) " \
                            "ORGANIZE BY ROW" \
                            % (self.quoted_schema, self.quoted_cache_tablename,
                               dbhelper.quotingTableName('uc_%s' % self.cache_tablename, self.is_postgre_sql))
            try:
                stmt = ibm_db.exec_immediate(self.db_connection, sql_statement)
                ibm_db.free_result(stmt)
            except Exception as ex:
                raise Exception('Execution of sql statement "%s" failed.' % sql_statement) from ex
        else:
            sql_statement = "CREATE TABLE %s.%s ( " \
                            "entity_type_id BIGINT NOT NULL, " \
                            "parquet_name VARCHAR(2048) NOT NULL, " \
                            "parquet_file BYTEA, " \
                            "updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " \
                            "CONSTRAINT %s UNIQUE(entity_type_id, parquet_name))" \
                            % (self.quoted_schema, self.quoted_cache_tablename,
                               dbhelper.quotingTableName('uc_%s' % self.cache_tablename, self.is_postgre_sql))
            try:
                dbhelper.execute_postgre_sql_query(self.db_connection, sql_statement)
            except Exception as ex:
                raise Exception('Execution of sql statement "%s" failed.' % sql_statement) from ex

        logger.info('Table %s.%s has been created.' % (self.quoted_schema, self.quoted_cache_tablename))

    def _cache_table_exists(self):
        exists = False
        try:
            if not self.is_postgre_sql:
                stmt = ibm_db.tables(self.db_connection, None, self.schema, self.cache_tablename, None)
                try:
                    fetch_value = ibm_db.fetch_row(stmt, 0)
                    if fetch_value:
                        exists = True
                finally:
                    ibm_db.free_result(stmt)
            else:
                exists = dbhelper.check_table_exist(self.db_connection, self.db_type, self.schema, self.cache_tablename)

        except Exception as ex:
            raise Exception('Error while probing for table %s.%s'
                            % (self.quoted_schema, self.quoted_cache_tablename)) from ex

        logger.debug('Table %s.%s %s.'
                     % (self.quoted_schema, self.quoted_cache_tablename, 'exists' if exists else 'does not exist'))

        return exists

    def _handle_cache_table(self):
        if not self._cache_table_exists():
            self._create_cache_table()

    def _push_cache(self, cache_filename, cache_pathname):

        if not self.is_postgre_sql:

            sql_statement = "MERGE INTO %s.%s AS TARGET " \
                            "USING (VALUES (?, ?, ?, CURRENT_TIMESTAMP)) " \
                            "AS SOURCE (ENTITY_TYPE_ID, PARQUET_NAME, PARQUET_FILE, UPDATED_TS) " \
                            "ON TARGET.ENTITY_TYPE_ID = SOURCE.ENTITY_TYPE_ID " \
                            "AND TARGET.PARQUET_NAME = SOURCE.PARQUET_NAME " \
                            "WHEN MATCHED THEN " \
                            "UPDATE SET TARGET.PARQUET_FILE = SOURCE.PARQUET_FILE, " \
                            "TARGET.UPDATED_TS = SOURCE.UPDATED_TS " \
                            "WHEN NOT MATCHED THEN " \
                            "INSERT (ENTITY_TYPE_ID, PARQUET_NAME, PARQUET_FILE, UPDATED_TS) " \
                            "VALUES (SOURCE.ENTITY_TYPE_ID, SOURCE.PARQUET_NAME, SOURCE.PARQUET_FILE, " \
                            "SOURCE.UPDATED_TS)" \
                            % (self.quoted_schema, self.quoted_cache_tablename)
            try:
                stmt = ibm_db.prepare(self.db_connection, sql_statement)

                try:
                    ibm_db.bind_param(stmt, 1, self.entity_type_id)
                    ibm_db.bind_param(stmt, 2, cache_filename)
                    ibm_db.bind_param(stmt, 3, cache_pathname, ibm_db.PARAM_FILE, ibm_db.SQL_BLOB)
                    ibm_db.execute(stmt)
                finally:
                    ibm_db.free_result(stmt)
            except Exception as ex:
                raise Exception('Storing cache file %s under name %s failed with sql statement "%s"'
                                % (cache_pathname, cache_filename, sql_statement)) from ex

        else:
            try:
                f = open(cache_pathname, 'rb')
                try:
                    blob = f.read()
                finally:
                    f.close()
            except Exception as ex:
                raise Exception('The cache file %s could not be read from disc.' % cache_pathname) from ex
            else:
                statement1 = "INSERT INTO %s.%s (entity_type_id, parquet_name, parquet_file, updated_ts) " \
                             % (self.quoted_schema, self.quoted_cache_tablename)

                statement3 = "ON CONFLICT ON CONSTRAINT %s DO update set entity_type_id = EXCLUDED.entity_type_id, " \
                             "parquet_name = EXCLUDED.parquet_name, parquet_file = EXCLUDED.parquet_file, " \
                             "updated_ts = EXCLUDED.updated_ts" \
                             % dbhelper.quotingTableName(('uc_%s' % self.cache_tablename), self.is_postgre_sql)

                sql_statement = statement1 + " values (%s, %s, %s, current_timestamp) " + statement3

                try:
                    dbhelper.execute_postgre_sql_query(self.db_connection, sql_statement,
                                                       (self.entity_type_id, cache_filename, psycopg2.Binary(blob)))
                except Exception as ex:
                    raise Exception('Storing cache under name %s failed with sql statement "%s"'
                                    % (cache_filename, sql_statement)) from ex

        logger.info('Cache has been stored under name %s in table %s.%s' % (cache_filename, self.quoted_schema,
                                                                            self.quoted_cache_tablename))

    def _get_cache(self, cache_filename, cache_pathname):
        # Remove file on disc if there is one
        try:
            if os.path.exists(cache_pathname):
                os.remove(cache_pathname)
        except Exception as ex:
            raise Exception('Removal of old cache file %s failed.' % cache_pathname) from ex

        if not self.is_postgre_sql:
            sql_statement = "SELECT PARQUET_FILE FROM %s.%s WHERE ENTITY_TYPE_ID = ? AND PARQUET_NAME = ?" \
                            % (self.quoted_schema, self.quoted_cache_tablename)

            stmt = ibm_db.prepare(self.db_connection, sql_statement)

            try:
                ibm_db.bind_param(stmt, 1, self.entity_type_id)
                ibm_db.bind_param(stmt, 2, cache_filename)
                ibm_db.execute(stmt)
                row = ibm_db.fetch_tuple(stmt)
                if row is False:
                    row = None
            except Exception as ex:
                raise Exception('Retrieval of cache %s failed with sql statement "%s"'
                                % (cache_filename, sql_statement)) from ex
            finally:
                ibm_db.free_result(stmt)
        else:
            sql_statement = 'SELECT parquet_file FROM %s.%s' % (self.quoted_schema, self.quoted_cache_tablename)
            sql_statement += ' WHERE entity_type_id = %s AND parquet_name = %s'

            try:
                row = dbhelper.execute_postgre_sql_select_query(self.db_connection, sql_statement,
                                                                (self.entity_type_id, cache_filename),
                                                                fetch_one_only=True)
            except Exception as ex:
                raise Exception('Retrieval of cache %s failed with sql statement "%s"'
                                % (cache_filename, sql_statement)) from ex

        if row is not None:
            parquet = row[0]
            if parquet is not None and len(parquet) > 0:
                try:
                    f = open(cache_pathname, "wb")
                    try:
                        f.write(parquet)
                        logger.info('Cache %s has been retrieved from table %s.%s and stored under %s'
                                    % (cache_filename, self.quoted_schema, self.quoted_cache_tablename, cache_pathname))
                    finally:
                        f.close()
                except Exception as ex:
                    raise Exception('Writing cache file %s to disc failed.' % cache_pathname) from ex
            else:
                logger.info('The cache %s is empty' % cache_filename)
        else:
            logger.info('No cache found for %s' % cache_filename)

    def _get_cache_filename(self, dep_grain, grain):

        # Create local path for cache file on disk.
        base_path = '%s/%s/%d' % (DBDataCache.PARQUET_DIRECTORY, self.tenant_id, self.entity_type_id)
        Path(base_path).mkdir(parents=True, exist_ok=True)

        # Assemble filename and full pathname of cache file
        src = '%s_%s_%s' % (str(dep_grain[0]), str('_'.join(dep_grain[1])), str(dep_grain[2])) \
            if dep_grain is not None else str(None)
        tar = '%s_%s_%s' % (str(grain[0]), str('_'.join(grain[1])), str(grain[2])) \
            if grain is not None else str(None)
        filename = '%s__%s__%s' % (DBDataCache.CACHE_FILE_STEM, src, tar)
        local_path = '%s/%s' % (base_path, filename)

        return filename, local_path, base_path

    def store_cache(self, dep_grain, grain, df):

        cache_filename, cache_pathname, base_path = self._get_cache_filename(dep_grain, grain)

        if df is not None:
            try:
                df.to_parquet(cache_pathname)
                logger.info('Cache %s of size %s has been saved to file %s'
                            % (cache_filename, str(df.shape), cache_pathname))
            except pa.lib.ArrowInvalid as ex:
                raise Exception('The dataframe could not be saved to file %s because pyarrow threw an exception.'
                                % cache_pathname) from ex
            except Exception as ex:
                raise Exception('The dataframe could not be saved to file %s.' % cache_pathname) from ex
            else:
                self._push_cache(cache_filename, cache_pathname)
        else:
            logger.warning('Dataframe is None. Therefore no cache has been stored in database.')

    def retrieve_cache(self, dep_grain, grain):

        cache_filename, cache_pathname, base_path = self._get_cache_filename(dep_grain, grain)
        self._get_cache(cache_filename, cache_pathname)
        df_loaded = None
        if os.path.exists(cache_pathname):
            try:
                df_loaded = pd.read_parquet(cache_pathname)
                if df_loaded is not None:
                    logger.info('Cache %s of size %s has been retrieved from file %s'
                                % (cache_filename, str(df_loaded.shape), cache_pathname))
            except Exception as ex:
                raise Exception('The dataframe could not be loaded from parquet file %s' % cache_pathname) from ex

        return df_loaded

    def delete_all_caches(self):
        # Delete all cache entries for this entity type locally
        cache_filename, cache_pathname, base_path = self._get_cache_filename(None, None)
        if os.path.exists(base_path):
            try:
                file_listing = os.listdir(base_path)
            except Exception as ex:
                raise Exception('Failure to list content of directory %s' % base_path) from ex

            for filename in file_listing:
                if filename.startswith(DBDataCache.CACHE_FILE_STEM):
                    full_path = '%s/%s' % (base_path, filename)
                    try:
                        os.remove(full_path)
                    except Exception as ex:
                        raise Exception('Removal of file %s failed' % full_path) from ex

        # Delete all cache entries for this entity type in database
        if not self.is_postgre_sql:
            sql_statement = "DELETE FROM %s.%s where ENTITY_TYPE_ID = ?" % (self.quoted_schema,
                                                                            self.quoted_cache_tablename)

            try:
                stmt = ibm_db.prepare(self.db_connection, sql_statement)

                try:
                    ibm_db.bind_param(stmt, 1, self.entity_type_id)
                    ibm_db.execute(stmt)
                finally:
                    ibm_db.free_result(stmt)
            except Exception as ex:
                raise Exception('Deletion of cache files failed with sql statement "%s"' % sql_statement) from ex
        else:
            sql_statement = "DELETE FROM %s.%s" % (self.quoted_schema, self.quoted_cache_tablename,)
            sql_statement += ' where entity_type_id = %s'

            try:
                dbhelper.execute_postgre_sql_query(self.db_connection, sql_statement, (self.entity_type_id,))
            except Exception as ex:
                raise Exception('Deletion of cache files failed with sql statement %s' % sql_statement) from ex

        logger.info('All caches have been deleted from table %s.%s for entity type id %d'
                    % (self.quoted_schema, self.quoted_cache_tablename, self.entity_type_id))


class DBModelStore:

    STORE_TABLENAME = 'KPI_MODEL_STORE'

    def __init__(self, tenant_id, entity_type_id, schema, db_connection, db_type):

        self.tenant_id = tenant_id
        self.entity_type_id = entity_type_id
        self.schema = schema
        self.db_connection = db_connection
        self.db_type = db_type

        if self.db_type == 'db2':
            self.is_postgre_sql = False
            self.schema = schema.upper()
            self.store_tablename = DBModelStore.STORE_TABLENAME.upper()
        elif self.db_type == 'postgresql':
            self.is_postgre_sql = True
            self.schema = schema.lower()
            self.store_tablename = DBModelStore.STORE_TABLENAME.lower()
        else:
            raise Exception('Initialization of %s failed because the database type %s is unknown.'
                            % (self.__class__.__name__, self.db_type))

        self.quoted_schema = dbhelper.quotingSchemaName(self.schema, self.is_postgre_sql)
        self.quoted_store_tablename = dbhelper.quotingTableName(self.store_tablename, self.is_postgre_sql)

        self._handle_store_table()

    def _create_store_table(self):

        if not self.is_postgre_sql:
            sql_statement = "CREATE TABLE %s.%s ( " \
                            "ENTITY_TYPE_ID VARCHAR(2048) NOT NULL, " \
                            "MODEL_NAME VARCHAR(2048) NOT NULL, " \
                            "MODEL BLOB(2G), " \
                            "UPDATED_TS TIMESTAMP  NOT NULL DEFAULT CURRENT TIMESTAMP, " \
                            "LAST_UPDATED_BY VARCHAR(256), " \
                            "CONSTRAINT %s UNIQUE(ENTITY_TYPE_ID, MODEL_NAME) ) " \
                            "ORGANIZE BY ROW" \
                            % (self.quoted_schema, self.quoted_store_tablename,
                               dbhelper.quotingTableName('uc_%s' % self.store_tablename, self.is_postgre_sql))
            try:
                stmt = ibm_db.exec_immediate(self.db_connection, sql_statement)
                ibm_db.free_result(stmt)
            except Exception as ex:
                raise Exception('Execution of sql statement "%s" failed.' % sql_statement) from ex
        else:
            sql_statement = "CREATE TABLE %s.%s ( " \
                            "entity_type_id BIGINT NOT NULL, " \
                            "model_name VARCHAR(2048) NOT NULL, " \
                            "model BYTEA, " \
                            "updated_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, " \
                            "last_updated_by VARCHAR(256), " \
                            "CONSTRAINT %s UNIQUE(entity_type_id, model_name))" \
                            % (self.quoted_schema, self.quoted_store_tablename,
                               dbhelper.quotingTableName('uc_%s' % self.store_tablename, self.is_postgre_sql))
            try:
                dbhelper.execute_postgre_sql_query(self.db_connection, sql_statement)
            except Exception as ex:
                raise Exception('Execution of sql statement "%s" failed.' % sql_statement) from ex

        logger.info('Table %s.%s has been created.' % (self.quoted_schema, self.quoted_store_tablename))

    def _store_table_exists(self):
        exists = False
        try:
            if not self.is_postgre_sql:
                stmt = ibm_db.tables(self.db_connection, None, self.schema, self.store_tablename, None)
                try:
                    fetch_value = ibm_db.fetch_row(stmt, 0)
                    if fetch_value:
                        exists = True
                finally:
                    ibm_db.free_result(stmt)
            else:
                exists = dbhelper.check_table_exist(self.db_connection, self.db_type, self.schema, self.store_tablename)

        except Exception as ex:
            raise Exception('Error while probing for table %s.%s'
                            % (self.quoted_schema, self.quoted_store_tablename)) from ex

        logger.debug('Table %s.%s %s.'
                     % (self.quoted_schema, self.quoted_store_tablename, 'exists' if exists else 'does not exist'))

        return exists

    def _handle_store_table(self):
        if not self._store_table_exists():
            self._create_store_table()

    def store_model(self, model_name, model, user_name=None, serialize=True):

        if serialize:
            try:
                model = pickle.dumps(model)
            except Exception as ex:
                raise Exception('Serialization of model %s that is supposed to be stored in ModelStore failed.'
                                % model_name) from ex

        if not self.is_postgre_sql:
            sql_statement = "MERGE INTO %s.%s AS TARGET " \
                            "USING (VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)) " \
                            "AS SOURCE (ENTITY_TYPE_ID, MODEL_NAME, MODEL, UPDATED_TS, LAST_UPDATED_BY) " \
                            "ON TARGET.ENTITY_TYPE_ID = SOURCE.ENTITY_TYPE_ID " \
                            "AND TARGET.MODEL_NAME = SOURCE.MODEL_NAME " \
                            "WHEN MATCHED THEN " \
                            "UPDATE SET TARGET.MODEL = SOURCE.MODEL, " \
                            "TARGET.UPDATED_TS = SOURCE.UPDATED_TS " \
                            "WHEN NOT MATCHED THEN " \
                            "INSERT (ENTITY_TYPE_ID, MODEL_NAME, MODEL, UPDATED_TS, LAST_UPDATED_BY) " \
                            "VALUES (SOURCE.ENTITY_TYPE_ID, SOURCE.MODEL_NAME, SOURCE.MODEL, " \
                            "SOURCE.UPDATED_TS, SOURCE.LAST_UPDATED_BY)" \
                            % (self.quoted_schema, self.quoted_store_tablename)
            try:
                stmt = ibm_db.prepare(self.db_connection, sql_statement)

                try:
                    ibm_db.bind_param(stmt, 1, self.entity_type_id)
                    ibm_db.bind_param(stmt, 2, model_name)
                    ibm_db.bind_param(stmt, 3, model)
                    ibm_db.bind_param(stmt, 4, user_name)
                    ibm_db.execute(stmt)
                finally:
                    ibm_db.free_result(stmt)
            except Exception as ex:
                raise Exception('Storing model %s failed with sql statement "%s"'
                                % (model_name, sql_statement)) from ex
        else:
            statement1 = "INSERT INTO %s.%s (entity_type_id, model_name, model, updated_ts, last_updated_by) " \
                            % (self.quoted_schema, self.quoted_store_tablename)

            statement3 = "ON CONFLICT ON CONSTRAINT %s DO update set entity_type_id = EXCLUDED.entity_type_id, " \
                         "model_name = EXCLUDED.model_name, model = EXCLUDED.model, " \
                         "updated_ts = EXCLUDED.updated_ts, last_updated_by = EXCLUDED.last_updated_by" \
                         % dbhelper.quotingTableName(('uc_%s' % self.store_tablename), self.is_postgre_sql)

            sql_statement = statement1 + " values (%s, %s, %s, current_timestamp, %s) " + statement3

            try:
                dbhelper.execute_postgre_sql_query(self.db_connection, sql_statement,
                                                   (self.entity_type_id, model_name, psycopg2.Binary(model), user_name))
            except Exception as ex:
                raise Exception('Storing model %s failed with sql statement "%s"'
                                % (model_name, sql_statement)) from ex

        logger.info('Model %s of size %d bytes has been stored in table %s.%s.'
                    % (model_name, len(model) if model is not None else 0, self.quoted_schema,
                       self.quoted_store_tablename))

    def retrieve_model(self, model_name, deserialize=True):

        if not self.is_postgre_sql:
            sql_statement = "SELECT MODEL FROM %s.%s WHERE ENTITY_TYPE_ID = ? AND MODEL_NAME = ?" \
                            % (self.quoted_schema, self.quoted_store_tablename)

            stmt = ibm_db.prepare(self.db_connection, sql_statement)

            try:
                ibm_db.bind_param(stmt, 1, self.entity_type_id)
                ibm_db.bind_param(stmt, 2, model_name)
                ibm_db.execute(stmt)
                row = ibm_db.fetch_tuple(stmt)
                if row is False:
                    model = None
                else:
                    model = row[0]
            except Exception as ex:
                raise Exception('Retrieval of model %s failed with sql statement "%s"'
                                % (model_name, sql_statement)) from ex
            finally:
                ibm_db.free_result(stmt)
        else:
            sql_statement = 'SELECT model FROM %s.%s' % (self.quoted_schema, self.quoted_store_tablename)
            sql_statement += ' WHERE entity_type_id = %s AND model_name = %s'

            try:
                row = dbhelper.execute_postgre_sql_select_query(self.db_connection, sql_statement,
                                                                (self.entity_type_id, model_name),
                                                                fetch_one_only=True)
                if row is None:
                    model = None
                else:
                    model = bytes(row[0])
            except Exception as ex:
                raise Exception('Retrieval of model %s failed with sql statement "%s"'
                                % (model_name, sql_statement)) from ex

        if model is not None:
            logger.info('Model %s of size %d bytes has been retrieved from table %s.%s'
                        % (model_name, len(model) if model is not None else 0, self.quoted_schema,
                           self.quoted_store_tablename))
        else:
            logger.info('Model %s does not exist in table %s.%s' % (model_name, self.quoted_schema,
                                                                    self.quoted_store_tablename))

        if model is not None and deserialize:
            try:
                model = pickle.loads(model)
            except Exception as ex:
                raise Exception('Deserialization of model %s that has been retrieved from ModelStore failed.'
                                % model_name) from ex

        return model

    def delete_model(self, model_name):
        if not self.is_postgre_sql:
            sql_statement = "DELETE FROM %s.%s where ENTITY_TYPE_ID = ? and MODEL_NAME = ?" \
                            % (self.quoted_schema, self.quoted_store_tablename)

            try:
                stmt = ibm_db.prepare(self.db_connection, sql_statement)

                try:
                    ibm_db.bind_param(stmt, 1, self.entity_type_id)
                    ibm_db.bind_param(stmt, 2, model_name)
                    ibm_db.execute(stmt)
                finally:
                    ibm_db.free_result(stmt)
            except Exception as ex:
                raise Exception('Deletion of model %s failed with sql statement "%s"'
                                % (model_name, sql_statement)) from ex
        else:
            sql_statement = "DELETE FROM %s.%s" % (self.quoted_schema, self.quoted_store_tablename)
            sql_statement += ' where entity_type_id = %s and model_name = %s'

            try:
                dbhelper.execute_postgre_sql_query(self.db_connection, sql_statement, (self.entity_type_id, model_name))
            except Exception as ex:
                raise Exception('Deletion of model %s failed with sql statement "%s"'
                                % (model_name, sql_statement)) from ex

        logger.info('Model %s has been deleted from table %s.%s' % (model_name, self.quoted_schema,
                                                                    self.quoted_store_tablename))
