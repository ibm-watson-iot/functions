# *****************************************************************************
# © Copyright IBM Corp. 2020, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import logging

import pandas as pd

from iotfunctions import dbhelper, util


class LoaderPipeline:

    def __init__(self, stages=[], dblogging=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))
        self.stages = stages.copy()
        self.dblogging = dblogging

    def execute(self, df, start_ts, end_ts, entities):
        if df is not None:
            util.log_data_frame(f"df before loaders: shape", df)
        else:
            self.logger.info("df is None before loaders")
        for s in self.stages:
            try:
                name = s.name
            except AttributeError:
                name = s.__class__.__name__

            start_time = pd.Timestamp.utcnow()
            self.logger.debug('Start of stage {{ %s }}' % name)

            if self.dblogging is not None:
                self.dblogging.update_stage_info(name)

            df = s.execute(df, start_ts, end_ts, entities)

            self.logger.debug('End of stage {{ %s }}, execution time = %s s' % (
                name, (pd.Timestamp.utcnow() - start_time).total_seconds()))

        util.log_data_frame(f"df after loaders: shape", df)

        return df


def _generate_metadata(cls, metadata):
    common_metadata = {'name': cls.__name__, 'moduleAndTargetName': '%s.%s' % (cls.__module__, cls.__name__),
                       'category': 'TRANSFORMER'}
    common_metadata.update(metadata)
    return common_metadata


class BaseLoader:

    def _set_dms(self, dms):
        self.dms = dms

    def _get_dms(self):
        return self.dms


class LoadTableAndConcat(BaseLoader):

    @classmethod
    def metadata(cls):
        return _generate_metadata(cls, {'description': 'Create a new data item by expression.', 'input': [
            {'name': 'table', 'description': 'Name of the table to load.', 'type': 'CONSTANT', 'required': True,
             'dataType': 'LITERAL'},
            {'name': 'columns', 'description': 'Names of table columns to load (comma separated).', 'type': 'CONSTANT',
             'required': True, 'dataType': 'ARRAY', 'dataTypeForArray': ["LITERAL"],
             'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "title": "columns", "type": "array",
                            "minItems": 1, "items": {"type": "string"}}},
            {'name': 'where_clause', 'description': 'The where clause, excluding the time range filtering.',
             'type': 'CONSTANT', 'required': False, 'dataType': 'LITERAL'},
            {'name': 'parse_dates', 'description': 'Names of table columns to parse as dates (comma separated).',
             'type': 'CONSTANT', 'required': False, 'dataType': 'ARRAY', 'dataTypeForArray': ["LITERAL"],
             'jsonSchema': {"$schema": "http://json-schema.org/draft-07/schema#", "title": "parse_dates",
                            "type": "array", "minItems": 1, "items": {"type": "string"}}},
            {'name': 'id_col', 'description': 'Name of the column containing the entity ID.', 'type': 'CONSTANT',
             'required': True, 'dataType': 'LITERAL'},
            {'name': 'timestamp_col', 'description': 'Name of the column containing the time series base timestamp.',
             'type': 'CONSTANT', 'required': True, 'dataType': 'LITERAL'}, ], 'output': [
            {'name': 'names', 'dataTypeFrom': 'columns', 'cardinalityFrom': 'columns',
             'description': 'New data item names to create (data items are comma separated).'}], 'tags': ['JUPYTER']})

    def __init__(self, table=None, columns=None, names=None, where_clause=None, parse_dates=None, id_col=None,
                 timestamp_col=None):
        self.logger = logging.getLogger('%s.%s' % (self.__module__, self.__class__.__name__))

        if table is None:
            raise RuntimeError("argument table must be provided")

        if columns is not None and isinstance(columns, str):
            columns = [n.strip() for n in columns.split(',') if len(n.strip()) > 0]
        if columns is None or not isinstance(columns, list):
            raise RuntimeError("argument columns must be provided and must be a comma separated string")

        if names is None:
            names = columns
        if names is not None and isinstance(names, str):
            names = [n.strip() for n in names.split(',') if len(n.strip()) > 0]
        if not isinstance(names, list):
            raise RuntimeError("argument names must be provided and must be a comma separated string")

        if len(columns) != len(names):
            raise RuntimeError("the length of arguments columns and names must be the same")

        if parse_dates is None:
            parse_dates = []
        if parse_dates is not None and isinstance(parse_dates, str):
            parse_dates = [n.strip() for n in parse_dates.split(',') if len(n.strip()) > 0]
        if parse_dates is not None and not isinstance(parse_dates, list):
            raise RuntimeError("argument parse_dates must be a comma separated string")
        parse_dates = set(parse_dates)

        if id_col is not None and isinstance(id_col, str):
            id_col = [n.strip() for n in id_col.split(',') if len(n.strip()) > 0]
        if id_col is None or not isinstance(id_col, list):
            raise RuntimeError("argument id_col must be a comma separated string")

        # comma separated string a temporary workaround befoe pipeline can distinguish constant from data item
        if timestamp_col is not None and isinstance(timestamp_col, str):
            timestamp_col = [n.strip() for n in timestamp_col.split(',') if len(n.strip()) > 0]
        if timestamp_col is None or not isinstance(timestamp_col, list):
            raise RuntimeError("argument timestamp_col must be string")

        self.table = table
        self.columns = columns
        self.names = names
        self.where_clause = where_clause
        self.parse_dates = parse_dates
        self.id_col = id_col[0]
        self.timestamp_col = timestamp_col[0]

    def execute(self, df, start_ts, end_ts, entities=None):
        key_id = 'key_id_'
        key_timestamp = 'key_timestamp_'

        sql = 'SELECT %s, %s AS "%s", %s AS "%s" FROM %s' % (
            ', '.join([dbhelper.quotingColumnName(col, self.dms.is_postgre_sql) for col in self.columns]),
            dbhelper.quotingColumnName(self.id_col, self.dms.is_postgre_sql), key_id,
            dbhelper.quotingColumnName(self.timestamp_col, self.dms.is_postgre_sql), key_timestamp,
            dbhelper.quotingTableName(self.table, self.dms.is_postgre_sql))
        condition_applied = False
        if self.where_clause is not None:
            sql += ' WHERE %s' % self.where_clause
            condition_applied = True
        if start_ts is not None and end_ts is not None:  # TODO start_ts and end_ts are expected to be not None
            if not condition_applied:
                sql += ' WHERE '
            else:
                sql += ' AND '
            sql += "%s < %s AND %s <= %s" % (dbhelper.quotingSqlString(str(start_ts)),
                                             dbhelper.quotingColumnName(self.timestamp_col, self.dms.is_postgre_sql),
                                             dbhelper.quotingColumnName(self.timestamp_col, self.dms.is_postgre_sql),
                                             dbhelper.quotingSqlString(str(end_ts)))
            condition_applied = True
        if entities is not None:
            if not condition_applied:
                sql += ' WHERE '
            else:
                sql += ' AND '
            sql += "%s IN (%s)" % (dbhelper.quotingColumnName(self.id_col, self.dms.is_postgre_sql),
                                   ', '.join([dbhelper.quotingSqlString(ent) for ent in entities]))

        self.parse_dates.add(key_timestamp)
        requested_col_names = self.names + [key_id, key_timestamp]
        df_sql = self._get_dms().db.read_sql_query(sql, parse_dates=self.parse_dates,
                                                   requested_col_names=requested_col_names)
        df_sql = df_sql.astype(dtype={key_id: str}, errors='ignore')

        self.logger.debug('loaded_df=\n%s' % df_sql.head())

        # reset and rename event df index to the same special column names
        original_event_index_names = df.index.names
        df = df.rename_axis([key_id, key_timestamp])
        df = df.reset_index()

        # concat ignoring index (simple concatenation) then set index back renamed to the original one
        df = pd.concat([df, df_sql], ignore_index=True, sort=False)
        df = df.set_index(keys=[key_id, key_timestamp])
        df = df.rename_axis(original_event_index_names)

        self.logger.debug('concatenated_df=\n%s' % df.head())

        return df
