# *****************************************************************************
# © Copyright IBM Corp. 2018, 2022  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
import datetime
import logging

import ibm_db
import pandas as pd
import psycopg2.extras
import re

logger = logging.getLogger(__name__)
SQL_PATTERN = re.compile('\w*')
SQL_PATTERN_EXTENDED = re.compile('[\w-]*')
SQL_PATTERN_EXTENDED2 = re.compile('^[\w\-.][\w\-.\;\,\/\s?]*')

# PostgreSQL Queries
POSTGRE_SQL_INFORMATION_SCHEMA = " SELECT table_schema,table_name , column_name ,udt_name, character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ; "
POSTGRE_SQL_TO_DB2_DATA_TYPE = {"int8": "BIGINT", "bpchar": "CHAR"}


def quotingColumnName(schemaName, is_postgre_sql=False):
    return quotingTableName(schemaName, is_postgre_sql)


def quotingSchemaName(schemaName, is_postgre_sql=False):
    return quotingTableName(schemaName, is_postgre_sql)


def quotingTableName(tableName, is_postgre_sql=False):
    quotedTableName = 'NULL'

    if tableName is not None:
        # Quote string and escape all quotes in string by an additional quote
        quotedTableName = f'"{tableName if is_postgre_sql else tableName.upper()}"'

    return quotedTableName


def quotingSqlString(sqlValue):
    preparedValue = 'NULL'

    if sqlValue is not None:
        if isinstance(sqlValue, str):
            # Quote string and escape all quotes in string by an additional quote
            preparedValue = f"'{sqlValue}'"
        else:
            # sqlValue is no string; therefore just return it as is
            preparedValue = sqlValue

    return preparedValue


def check_table_exist(db_connection, db_type, schema_name, unqualified_table_name):
    if db_type == 'postgresql':
        result = get_postgre_sql_information_schema(db_connection, schema_name, unqualified_table_name)
        return len(result) > 0


def get_table_layout(db_connection, db_type, schema_name, table_name):
    if db_type == 'postgresql':
        column_list = list()
        db_layout = dict()
        results = get_postgre_sql_information_schema(db_connection, schema_name, table_name)
        if len(results) != 0:
            for row in results:
                col_name = row[2].upper()  # COLUMN_NAME
                col_type = POSTGRE_SQL_TO_DB2_DATA_TYPE.get(row[3], row[3].upper())  # COLUMN_TYPE
                col_size = row[4]  # COLUMN_LENGTH
                db_layout[col_name] = (col_type, col_size)
                column_list.append('(\'%s\', \'%s\', \'%s\')' % (col_name, col_type, col_size))
        return (db_layout, column_list)


def get_postgre_sql_information_schema(db_connection, schema_name, table_name):
    result = execute_postgre_sql_select_query(db_connection, POSTGRE_SQL_INFORMATION_SCHEMA, (schema_name, table_name))
    return result


def execute_batch(db_connection, sql, params_list=None, page_size=5000):
    cursor = None
    try:
        cursor = db_connection.cursor()
        if params_list is None:
            logger.warning('params_list should not be empty')
        else:
            psycopg2.extras.execute_batch(cursor, sql, params_list, page_size)
            db_connection.commit()
    except:
        db_connection.rollback()
        logger.warning(
            'Error while executing PostgreSQL execute_batch query. SQL: %s , params_list: %s ' % (sql, params_list))
        raise
    finally:
        if cursor is not None:
            cursor.close()


def execute_sql_query(db_connection, db_type, sql, params=None):
    if db_type == 'db2':
        ibm_db.exec_immediate(db_connection, sql)
    elif db_type == 'postgresql':
        execute_postgre_sql_select_query(db_connection, sql, params)


def execute_postgre_sql_select_query(db_connection, sql, params=None, fetch_one_only=False):
    cursor = None
    try:
        cursor = db_connection.cursor()
        cursor.execute(sql) if params is None else cursor.execute(sql, params)
        if fetch_one_only:
            result = cursor.fetchone()
        else:
            result = cursor.fetchall()
        return result
    except:
        logger.warning('Error while executing PostgreSQL query. SQL: %s , params: %s ' % (sql, params))
        raise
    finally:
        if cursor is not None:
            cursor.close()


def execute_postgre_sql_query(db_connection, sql, params=None, raise_error=True):
    cursor = None
    try:
        cursor = db_connection.cursor()
        cursor.execute(sql) if params is None else cursor.execute(sql, params)
        db_connection.commit()
    except:
        db_connection.rollback()
        if raise_error:
            raise
        logger.warning('Error while executing PostgreSQL query. SQL: %s , params: %s ' % (sql, params))
    finally:
        if cursor is not None:
            cursor.close()


def check_sql_injection(input_obj):
    input_type = type(input_obj)
    if input_type == str:
        if SQL_PATTERN.fullmatch(input_obj) is None:
            raise RuntimeError(f"The following string contains forbidden characters and cannot be inserted into a sql "
                               f"statement for security reason. Only letters including underscore are allowed: {input_obj}")
    elif input_type == int or input_type == float:
        pass
    elif input_type == pd.Timestamp or input_type == datetime.datetime:
        pass
    else:
        raise RuntimeError(f"The following object has an unexpected type {input_type} and cannot be inserted into a "
                           f"sql statement for security reason: {input_obj}")

    return input_obj


def check_sql_injection_extended(input_string):

    if type(input_string) == str:
        if SQL_PATTERN_EXTENDED.fullmatch(input_string) is None:
            raise RuntimeError(f"The string {input_string} contains forbidden characters and cannot be inserted "
                               f"into a sql statement for security reason. Only letters, underscore and hyphen are allowed.")
    else:
        raise RuntimeError(f"A string is expected but the object {input_string} has type {type(input_string)}. "
                           f"It cannot be inserted into a sql statement for security reason.")

    return input_string


def check_sql_injection_extended2(input_string):

    if type(input_string) == str:
        if SQL_PATTERN_EXTENDED2.fullmatch(input_string) is None:
            raise RuntimeError(f"The string {input_string} contains forbidden characters and cannot be inserted "
                               f"into a sql statement for security reason. Only letters, numbers, underscore, "
                               f"semi-colon, comma, dot, slash, hyphen, question mark and white spaces are allowed. "
                               f"The first character must be either a letter, a number, a hyphen, a dot or an underscore.")
    else:
        raise RuntimeError(f"A string is expected but the object {input_string} has type {type(input_string)}. "
                           f"It cannot be inserted into a sql statement for security reason.")

    return input_string