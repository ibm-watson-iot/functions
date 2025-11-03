# *****************************************************************************
# © Copyright IBM Corp. 2018, 2024  All Rights Reserved.
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
import re

logger = logging.getLogger(__name__)
SQL_PATTERN = re.compile(r'\w*')
SQL_PATTERN_EXTENDED = re.compile(r'[\w\-.]*')
SQL_PATTERN_EXTENDED2 = re.compile(r'^[\w\-.][\w\-.\;\,\/\s?]*')



def quotingColumnName(schemaName):
    return quotingTableName(schemaName)


def quotingSchemaName(schemaName):
    return quotingTableName(schemaName)


def quotingTableName(tableName):
    quotedTableName = 'NULL'

    if tableName is not None:
        # Quote string and escape all quotes in string by an additional quote
        quotedTableName = f'"{tableName.upper()}"'

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

def execute_sql_query(db_connection, db_type, sql, params=None):
    ibm_db.exec_immediate(db_connection, sql)

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