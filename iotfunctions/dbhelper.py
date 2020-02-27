import logging
import ibm_db
import psycopg2.extras

logger = logging.getLogger(__name__)

# PostgreSQL Queries
POSTGRE_SQL_INFORMATION_SCHEMA = " SELECT table_schema,table_name , column_name ,udt_name, character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ; "
POSTGRE_SQL_TO_DB2_DATA_TYPE = {"int8": "BIGINT", "bpchar": "CHAR"}


def quotingColumnName(schemaName, is_postgre_sql=False):
    return quotingTableName(schemaName, is_postgre_sql)


def quotingSchemaName(schemaName, is_postgre_sql=False):
    return quotingTableName(schemaName, is_postgre_sql)


def quotingTableName(tableName, is_postgre_sql=False):
    quotedTableName = 'NULL'
    quote = '\"'
    twoQuotes = '\"\"'

    if tableName is not None:
        tableName = tableName if is_postgre_sql else tableName.upper()
        # Quote string and escape all quotes in string by an additional quote
        quotedTableName = quote + tableName.replace(quote, twoQuotes) + quote

    return quotedTableName


def quotingSqlString(sqlValue):
    preparedValue = 'NULL'
    quote = '\''
    twoQuotes = '\'\''

    if sqlValue is not None:
        if isinstance(sqlValue, str):
            # Quote string and escape all quotes in string by an additional quote
            preparedValue = quote + sqlValue.replace(quote, twoQuotes) + quote
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
        columnList = list()
        dblayout = dict()
        results = get_postgre_sql_information_schema(db_connection, schema_name, table_name)
        if len(results) != 0:
            for row in results:
                colName = row[2].upper()  # COLUMN_NAME
                colType = POSTGRE_SQL_TO_DB2_DATA_TYPE.get(row[3], row[3].upper())  # COLUMN_TYPE
                colSize = row[4]  # COLUMN_LENGTH
                dblayout[colName] = (colType, colSize)
                columnList.append('(\'%s\', \'%s\', \'%s\')' % (colName, colType, colSize))
        return (dblayout, columnList)


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
