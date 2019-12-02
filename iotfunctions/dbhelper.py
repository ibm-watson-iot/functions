import logging

logger = logging.getLogger(__name__)


def quoting_column_name(column_name):
    return quoting_table_name(column_name)


def quoting_schema_name(schema_name):
    return quoting_table_name(schema_name)


def quoting_table_name(table_name):
    quoted_table_name = 'NULL'
    quote = '\"'
    two_quotes = '\"\"'

    if table_name is not None:
        table_name = table_name.upper()
        # Quote string and escape all quotes in string by an additional quote 
        quoted_table_name = quote + table_name.replace(quote, two_quotes) + quote

    return quoted_table_name


def quoting_sql_string(sql_value):
    prepared_value = 'NULL'
    quote = '\''
    two_quotes = '\'\''

    if sql_value is not None:
        if isinstance(sql_value, str):
            # Quote string and escape all quotes in string by an additional quote 
            prepared_value = quote + sql_value.replace(quote, two_quotes) + quote
        else:
            # sql_value is no string; therefore just return it as is
            prepared_value = sql_value

    return prepared_value
