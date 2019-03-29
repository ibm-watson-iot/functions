import logging

logger = logging.getLogger(__name__)


def quotingColumnName(schemaName):
    return quotingTableName(schemaName)
    
def quotingSchemaName(schemaName):
    return quotingTableName(schemaName)
    
def quotingTableName(tableName):
    
    quotedTableName = 'NULL'
    quote = '\"'
    twoQuotes='\"\"'

    if tableName is not None:
        tableName = tableName.upper()
        # Quote string and escape all quotes in string by an additional quote 
        quotedTableName = quote + tableName.replace(quote,twoQuotes) + quote
        
    return quotedTableName

def quotingSqlString(sqlValue):
    
    preparedValue = 'NULL'
    quote = '\''
    twoQuotes='\'\''

    if sqlValue is not None:
        if isinstance(sqlValue, str):
            # Quote string and escape all quotes in string by an additional quote 
            preparedValue = quote + sqlValue.replace(quote,twoQuotes) + quote
        else:
            # sqlValue is no string; therefore just return it as is
            preparedValue = sqlValue
            
    return preparedValue