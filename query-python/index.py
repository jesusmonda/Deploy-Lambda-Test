import json
from psycopg2 import connect
import os

replica = connect(
    host=os.environ['PG_REPLICA_HOST'],
    database=os.environ['PG_REPLICA_DATABASE'],
    user=os.environ['PG_REPLICA_USER'],
    password=os.environ['PG_REPLICA_PASSWORD'],
    port=os.environ['PG_REPLICA_PORT'],
    sslmode='require'
)
cursor_replica = replica.cursor()


def dynamicQuery(cur, queryArray, queryValues, tableName): 
    #Dynamically creates query for the selected table based on input filter elements, and fetch data for response. 
    #The full query is built (instead of using SELECT *) to parse date data types (gf_audit_date)

    switcher = {
        'entity' : 'SELECT \
            g_entity_id, g_holding_entity_id, g_country_id, \
            gf_entity_id, g_country_data_id, g_language_data_id, gf_entity_name, gf_entity_desc, \
            g_institution_type, g_bbva_owned_security_co_id, gf_user_audit_id, to_char(gf_audit_date, \'YYYY-MM-DD HH24:MI:SS.MS\') FROM public.t_kcog_entity ', 
        'country' : 'SELECT \
            g_country_id, g_country_large_id, g_country_numeric_id, \
            g_country_data_id, g_language_data_id, g_country_name, g_country_short_name, g_currency_id, \
            g_prefix_phone_id, gf_user_audit_id, to_char(gf_audit_date, \'YYYY-MM-DD HH24:MI:SS.MS\') FROM public.t_kcog_country ', 
        'currency' : 'SELECT \
            g_currency_id, g_currency_numeric_id, g_country_data_id, g_language_data_id, g_currency_name, \
            g_currency_abbreviated_name, g_currency_plural_name, gf_currency_decimal_number, \
            gf_user_audit_id, to_char(gf_audit_date, \'YYYY-MM-DD HH24:MI:SS.MS\') FROM public.t_kcog_currency '
    }

    query = switcher.get(tableName, False) 

    if not query :
        raise ValueError({'status': 404, 'message': 'No supported table selected.'}) 

    if (len(queryArray) > 0) and (len(queryArray) == len(queryValues)): 
        query += ' WHERE '
        queryDataToJoin = []
        for x in range(len(queryArray)):
            if type(queryValues[x]) == list:
                queryDataToJoin.append(' ' + queryArray[x] + ' IN %s ')
                queryValues[x] = tuple(queryValues[x])

            else:
                queryDataToJoin.append(' ' + queryArray[x] + ' = %s ')
                queryValues[x] = str(queryValues[x])

        query += ' AND '.join(queryDataToJoin) #Join the elements on the array (Ex. [column = %s, column IN %s, ... ]) with all the other parameter-ready elements. 

    query += ';'

    if cur == None or cur.closed: 
        print("No cursor to query database. Create cursor. ")
        cur = replica.cursor()

    cur.execute(query, queryValues)
    
    columns = []

    for column in cur.description:
        columns.append(column[0])

    values = cur.fetchall() 

    #Check with Bank's API Gateway team the desired way to return data. 

    return {
        'columns' : columns, 
        'values' : values
    }


def filterEventMap(jsonData, tableName): #From event filter input, filters those accepted as columns to query on the selected table. 
    try:
        queryArray = []
        queryValues = []
        with open('dictQuery.json') as dictQuery:
            dictQuery = json.load(dictQuery)
            dictQuery = dictQuery[tableName]

        for key in jsonData:
            try : 
                if dictQuery[key]:
                    queryArray.append(dictQuery[key])
                    queryValues.append(jsonData[key])
            except : 
                print('Element to filter %s not available for table %s. Skipping' % (key, tableName))

        return queryArray, queryValues
    except:
        raise ValueError({'status': 404, 'message': "Not found table"})


def handler(event=None, context=None):
    try:
        print('Received event %s' % event)
        
        if 'table' not in event :
            raise ValueError({'status': 400, 'message': "Invalid parameters"})
            
        tableName = event['table']

        if 'filter' not in event :
            queryData = []
        else : queryData = event['filter']
    
        queryArray, queryValues = filterEventMap(queryData, tableName)
        result = dynamicQuery(cursor_replica, queryArray, queryValues, tableName)
        
        return {
            'statusCode': 200,
            'body': {
                "rowsNumber" : len(result['values']),
                "columns" : result['columns'], 
                "values" : result['values']
            }
        }

    except ValueError as error:
        error = error.args[0]
        print(error)
        return {
            'statusCode': error['status'] if 'status' in error else 500,
            'body': error['message'] if 'message' in error else error
        }