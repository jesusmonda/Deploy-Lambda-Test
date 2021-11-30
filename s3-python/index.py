import boto3
from psycopg2 import connect
import os
s3_client = boto3.client('s3')

master = connect(
    host=os.environ['PG_MASTER_HOST'],
    database=os.environ['PG_MASTER_DATABASE'],
    user=os.environ['PG_MASTER_USER'],
    password=os.environ['PG_MASTER_PASSWORD'],
    port=os.environ['PG_MASTER_PORT'],
    sslmode='require'
)
cursor_master = master.cursor()


# Read CSV file content from S3 bucket
def read_data_from_s3(event):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_name = event["Records"][0]["s3"]["object"]["key"]
    
    resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)

    data = resp['Body'].read().decode('utf-8')
    data = data.split("\n")

    return data


def create_s3_connection(event):
    data = read_data_from_s3(event)
    
    data = list(filter(None, data))
    for index, emp in enumerate(data, start=0): # Iterate over S3 csv file content and format to insert into PostgreSQL
        try:
            if index == 0 : 
                data[index] = "(" + ", ".join(emp.replace("\r", "").split(";")) + ")"
            else :                 
                data[index] = "(\'" + "\', \'".join((emp.replace("\r", "")).replace("\'", "\'\'").split(";")) + "\')"

        except (Exception) as error:
            raise ValueError({'status': 400, 'message': 'Exception on S3 connection: %s' % error})
        except :
            print('Error parsing S3 data, continue')
            continue

    columns = data[0]
    del data[0]
    values = ", ".join(data)

    return columns, values


def executeQuery(columns, values, tableName):
    try:
        global cursor_master
        if cursor_master == None or cursor_master.closed :
            print("No cursor to query database. Create cursor. ")
            cursor_master = master.cursor()
        cursor_master.execute("TRUNCATE TABLE " + tableName + " ;")
        print('Table %s succesfully truncated. ' % tableName)
        cursor_master.execute("INSERT INTO " + tableName + " " + columns + " VALUES "+ values + " ;")
        master.commit()
        print('Data inserted on %s successfully. ' % tableName)

    except (Exception) as error:
        print('INSERT DATA FAIL: %s ' % error)
        master.rollback()
        print('DB Rollback')
        raise ValueError({'status': 400, 'message': 'There has been an error updating table: %s ' % error})


def checkAffectedTable(objectKey):
    try: 
        splittedString = objectKey.split("/")[1]

        switcher = {
            'Country': "public.t_kcog_country", 
            'Entity': "public.t_kcog_entity", 
            'Currency': "public.t_kcog_currency"
        }

        return switcher.get(splittedString, False)
    except: 
        raise ValueError({'status': 400, 'message': 'Error retrieving target folder from path %s to operate. ' % objectKey})

def handler(event=None, context=None):
    print(event)
    try:
        tableName = checkAffectedTable(event["Records"][0]["s3"]["object"]["key"])
    
        if tableName :
            columns, data = create_s3_connection(event)
            print("Data retrieved from S3")
            
            executeQuery(columns, data, tableName)
        else : 
            raise ValueError({'status': 400, 'message': 'Error. Unhandled S3 path.'})

    except (ValueError) as error:
        error = error.args[0]
        print(error)
        return {
            'statusCode': error['status'] if 'status' in error else 500,
            'body': error['message'] if 'message' in error else error
        }