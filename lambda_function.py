import sys
import logging
import csv
import pymysql
import boto3
import rds_config

s3 = boto3.client('s3')
rds_host  = rds_config.db_endpoint
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def read_data(event):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    file = event["Records"][0]["s3"]["object"]["key"]
    resp = s3.get_object(Bucket=bucket, Key=file)
    data = resp['Body'].read().decode('utf-8').split("\n")
    return data

def lambda_handler(event, context):
    # Verify connection to DB is possible
    try:
        connection= pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit()
    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
    
    data = read_data(event)
    rows = csv.reader(data)
    headers = next(rows)
    headers[0] = 'listing_id'
    headers[1] = 'listing_name'
    
    query_placeholders = ', '.join(['%s'] * len(headers))
    query_columns = ', '.join(headers)
    insert_query = ''' INSERT INTO nl_listing (%s) VALUES (%s) ''' %(query_columns, query_placeholders)

    with connection.cursor() as cursor:
        for row in rows:
            if row:
                #print(row)
                cursor.execute(insert_query, row)
        connection.commit()
        cursor.close()    
    connection.close()