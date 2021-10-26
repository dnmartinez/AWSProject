import json
import sys
import logging
import csv
import pymysql
import boto3

s3 = boto3.client('s3')
rds_host  = "naomi-airbnb-db.chjxclsv9tv3.us-east-2.rds.amazonaws.com"
name = 'admin'
password = 'yokocat22'
db_name = 'listings'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name)
except pymysql.MySQLError as e:
    logger.error("ERROR: THE KILLER IS ESCAPING Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    file = event["Records"][0]["s3"]["object"]["key"]
    resp = s3.get_object(Bucket=bucket, Key=file)
    
    data = resp['Body'].read().decode('utf-8').split("\n")
    # print(">>>>>>>>>",data[0])
    # print(">>>>>>>>>",data[1])
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

