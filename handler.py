import boto3
s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    response = s3.get_object(Bucket=bucket, Key=file_name)
    data = response['Body'].read().decode('utf-8')
    print(data)