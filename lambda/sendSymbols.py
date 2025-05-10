import os
import boto3
import json

dynamodb = boto3.client('dynamodb')
sqs = boto3.client('sqs')

TABLE_NAME = os.environ['SYMBOL_TABLE']
QUEUE_URL = os.environ['SYMBOL_QUEUE_URL']
QUEUE_URL2 = os.environ['SYMBOL_QUEUE_URL2']

# 
def lambda_handler(event, context):
    last_key = None
    while True:
        response = dynamodb.scan(
            TableName=TABLE_NAME,
            ExclusiveStartKey=last_key
        ) if last_key else dynamodb.scan(TableName=TABLE_NAME)

        for item in response.get('Items', []):
            symbol = item.get('symbol', {}).get('S')
            if symbol:
                sqs.send_message(
                    QueueUrl=QUEUE_URL,
                    MessageBody=json.dumps({'symbol': symbol})
                )
                sqs.send_message(
                    QueueUrl=QUEUE_URL2,
                    MessageBody=json.dumps({'symbol': symbol})
                )

        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break

    return {
        'statusCode': 200,
        'body': 'All symbols sent to SQS.'
    }
