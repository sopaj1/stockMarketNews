import boto3
import json
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key

# Set up DynamoDB
dynamodb = boto3.resource('dynamodb')
table_name = os.environ.get('DYNAMO_TABLE')  # Fetch table name from environment variables
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    try:
        # Determine where the symbol is coming from
        if 'body' in event and isinstance(event['body'], str):
            # API Gateway request with stringified JSON
            try:
                body = json.loads(event['body'])
            except json.JSONDecodeError:
                return {'statusCode': 400, 'body': json.dumps({'error': 'Invalid JSON in body'})}
        else:
            # Direct invocation
            body = event

        # Extract symbol
        symbol = body.get('symbol')
        print(symbol)
        if not symbol:
            return {'statusCode': 400, 'body': json.dumps({'error': 'Missing "symbol"'})}

        # Format today's date (UTC)
        today = datetime.utcnow().strftime('%Y-%m-%d')

        # Query DynamoDB for the given symbol and today's date
        response = table.query(
            KeyConditionExpression=Key('symbol').eq(symbol.upper()) & Key('date').eq(today)
        )

        items = response.get('Items', [])
        if not items:
            return {
                'statusCode': 404,
                'body': json.dumps({'message': f'No data found for symbol "{symbol}" on {today}'})
            }

        # Return the first item found (if multiple results exist, you can change this)
        return {
            'statusCode': 200,
            'body': json.dumps(items[0])
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
