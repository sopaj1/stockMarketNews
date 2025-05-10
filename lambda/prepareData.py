import boto3
import os
import datetime
import json

# Initialize resources
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')
staging_table = dynamodb.Table(os.environ['STAGING_TABLE_NAME'])
final_table = dynamodb.Table(os.environ['FINAL_TABLE_NAME'])

# Get the SQS Queue URL from environment variable
queue_url = os.environ['SQS_QUEUE_URL']

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")

    if 'Records' not in event:
        return {'statusCode': 400, 'body': json.dumps({'error': "Missing 'Records'"})}

    if not event['Records']:
        return {'statusCode': 200, 'body': json.dumps({'message': 'No records to process'})}

    processed_symbols = set()

    for record in event['Records']:
        if record.get('eventName') not in ['INSERT', 'MODIFY']:
            continue

        new_image = record.get('dynamodb', {}).get('NewImage', {})
        symbol_key = new_image.get('symbol', {}).get('S')

        if not symbol_key:
            continue

        if symbol_key.startswith('price_'):
            base_symbol = symbol_key.replace('price_', '')
        elif symbol_key.startswith('sentiment_'):
            base_symbol = symbol_key.replace('sentiment_', '')
        else:
            continue

        if base_symbol in processed_symbols:
            continue

        current_date = datetime.datetime.now().strftime('%Y-%m-%d')

        try:
            # Full primary key includes both partition key (symbol) and sort key (type)
            price_key = {
                'symbol': f'price_{base_symbol}',
                'type': 'price'
            }
            sentiment_key = {
                'symbol': f'sentiment_{base_symbol}',
                'type': 'sentiment'
            }

            print(f"Fetching price with key: {price_key}")
            print(f"Fetching sentiment with key: {sentiment_key}")

            # Fetch price and sentiment from staging table
            price_response = staging_table.get_item(Key=price_key)
            sentiment_response = staging_table.get_item(Key=sentiment_key)

            if 'Item' not in price_response or 'Item' not in sentiment_response:
                print(f"Missing data for {base_symbol}")
                continue

            price_item = price_response['Item']
            sentiment_item = sentiment_response['Item']

            combined_item = {
                'symbol': base_symbol,
                'price': price_item.get('price'),
                'sentiment_score': sentiment_item.get('sentiment_score'),
                'bullish_article': sentiment_item.get('bullish_article'),
                'bearish_article': sentiment_item.get('bearish_article'),
                'type': 'final',
                'date': current_date
            }

            # Insert combined data into final table
            final_table.put_item(Item=combined_item)
            print(f"Inserted combined data for {base_symbol} into final table")

            # Delete staging data (price and sentiment)
            staging_table.delete_item(Key=price_key)
            staging_table.delete_item(Key=sentiment_key)
            print(f"Deleted staging data for {base_symbol}")

            # Mark the symbol as processed
            processed_symbols.add(base_symbol)

            # After processing the data, delete the SQS message
            receipt_handle = record['receiptHandle']
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
            print(f"Deleted SQS message for {base_symbol}")

        except Exception as e:
            print(f"Error processing {base_symbol}: {str(e)}")

    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Processed {len(processed_symbols)} symbols',
            'processed': list(processed_symbols)
        })
    }
