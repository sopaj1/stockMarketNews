import json
import os
import urllib.request
import urllib.error
import boto3
from botocore.exceptions import ClientError

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table_name = os.environ.get("DDB_TABLE_NAME")
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    # Process each record in the event (SQS messages)
    for record in event['Records']:
        # Extract symbol from the message body
        try:
            message_body = json.loads(record['body'])
            symbol = message_body.get("symbol")
            
            if not symbol:
                # If symbol is missing, skip this message and log the error
                print(f"Error: Symbol is required in message {record['messageId']}")
                continue

            # Get Alpha Vantage API key from environment variable
            api_key = os.environ.get("API_KEY")
            if not api_key:
                print(f"Error: API key is not configured")
                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": "API key not configured"})
                }

            # Alpha Vantage API endpoint for daily adjusted stock price
            url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}"

            # Fetch the stock price from Alpha Vantage API
            try:
                with urllib.request.urlopen(url) as response:
                    response_body = response.read()
                    data = json.loads(response_body.decode('utf-8'))

                # Extract price from response
                quote = data.get("Global Quote", {})
                price = quote.get("05. price")

                if not price:
                    print(f"Error: No price data found for symbol {symbol}")
                    continue

                # Store data in DynamoDB
                try:
                    table.put_item(Item={
                        'symbol': 'price_' + symbol,
                        'type': 'price',
                        'price': price
                    })
                    print(f"Successfully stored price for symbol {symbol} in DynamoDB")

                except ClientError as e:
                    print(f"Error: Failed to write to DynamoDB for symbol {symbol}: {str(e)}")
                    continue

                # Return formatted result (for logging purposes)
                print(f"Processed symbol {symbol} with price {price}")

            except urllib.error.URLError as e:
                print(f"Error: Failed to fetch price for symbol {symbol}: {e.reason}")
                continue

        except KeyError as e:
            print(f"Error: Invalid message format for SQS record {record['messageId']}: {str(e)}")
            continue

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Processing complete"})
    }
