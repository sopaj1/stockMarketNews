import json
import os
import urllib.request
import urllib.error
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

# Set up DynamoDB table
dynamodb = boto3.resource('dynamodb')
table_name = os.environ.get("DDB_TABLE_NAME")
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    # Process each record in the SQS event
    for record in event['Records']:
        try:
            # Extract symbol from the message body
            message_body = json.loads(record['body'])
            symbol = message_body.get("symbol")
            
            if not symbol:
                print(f"Error: Symbol is missing in message {record['messageId']}")
                continue

            # Get API key from environment variable
            api_key = os.environ.get("API_KEY")
            if not api_key:
                print("Error: API key is not configured")
                continue
            
            # Alpha Vantage API endpoint for News Sentiment
            url = f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={symbol}&apikey={api_key}"

            try:
                # Fetch the sentiment data from Alpha Vantage
                with urllib.request.urlopen(url) as response:
                    response_body = response.read()
                    data = json.loads(response_body.decode('utf-8'))

                feed = data.get("feed", [])
                if not feed:
                    sentiment_score = 50
                    bullish_url = ""
                    bearish_url = ""
                else:
                    sentiment_scores = []
                    bullish_article = {"url": "", "sentiment": -1}
                    bearish_article = {"url": "", "sentiment": 1}
                    now = datetime.utcnow()

                    # Process articles from the past 24 hours only
                    for article in feed:
                        time_published_str = article.get("time_published")
                        if not time_published_str:
                            continue
                        try:
                            time_published = datetime.strptime(time_published_str, "%Y%m%dT%H%M%S")
                        except ValueError:
                            continue
                        
                        # Filter to articles from the past day
                        if now - time_published < timedelta(days=1):
                            sentiment = float(article.get("overall_sentiment_score", 0))
                            sentiment_scores.append(sentiment)
                            article_url = article.get("url", "")
                            if sentiment > bullish_article["sentiment"] and article_url:
                                bullish_article = {"url": article_url, "sentiment": sentiment}
                            if sentiment < bearish_article["sentiment"] and article_url:
                                bearish_article = {"url": article_url, "sentiment": sentiment}

                    # Calculate sentiment score or set default
                    if sentiment_scores:
                        avg_sentiment = sum(sentiment_scores) / len(sentiment_scores)
                        sentiment_score = round((avg_sentiment + 1) / 2 * 100)
                        bullish_url = bullish_article["url"]
                        bearish_url = bearish_article["url"]
                    else:
                        sentiment_score = 50
                        bullish_url = ""
                        bearish_url = ""

            except urllib.error.URLError as e:
                print(f"Error: Failed to fetch sentiment data for symbol {symbol}: {e.reason}")
                sentiment_score = 50
                bullish_url = ""
                bearish_url = ""

            # Store the sentiment data in DynamoDB
            try:
                table.put_item(Item={
                    "symbol": "sentiment_" + symbol,
                    "type": "sentiment",
                    "sentiment_score": str(sentiment_score),
                    "bullish_article": bullish_url,
                    "bearish_article": bearish_url
                })
                print(f"Successfully stored sentiment data for symbol {symbol} in DynamoDB")

            except ClientError as e:
                print(f"Error: Failed to write to DynamoDB for symbol {symbol}: {str(e)}")
                continue

            print(f"Processed symbol {symbol} with sentiment score {sentiment_score}")

        except KeyError as e:
            print(f"Error: Invalid message format for SQS record {record['messageId']}: {str(e)}")
            continue

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Processing complete"})
    }