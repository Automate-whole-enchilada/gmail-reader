import os.path
import json
import base64
import re
import time
import boto3
from botocore.exceptions import ClientError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Define scope and initialize DynamoDB client
SCOPES = ['https://mail.google.com/']
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TickerAlerts')

def lambda_handler(event, context):
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            print("Error: Could not find valid credentials or refresh token.")
            return {'statusCode': 500, 'body': json.dumps('Authentication error.')}
    
    try:
        service = build('gmail', 'v1', credentials=creds)

        # --- PHASE 1: Process emails from the last 24 hours ---
        print("PHASE 1: Processing recent emails (newer_than:1d)...")
        # Note: This query looks at ALL emails from the last day, not just unread ones.
        results = service.users().messages().list(userId='me', q='newer_than:1d').execute()
        recent_messages = results.get('messages', [])

        if not recent_messages:
            print("No recent messages found to process.")
        else:
            for message in recent_messages:
                # Use a try/except block for each message in case one fails to parse
                try:
                    msg = service.users().messages().get(userId='me', id=message['id']).execute()
                    headers = msg['payload']['headers']
                    subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
                    
                    trade_type = None
                    if '2+ATR_40+Delta_Weekly_Calls' in subject:
                        trade_type = 'CALL'
                    elif '2+ATR_40+Delta_Weekly_Puts' in subject:
                        trade_type = 'PUT'

                    if trade_type:
                        tickers = re.findall(r'\.[A-Z0-9\.]+', subject)
                        print(f"Found tickers {tickers} for trade type {trade_type}")
                        
                        # NEW: Calculate the expiration time (current time + 2 hours)
                        expiration_time = int(time.time()) + 7200 
                        
                        for ticker in tickers:
                            clean_ticker = ticker.lstrip('.')
                            try:
                                table.put_item(
                                   Item={
                                        'TickerSymbol': clean_ticker,
                                        'TradeType': trade_type,
                                        'AddedTimestamp': int(time.time()),
                                        'ExpirationTime': expiration_time # NEW: Add TTL attribute
                                    },
                                    ConditionExpression='attribute_not_exists(TickerSymbol)'
                                )
                                print(f"Successfully added {clean_ticker} to DynamoDB.")
                            except ClientError as e:
                                if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                                    print(f"Ticker {clean_ticker} already exists. Skipping.")
                                else:
                                    raise

                    # IMPORTANT: Delete the recent email after it has been processed
                    service.users().messages().delete(userId='me', id=message['id']).execute()
                    print(f"Deleted processed message {message['id']}.")
                except HttpError as e:
                    print(f"Could not process message {message.get('id', 'N/A')}: {e}")


        # --- PHASE 2: Find and permanently delete all emails older than 24 hours ---
        print("\nPHASE 2: Deleting old emails (older_than:1d)...")
        old_messages_results = service.users().messages().list(userId='me', q='older_than:1d', maxResults=500).execute()
        old_messages = old_messages_results.get('messages', [])
        
        if not old_messages:
            print("No old emails found to delete.")
        else:
            # Batch delete is more efficient than deleting one by one
            message_ids_to_delete = [msg['id'] for msg in old_messages]
            print(f"Found {len(message_ids_to_delete)} old emails. Preparing to batch delete.")
            
            # The Gmail API allows a maximum of 1000 IDs per batchDelete request
            # We will process in chunks of 100 to be safe
            for i in range(0, len(message_ids_to_delete), 100):
                batch = message_ids_to_delete[i:i + 100]
                service.users().messages().batchDelete(
                    userId='me',
                    body={'ids': batch}
                ).execute()
                print(f"Permanently deleted batch of {len(batch)} old emails.")

        return {'statusCode': 200, 'body': json.dumps('Processing complete.')}

    except HttpError as error:
        print(f'An error occurred: {error}')
        return {'statusCode': 500, 'body': json.dumps(f'An error occurred: {error}')}
