import boto3
import os

# --- TOGGLE THIS FLAG ---
# Set to TRUE for development (uses your server's Java DB)
# Set to FALSE for the final $12 upload (uses real AWS)
USE_LOCAL_DB = True

def get_dynamodb_resource():
    if USE_LOCAL_DB:
        # Connects to the Java tool running on your server
        print("🔧 connecting to LOCAL DynamoDB (localhost:8000)...")
        return boto3.resource('dynamodb', 
                              endpoint_url='http://localhost:8000',
                              region_name='us-east-1',
                              aws_access_key_id='fake', 
                              aws_secret_access_key='fake')
    else:
        # Connects to real AWS using your exported Env Vars
        print("☁️ connecting to REAL AWS DynamoDB...")
        return boto3.resource('dynamodb', region_name='us-east-1')

# --- USAGE ---
dynamodb = get_dynamodb_resource()
table = dynamodb.Table('WeatherForecast')

print(table)