import boto3
import json
import time

# --- CONFIGURATION ---
INPUT_FILE = 'japan_weather_final.json' 
TABLE_NAME = 'WeatherForecast'
USE_LOCAL_DB = True 

def get_db():
    if USE_LOCAL_DB:
        print("🔧 Connecting to LOCAL Java DynamoDB...")
        return boto3.resource('dynamodb', endpoint_url='http://localhost:8000', 
                              region_name='us-east-1',
                              aws_access_key_id='fake', aws_secret_access_key='fake')
    else:
        print("☁️ Connecting to REAL AWS DynamoDB...")
        return boto3.resource('dynamodb', region_name='us-east-1')

def upload():
    dynamodb = get_db()
    table = dynamodb.Table(TABLE_NAME)
    
    try:
        table.load()
    except:
        print(f"⚠️ Table {TABLE_NAME} missing. Creating...")
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'GridID', 'KeyType': 'HASH'}, 
                {'AttributeName': 'Timestamp', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'GridID', 'AttributeType': 'S'},
                {'AttributeName': 'Timestamp', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        table.meta.client.get_waiter('table_exists').wait(TableName=TABLE_NAME)

    print(f"🚀 Reading {INPUT_FILE}...")
    try:
        with open(INPUT_FILE, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print("❌ File not found. Did you run Pipeline 1?")
        return

    total = len(data)
    print(f"🔥 Uploading {total} records...")
    
    with table.batch_writer() as batch:
        for i, record in enumerate(data):
            safe_record = json.loads(json.dumps(record), parse_float=str)
            batch.put_item(Item=safe_record)
            if i % 5000 == 0 and i > 0:
                print(f"   Uploaded {i}/{total} records...")

    print("✅ Upload Complete!")

if __name__ == "__main__":
    upload()