import boto3
import json
import time
import sys
from tqdm import tqdm
from botocore.exceptions import ClientError

# --- CONFIGURATION ---
INPUT_FILE = '/data/users/tockier/world_weather_final.json'
TABLE_NAME = 'WeatherForecast'

# TOGGLE THIS FOR DEPLOYMENT
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

def create_table_safe(dynamodb):
    # Define Schema
    key_schema = [
        {'AttributeName': 'GridID', 'KeyType': 'HASH'}, 
        {'AttributeName': 'Timestamp', 'KeyType': 'RANGE'}
    ]
    
    attr_defs = [
        {'AttributeName': 'GridID', 'AttributeType': 'S'},
        {'AttributeName': 'Timestamp', 'AttributeType': 'S'},
        {'AttributeName': 'LocationName', 'AttributeType': 'S'} 
    ]
    
    gsi = [
        {
            'IndexName': 'CityNameIndex',
            'KeySchema': [
                {'AttributeName': 'LocationName', 'KeyType': 'HASH'},
                {'AttributeName': 'Timestamp', 'KeyType': 'RANGE'}
            ],
            'Projection': {'ProjectionType': 'ALL'}
        }
    ]

    billing_mode = 'PAY_PER_REQUEST' if not USE_LOCAL_DB else 'PROVISIONED'
    throughput = {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5} if USE_LOCAL_DB else {}

    args = {
        'TableName': TABLE_NAME,
        'KeySchema': key_schema,
        'AttributeDefinitions': attr_defs,
        'GlobalSecondaryIndexes': gsi
    }

    if USE_LOCAL_DB:
        args['ProvisionedThroughput'] = throughput
        args['GlobalSecondaryIndexes'][0]['ProvisionedThroughput'] = throughput
    else:
        args['BillingMode'] = billing_mode

    print(f"✨ Creating table {TABLE_NAME}...")
    table = dynamodb.create_table(**args)
    table.meta.client.get_waiter('table_exists').wait(TableName=TABLE_NAME)
    print("✅ Table created successfully.")
    return table

def upload():
    # --- SAFETY CHECK ---
    if not USE_LOCAL_DB:
        print("!"*60)
        print("⚠️  WARNING: YOU ARE ABOUT TO UPLOAD TO REAL AWS!")
        print("!"*60)
        confirm = input(f"Are you sure you want to write to table '{TABLE_NAME}' in AWS? (yes/no): ")
        if confirm.lower() != 'yes':
            print("❌ Aborted.")
            return

    dynamodb = get_db()
    
    # --- TABLE HANDLING STRATEGY ---
    try:
        table = dynamodb.Table(TABLE_NAME)
        table.load()
        print(f"ℹ️  Table {TABLE_NAME} exists.")
        
        # IF LOCAL: We assume you want to update the schema (GSI), so we wipe it.
        if USE_LOCAL_DB:
            print("♻️  LOCAL MODE: Deleting existing table to ensure schema update...")
            table.delete()
            table.meta.client.get_waiter('table_not_exists').wait(TableName=TABLE_NAME)
            table = create_table_safe(dynamodb)
            
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"⚠️ Table {TABLE_NAME} missing. Creating new table...")
            table = create_table_safe(dynamodb)
        else:
            print(f"❌ Unexpected Database Error: {e}")
            return
    except Exception as e:
        print(f"❌ Connection Error (Is DynamoDB Local running?): {e}")
        return

    # Load Data
    print(f"🚀 Reading artifact: {INPUT_FILE}...")
    try:
        with open(INPUT_FILE, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: '{INPUT_FILE}' not found.")
        return

    total = len(data)
    print(f"🔥 Uploading {total} records to {'LOCAL' if USE_LOCAL_DB else 'AWS'}...")
    
    start_time = time.time()
    
    # --- DEDUPLICATION CACHE ---
    # Keeps track of keys seen IN THIS UPLOAD to prevent batch errors
    seen_keys = set()
    skipped_dupes = 0
    
    with table.batch_writer() as batch:
        for i, record in enumerate(tqdm(data)):
            
            if record.get('Type') == 'CityLookup':
                try:
                    clean_name = record['LocationName'].replace(' ', '_').replace('#', '')
                    record['Timestamp'] = f"0000-00-00_METADATA#{clean_name}"
                except Exception as e:
                    print(f'{e}, skipping...')
                    continue
            
            pk_sig = f"{record['GridID']}::{record['Timestamp']}"
            
            if pk_sig in seen_keys:
                skipped_dupes += 1
                continue # Skip this record
            
            seen_keys.add(pk_sig)
            
            # Float sanitization
            safe_record = json.loads(json.dumps(record), parse_float=str)
            try:
                batch.put_item(Item=safe_record)
            except Exception as e:
                print(f'{e}, skipping...')
                continue
            
    print(f"✅ Upload Complete! Total time: {time.time() - start_time:.2f}s")
    if skipped_dupes > 0:
        print(f"⚠️  Skipped {skipped_dupes} duplicate keys to prevent errors.")

if __name__ == "__main__":
    upload()
