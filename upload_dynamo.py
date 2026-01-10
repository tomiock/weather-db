import boto3
import json
import time
import sys
from decimal import Decimal
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError
from tqdm import tqdm

# --- CONFIGURATION ---
INPUT_FILE = 'world_weather_final.json'
TABLE_NAME = 'WeatherForecast'
REGION = 'us-east-1'

def upload_from_laptop():
    # 1. Initialize Client
    try:
        # This will pick up credentials from ~/.aws/credentials
        client = boto3.client('dynamodb', region_name=REGION)
        serializer = TypeSerializer()
        print("✅ AWS Credentials detected.")
    except Exception as e:
        print(f"❌ Could not load AWS credentials: {e}")
        print("   Did you run 'aws configure' and add the 'aws_session_token'?")
        return

    # 2. Load Data
    print(f"📂 Reading {INPUT_FILE}...")
    try:
        # Parse floats as Decimal directly for DynamoDB compatibility
        with open(INPUT_FILE, 'r') as f:
            data = json.load(f, parse_float=Decimal)
    except FileNotFoundError:
        print(f"❌ Error: '{INPUT_FILE}' not found.")
        return

    total = len(data)
    print(f"🔥 Preparing to upload {total:,} records...")
    
    # 3. Batch Upload Loop
    seen_keys = set()
    batch = []
    
    # Helper to flush a batch of 25 items
    def flush_batch(batch_items):
        if not batch_items: return
        
        request_items = {
            TABLE_NAME: [{'PutRequest': {'Item': item}} for item in batch_items]
        }
        
        try:
            resp = client.batch_write_item(RequestItems=request_items)
            
            # Retry Throttled Items (Unprocessed)
            unprocessed = resp.get('UnprocessedItems', {})
            retries = 0
            while unprocessed and retries < 5:
                time.sleep(1 + retries) # Exponential backoff
                resp = client.batch_write_item(RequestItems=unprocessed)
                unprocessed = resp.get('UnprocessedItems', {})
                retries += 1
                
        except ClientError as e:
            if "ResourceNotFound" in str(e):
                print(f"\n❌ CRITICAL: Table '{TABLE_NAME}' does not exist.")
                print("   Run the 'aws dynamodb create-table' CLI command first.")
                sys.exit(1)
            print(f"⚠️  Batch Error: {e}")

    # Main Processing Loop
    start_time = time.time()
    
    # Using tqdm for a nice progress bar
    for record in tqdm(data, desc="Uploading", unit="rec"):
        
        # --- FIX: Uniqueness for City Lookups ---
        if record.get('Type') == 'CityLookup':
            try:
                clean_name = record['LocationName'].replace(' ', '_').replace('#', '')
                record['Timestamp'] = f"0000-00-00_METADATA#{clean_name}"
            except Exception as e:
                print(f'{e}, skipping')
                continue

        # --- FIX: Deduplication ---
        pk_sig = f"{record['GridID']}::{record['Timestamp']}"
        if pk_sig in seen_keys:
            continue
        seen_keys.add(pk_sig)

        # Serialize
        try:
            dynamo_item = {k: serializer.serialize(v) for k, v in record.items()}
            batch.append(dynamo_item)
        except Exception as e:
            print(f'{e}, skipping')
            continue

        # Flush if 25 items (DynamoDB Limit)
        if len(batch) == 25:
            flush_batch(batch)
            batch = []

    # Flush remainder
    if batch:
        flush_batch(batch)

    duration = time.time() - start_time
    print(f"\n✅ Upload Complete!")
    print(f"   Time taken: {duration:.2f} seconds")
    print(f"   Throughput: {total / duration:.0f} records/sec")

if __name__ == "__main__":
    upload_from_laptop()
