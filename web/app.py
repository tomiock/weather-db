from flask import Flask, render_template, jsonify, request
import boto3
from boto3.dynamodb.conditions import Attr, Key
import os
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

# --- CONFIGURATION ---
TABLE_NAME = 'WeatherForecast'
REGION = 'us-east-1'
USE_LOCAL_DB = False 
GRID_DEG = 0.18
MAX_WORKERS = 10  # Parallel threads

def get_db():
    if USE_LOCAL_DB:
        return boto3.resource('dynamodb', endpoint_url='http://localhost:8000', 
                              region_name=REGION,
                              aws_access_key_id='fake', aws_secret_access_key='fake')
    else:
        return boto3.resource('dynamodb', region_name=REGION)

# Helper function for threading
def fetch_batch_items(dynamodb, chunk):
    """Fetches a single batch of 100 items"""
    try:
        response = dynamodb.batch_get_item(
            RequestItems={
                TABLE_NAME: {
                    'Keys': chunk,
                    'ProjectionExpression': "GridID, LocationName, Temperature, Precipitation, Humidity, WindSpeed, IsAnchor"
                }
            }
        )
        return response['Responses'].get(TABLE_NAME, [])
    except Exception as e:
        print(f"Batch Error: {e}")
        return []

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/timestamps')
def get_timestamps():
    dynamodb = get_db()
    table = dynamodb.Table(TABLE_NAME)
    
    try:
        scan_one = table.scan(Limit=1)
        if not scan_one['Items']:
            return jsonify([])
        
        sample_grid_id = scan_one['Items'][0]['GridID']
        
        response = table.query(
            KeyConditionExpression=Key('GridID').eq(sample_grid_id),
            ProjectionExpression="#ts",
            ExpressionAttributeNames={"#ts": "Timestamp"}
        )
        
        timestamps = sorted(list(set([item['Timestamp'] for item in response['Items']])))
        clean_timestamps = [t for t in timestamps if "METADATA" not in t]
        
        return jsonify(clean_timestamps)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/weather')
def get_weather():
    timestamp = request.args.get('timestamp')
    min_lat = request.args.get('min_lat', type=float)
    max_lat = request.args.get('max_lat', type=float)
    min_lon = request.args.get('min_lon', type=float)
    max_lon = request.args.get('max_lon', type=float)

    if not timestamp or min_lat is None:
        return jsonify({"error": "Missing parameters"}), 400

    dynamodb = get_db()
    
    # 1. Calculate Grid Ranges
    min_gx = math.floor(min_lon / GRID_DEG)
    max_gx = math.floor(max_lon / GRID_DEG)
    min_gy = math.floor(min_lat / GRID_DEG)
    max_gy = math.floor(max_lat / GRID_DEG)

    # 2. Generate Keys
    keys_to_fetch = []
    
    # Safety Limit
    total_grids = (max_gx - min_gx + 1) * (max_gy - min_gy + 1)
    if total_grids > 10000: # Increased limit slightly due to parallelism
        return jsonify({"error": "Area too large. Please zoom in."}), 400

    for gx in range(min_gx, max_gx + 1):
        for gy in range(min_gy, max_gy + 1):
            keys_to_fetch.append({
                'GridID': f"GRID#{gx}#{gy}",
                'Timestamp': timestamp
            })

    if not keys_to_fetch:
        return jsonify([])

    # 3. Parallel Batch Fetch
    results = []
    batch_size = 100 # DynamoDB Hard Limit
    chunks = [keys_to_fetch[i:i + batch_size] for i in range(0, len(keys_to_fetch), batch_size)]
    
    print(f"⚡ Fetching {len(keys_to_fetch)} grids in {len(chunks)} parallel batches...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all batches to the pool
        future_to_chunk = {executor.submit(fetch_batch_items, dynamodb, chunk): chunk for chunk in chunks}
        
        for future in as_completed(future_to_chunk):
            data = future.result()
            results.extend(data)

    # Convert Decimal to float
    for item in results:
        for k, v in item.items():
            if hasattr(v, 'to_eng_string'):
                item[k] = float(v)

    return jsonify(results)

if __name__ == '__main__':
    if not os.path.exists('templates'):
        os.makedirs('templates')
    print("🚀 Weather App running on http://127.0.0.1:5000")
    app.run(debug=True, port=5000)
