import boto3
from boto3.dynamodb.conditions import Key
import math

# --- CONFIGURATION ---
USE_LOCAL_DB = True

def get_db():
    if USE_LOCAL_DB:
        return boto3.resource('dynamodb', endpoint_url='http://localhost:8000', 
                              region_name='us-east-1',
                              aws_access_key_id='fake', aws_secret_access_key='fake')
    else:
        return boto3.resource('dynamodb', region_name='us-east-1')

def test_query():
    dynamodb = get_db()
    table = dynamodb.Table('WeatherForecast')
    GRID_DEG = 0.18

    # TEST: Barcelona (Large city, should have naming like "Barcelona Center")
    #41.5468° N, 2.1089° E
    #41°32'16.6"N 2°06'06.7"E
    lat, lon =  41.543348, 2.094540
    lat, lon = 41.561363, 2.011795
    lat, lon = 41.356825, 2.104608
    lat, lon = 41.384126, 2.142685
    lat, lon = 41.409335, 2.183115
    lat, lon = 41.395432, 2.175677
    
    gx = math.floor(lon / GRID_DEG)
    gy = math.floor(lat / GRID_DEG)
    target_grid = f"GRID#{gx}#{gy}"
    
    response = table.query(
        KeyConditionExpression=Key('GridID').eq(target_grid)
    )
    
    items = response.get('Items', [])
    
    if not items:
        print("❌ No data found.")
    else:
        first = items[0]
        print(f"✅ SUCCESS! Found {len(items)} records.")
        print(f"📍 Location Name in DB: '{first.get('LocationName', 'UNKNOWN')}'") 
        print("-" * 100)
        
        # Filter and sort lists
        hourlies = sorted([i for i in items if i.get('Type') == 'Hourly'], key=lambda x: x['Timestamp'])
        dailies = sorted([i for i in items if i.get('Type') == 'Daily'], key=lambda x: x['Timestamp'])

        # Exclude metadata keys we don't need to repeat on every line
        skip_keys = ['GridID', 'Timestamp', 'Type', 'LocationName', 'City']

        print(f"\n   [Hourly Snapshot ({len(hourlies)} records)]")
        for item in hourlies:
            ts_time = item['Timestamp'].split('T')[1] if 'T' in item['Timestamp'] else item['Timestamp']
            # Join all other attributes into a single string
            details = " | ".join([f"{k}: {v}" for k, v in item.items() if k not in skip_keys])
            print(f"   🕒 {ts_time} -> {details}")

        print(f"\n   [Daily Forecast ({len(dailies)} records)]")
        for item in dailies:
            ts_date = item['Timestamp'].split('T')[0] if 'T' in item['Timestamp'] else item['Timestamp']
            details = " | ".join([f"{k}: {v}" for k, v in item.items() if k not in skip_keys])
            print(f"   📅 {ts_date:<10} -> {details}")

if __name__ == "__main__":
    test_query()