import folium
import boto3
from boto3.dynamodb.conditions import Attr
import os

# --- CONFIGURATION ---
USE_LOCAL_DB = True
TABLE_NAME = 'WeatherForecast'
GRID_DEG = 0.18  # Must match the generator's grid size

def get_db():
    if USE_LOCAL_DB:
        return boto3.resource('dynamodb', endpoint_url='http://localhost:8000', 
                              region_name='us-east-1',
                              aws_access_key_id='fake', aws_secret_access_key='fake')
    else:
        return boto3.resource('dynamodb', region_name='us-east-1')

def get_grid_bounds_from_id(grid_id):
    """Parses GRID#X#Y to get lat/lon bounds"""
    try:
        parts = grid_id.split('#')
        gx = int(parts[1])
        gy = int(parts[2])
        
        min_lon = gx * GRID_DEG
        min_lat = gy * GRID_DEG
        max_lon = (gx + 1) * GRID_DEG
        max_lat = (gy + 1) * GRID_DEG
        
        return [[min_lat, min_lon], [max_lat, max_lon]]
    except (IndexError, ValueError):
        return None

def generate_map_from_db():
    print(f"📡 Connecting to DynamoDB ({'LOCAL' if USE_LOCAL_DB else 'AWS'})...")
    dynamodb = get_db()
    table = dynamodb.Table(TABLE_NAME)

    print("🔍 Scanning table for unique grids...")
    
    unique_grids = {}
    
    # We scan for the needed attributes
    scan_kwargs = {
        'ProjectionExpression': "GridID, LocationName, Lat, Lon, Temperature, IsAnchor",
    }
    
    done = False
    start_key = None
    count = 0
    skipped = 0
    
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
            
        response = table.scan(**scan_kwargs)
        items = response.get('Items', [])
        count += len(items)
        
        for item in items:
            gid = item['GridID']
            
            # --- SAFEGUARD ---
            if 'Lat' not in item or 'Lon' not in item:
                skipped += 1
                continue

            if gid not in unique_grids:
                try:
                    unique_grids[gid] = {
                        'GridID': gid,
                        'LocationName': item.get('LocationName', 'Unknown'),
                        'Lat': float(item['Lat']),
                        'Lon': float(item['Lon']),
                        'Temperature': float(item['Temperature']),
                        'IsAnchor': item.get('IsAnchor', False)
                    }
                except (ValueError, TypeError):
                    skipped += 1
                    continue
        
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
        print(f"   Scanned {count} records... found {len(unique_grids)} unique grids.")
    
    if not unique_grids:
        print("❌ No valid grid data found.")
        return

    grids = list(unique_grids.values())
    
    # Calculate map center
    avg_lat = sum(g['Lat'] for g in grids) / len(grids)
    avg_lon = sum(g['Lon'] for g in grids) / len(grids)

    print(f"📍 Centering map on Japan (Lat: {avg_lat:.2f}, Lon: {avg_lon:.2f})...")
    m = folium.Map(location=[avg_lat, avg_lon], zoom_start=6)

    print(f"🎨 Drawing {len(grids)} grids as rectangular blocks...")

    for grid in grids:
        temp = grid['Temperature']
        is_real = grid['IsAnchor']
        
        # Color Logic: Real = Red, Synthetic = Blue
        # Using lighter/transparent fill so we can see map underneath
        color = "#e74c3c" if is_real else "#3498db" 
        data_type = "REAL DATA" if is_real else "SYNTHETIC"
        
        # Calculate Bounds
        bounds = get_grid_bounds_from_id(grid['GridID'])
        
        if bounds:
            # 1. Draw Rectangle (The Grid)
            folium.Rectangle(
                bounds=bounds,
                color=color,
                weight=1,
                fill=True,
                fill_color=color,
                fill_opacity=0.4,
                popup=folium.Popup(f"<b>{grid['LocationName']}</b><br>Type: {data_type}<br>ID: {grid['GridID']}", max_width=200)
            ).add_to(m)

            # 2. Add Label (Name + Temp) centered in the box
            folium.Marker(
                [grid['Lat'], grid['Lon']],
                icon=folium.DivIcon(
                    html=f"""
                        <div style="
                            font-family: Arial; color: black; font-weight: bold; font-size: 10px; 
                            text-align: center; white-space: nowrap; 
                            text-shadow: 0px 0px 3px white;
                            transform: translate(-50%, -50%);
                        ">
                            {grid['LocationName']}<br>
                            <span style="color:{'darkred' if is_real else 'darkblue'};">{temp}°C</span>
                        </div>
                    """
                )
            ).add_to(m)

    output_file = "japan_live_weather_map.html"
    m.save(output_file)
    print(f"✅ Map generated! Open '{output_file}' to see the Grid Lines.")

if __name__ == "__main__":
    generate_map_from_db()