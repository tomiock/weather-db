import pandas as pd
import requests
import time
import json
import math
import numpy as np
import random
from scipy.spatial import cKDTree

# --- CONFIGURATION ---
INPUT_FILE = '/home-local/tockier/weather/database_cities/worldcities.csv'
OUTPUT_FILE = 'japan_weather_final.json'
TARGET_COUNTRY = 'Japan'
ANCHOR_PERCENTAGE = 0.05  # Base percentage for EACH pass (Total will be ~10%)
GRID_DEG = 0.18

# API Config
API_URL = "https://api.open-meteo.com/v1/forecast"

# --- PHASE 1: GRID GENERATION & DEDUPLICATION ---
def generate_unique_grids():
    print(f"🇯🇵 PHASE 1: Generating unique grids for {TARGET_COUNTRY}...")
    
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print("❌ Error: 'worldcities.csv' not found.")
        return []

    # Filter Japan
    df = df[df['country'] == TARGET_COUNTRY].copy()
    
    # Dictionary to handle collisions: {GridID: CityData}
    grid_map = {}
    
    print(f"   Processing {len(df)} candidate cities...")

    for idx, row in df.iterrows():
        lat, lon = row['lat'], row['lng']
        pop = row['population']
        
        # Calculate Grid ID (The Bucket)
        gx = math.floor(lon / GRID_DEG)
        gy = math.floor(lat / GRID_DEG)
        grid_id = f"GRID#{gx}#{gy}"
        
        # Grid Center Coordinates
        grid_lat = (gy * GRID_DEG) + (GRID_DEG/2)
        grid_lon = (gx * GRID_DEG) + (GRID_DEG/2)
        
        city_data = {
            "GridID": grid_id,
            "City": row['city_ascii'],
            "LocationName": row['city_ascii'],
            "Lat": grid_lat,
            "Lon": grid_lon,
            "Population": pop if not pd.isna(pop) else 0
        }
        
        # COLLISION LOGIC: Keep the city with the highest population
        if grid_id in grid_map:
            if city_data['Population'] > grid_map[grid_id]['Population']:
                grid_map[grid_id] = city_data 
        else:
            grid_map[grid_id] = city_data

    unique_grids = list(grid_map.values())
    print(f"✅ Reduced to {len(unique_grids)} unique grids.")
    
    # --- SAMPLING PASS 1: SYSTEMATIC SPATIAL (5%) ---
    # Sort by Latitude (North -> South), then Longitude
    unique_grids.sort(key=lambda x: (-x['Lat'], x['Lon']))
    
    # Calculate Stride
    stride = int(1 / ANCHOR_PERCENTAGE)
    
    # Add random offset
    start_index = random.randint(0, stride - 1)
    
    spatial_count = 0
    for i in range(len(unique_grids)):
        if (i - start_index) % stride == 0:
            unique_grids[i]['IsAnchor'] = True
            spatial_count += 1
        else:
            unique_grids[i]['IsAnchor'] = False
            
    print(f"   Pass 1 (Spatial): Selected {spatial_count} anchors.")

    # --- SAMPLING PASS 2: POPULATION TOP-UP (5%) ---
    # We want another 5% of the TOTAL grids, strictly based on population.
    target_pop_count = int(len(unique_grids) * ANCHOR_PERCENTAGE)
    
    # Filter for grids that are NOT yet anchors from Pass 1
    # We create a list of candidates to sort, but we must modify the dictionary objects in place
    non_anchors = [g for g in unique_grids if not g['IsAnchor']]
    
    # Sort them by Population (Highest first)
    non_anchors.sort(key=lambda x: x['Population'], reverse=True)
    
    pop_count = 0
    # Select the top N from the remaining candidates
    for i in range(min(len(non_anchors), target_pop_count)):
        # Modifying the dictionary here reflects in the main 'unique_grids' list
        non_anchors[i]['IsAnchor'] = True 
        pop_count += 1
        
    print(f"   Pass 2 (Population): Selected {pop_count} additional anchors (Top populated areas).")
    print(f"🎯 Total Anchors: {spatial_count + pop_count} (out of {len(unique_grids)} grids).")
    
    return unique_grids

# --- PHASE 2: FETCH REAL DATA ---
def fetch_anchors(grids):
    print(f"\n📡 PHASE 2: Fetching Real Data for Anchors...")
    
    anchor_grids = [g for g in grids if g['IsAnchor']]
    real_data_cache = {} 
    
    params = {
        "hourly": "temperature_2m,relative_humidity_2m,precipitation_probability,precipitation,wind_speed_10m",
        "daily": "temperature_2m_max,precipitation_sum,precipitation_probability_max,wind_speed_10m_max",
        "timezone": "Asia/Tokyo",
        "forecast_days": 7
    }

    for i, grid in enumerate(anchor_grids):
        print(f"   [{i+1}/{len(anchor_grids)}] Fetching {grid['LocationName']}...")
        
        try:
            p = params.copy()
            p['latitude'] = grid['Lat']
            p['longitude'] = grid['Lon']
            
            r = requests.get(API_URL, params=p, timeout=5)
            if r.status_code == 200:
                real_data_cache[grid['GridID']] = r.json()
            else:
                print(f"❌ API Error {r.status_code}")
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Exception: {e}")
            
    return real_data_cache

# --- PHASE 3: INTERPOLATION ---
def interpolate_and_save(all_grids, real_data_cache):
    print(f"\nmath PHASE 3: Creating Synthetic Data (Nearest Neighbor)...")
    
    valid_anchors = [g for g in all_grids if g['GridID'] in real_data_cache]
    
    if not valid_anchors:
        print("❌ Critical: No anchor data available.")
        return

    anchor_coords = [[g['Lat'], g['Lon']] for g in valid_anchors]
    anchor_ids = [g['GridID'] for g in valid_anchors]
    
    tree = cKDTree(anchor_coords)
    final_records = []
    
    for grid in all_grids:
        # Find nearest anchor
        dist, idx = tree.query([grid['Lat'], grid['Lon']], k=1)
        nearest_anchor_id = anchor_ids[idx]
        source_data = real_data_cache[nearest_anchor_id]
        
        is_satellite = (grid['GridID'] != nearest_anchor_id)
        
        noise_factor = 0
        if is_satellite:
            noise_factor = np.random.normal(0, 0.5) + (dist * 2.0 * np.random.uniform(-1, 1))

        # --- EXTRACT & TRANSFORM ---
        # Hourly
        h_data = source_data['hourly']
        for h in range(24):
            final_records.append({
                "GridID": grid['GridID'],
                "Timestamp": h_data['time'][h],
                "Type": "Hourly",
                "LocationName": grid['LocationName'],
                "Lat": grid['Lat'],
                "Lon": grid['Lon'],
                "IsAnchor": grid['IsAnchor'],
                "Temperature": round(h_data['temperature_2m'][h] + noise_factor, 1),
                "Humidity": h_data['relative_humidity_2m'][h],
                "ChanceOfRain": h_data['precipitation_probability'][h],
                "Precipitation": h_data['precipitation'][h],
                "WindSpeed": round(max(0, h_data['wind_speed_10m'][h] + noise_factor), 1)
            })

        # Daily
        d_data = source_data['daily']
        for d in range(1, 7):
            final_records.append({
                "GridID": grid['GridID'],
                "Timestamp": f"{d_data['time'][d]}T12:00:00",
                "Type": "Daily",
                "LocationName": grid['LocationName'],
                "Lat": grid['Lat'],
                "Lon": grid['Lon'],
                "IsAnchor": grid['IsAnchor'],
                "Temperature": round(d_data['temperature_2m_max'][d] + noise_factor, 1),
                "ChanceOfRain": d_data['precipitation_probability_max'][d],
                "Precipitation": d_data['precipitation_sum'][d],
                "WindSpeed": round(max(0, d_data['wind_speed_10m_max'][d] + noise_factor), 1)
            })

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(final_records, f)
        
    print(f"🎉 Pipeline Complete! Saved {len(final_records)} records to {OUTPUT_FILE}")

if __name__ == "__main__":
    grids = generate_unique_grids()
    if grids:
        real_data = fetch_anchors(grids)
        interpolate_and_save(grids, real_data)