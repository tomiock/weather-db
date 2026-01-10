import pandas as pd
import requests
import time
import json
import math
import numpy as np
import random
import os
from tqdm import tqdm
from scipy.spatial import cKDTree

# --- CONFIGURATION ---
INPUT_FILE = '/home-local/tockier/weather/database_cities/worldcities.csv'
OUTPUT_FILE = '/data/users/tockier/world_weather_final.json'
CHECKPOINT_FILE = '/data/users/tockier/checkpoint_anchors.json'
ANCHOR_PERCENTAGE = 0.05
GRID_DEG = 0.18

# API Config
API_URL = "https://api.open-meteo.com/v1/forecast"

# --- PHASE 1: GRID GENERATION & DEDUPLICATION ---
def generate_unique_grids():
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print("❌ Error: 'worldcities.csv' not found.")
        return [], []

    # 1. Weather Representatives (One per Grid)
    grid_map = {}
    
    # 2. All City Lookups (One per City) - NEW LIST
    all_city_lookups = []
    
    print(f"   Processing {len(df)} candidate cities...")

    # WRAPPED WITH TQDM for ETA on initial processing
    for idx, row in tqdm(df.iterrows(), total=df.shape[0], desc="Mapping Cities", unit="city"):
        lat, lon = row['lat'], row['lng']
        pop = row['population']
        
        # Calculate Grid ID (The Bucket)
        gx = math.floor(lon / GRID_DEG)
        gy = math.floor(lat / GRID_DEG)
        grid_id = f"GRID#{gx}#{gy}"
        
        # Grid Center Coordinates
        grid_lat = (gy * GRID_DEG) + (GRID_DEG/2)
        grid_lon = (gx * GRID_DEG) + (GRID_DEG/2)
        
        city_name = row['city_ascii']
        country_name = row['country'] # Capture Country
        
        # --- A. CREATE LOOKUP RECORD (For Search) ---
        all_city_lookups.append({
            "GridID": grid_id,
            "Timestamp": "0000-00-00_METADATA", 
            "Type": "CityLookup",
            "LocationName": city_name,
            "Country": country_name,  # <--- NEW FIELD
            "Lat": lat, 
            "Lon": lon,
            "Population": pop if not pd.isna(pop) else 0
        })

        # --- B. COLLISION LOGIC (For Weather Generation) ---
        city_data = {
            "GridID": grid_id,
            "City": city_name,
            "LocationName": city_name,
            "Country": country_name, # <--- NEW FIELD
            "Lat": grid_lat,
            "Lon": grid_lon,
            "Population": pop if not pd.isna(pop) else 0
        }
        
        if grid_id in grid_map:
            if city_data['Population'] > grid_map[grid_id]['Population']:
                grid_map[grid_id] = city_data 
        else:
            grid_map[grid_id] = city_data

    unique_grids = list(grid_map.values())
    print(f"✅ Created {len(all_city_lookups)} city lookup records.")
    print(f"✅ Reduced to {len(unique_grids)} unique grids for weather generation.")
    
    # --- SAMPLING PASS 1: SYSTEMATIC SPATIAL (5%) ---
    unique_grids.sort(key=lambda x: (-x['Lat'], x['Lon']))
    stride = int(1 / ANCHOR_PERCENTAGE)
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
    target_pop_count = int(len(unique_grids) * ANCHOR_PERCENTAGE)
    non_anchors = [g for g in unique_grids if not g['IsAnchor']]
    non_anchors.sort(key=lambda x: x['Population'], reverse=True)
    
    pop_count = 0
    for i in range(min(len(non_anchors), target_pop_count)):
        non_anchors[i]['IsAnchor'] = True 
        pop_count += 1
        
    print(f"   Pass 2 (Population): Selected {pop_count} additional anchors.")
    print(f"🎯 Total Anchors: {spatial_count + pop_count} (out of {len(unique_grids)} grids).")
    
    return unique_grids, all_city_lookups

# --- PHASE 2: FETCH REAL DATA (WITH CHECKPOINTS) ---
def fetch_anchors(grids):
    print(f"\n📡 PHASE 2: Fetching Real Data for Anchors...")
    
    anchor_grids = [g for g in grids if g['IsAnchor']]
    real_data_cache = {} 
    
    # --- RESUME LOGIC ---
    if os.path.exists(CHECKPOINT_FILE):
        print(f"   ⚠️  Found checkpoint file '{CHECKPOINT_FILE}'. Resuming...")
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                real_data_cache = json.load(f)
            print(f"   ✅ Loaded {len(real_data_cache)} already fetched anchors.")
        except Exception as e:
            print(f"   ❌ Error loading checkpoint: {e}. Starting fresh.")

    params = {
        "hourly": "temperature_2m,relative_humidity_2m,precipitation_probability,precipitation,wind_speed_10m",
        "daily": "temperature_2m_max,precipitation_sum,precipitation_probability_max,wind_speed_10m_max",
        "timezone": "UTC", 
        "forecast_days": 7
    }

    # Only fetch grids that are NOT in the cache
    grids_to_fetch = [g for g in anchor_grids if g['GridID'] not in real_data_cache]
    
    print(f"   📝 Need to fetch {len(grids_to_fetch)} more anchors (Total: {len(anchor_grids)}).")

    # CORRECT TQDM USAGE: wrap the list, THEN enumerate
    for i, grid in enumerate(tqdm(grids_to_fetch, desc="Fetching API", unit="call")):
        try:
            p = params.copy()
            p['latitude'] = grid['Lat']
            p['longitude'] = grid['Lon']
            
            r = requests.get(API_URL, params=p, timeout=5)
            if r.status_code == 200:
                real_data_cache[grid['GridID']] = r.json()
            elif r.status_code == 429:
                 print("\n   ❌ API LIMIT REACHED (429). Stop script (Ctrl+C) and retry later.")
            else:
                # Use tqdm.write so the progress bar doesn't get messed up
                tqdm.write(f"   ❌ API Error {r.status_code}")
            
            # Save checkpoint every 50 requests
            if i % 50 == 0:
                 with open(CHECKPOINT_FILE, 'w') as f:
                    json.dump(real_data_cache, f)

            time.sleep(0.1)
            
        except Exception as e:
            tqdm.write(f"   ❌ Exception: {e}")
            
    # Final save of checkpoint
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(real_data_cache, f)
            
    return real_data_cache

# --- PHASE 3: INTERPOLATION ---
def interpolate_and_save(all_grids, real_data_cache, all_city_lookups):
    print(f"\nmath PHASE 3: Creating Synthetic Data (Nearest Neighbor)...")
    
    valid_anchors = [g for g in all_grids if g['GridID'] in real_data_cache]
    
    if not valid_anchors:
        print("❌ Critical: No anchor data available.")
        return

    anchor_coords = [[g['Lat'], g['Lon']] for g in valid_anchors]
    anchor_ids = [g['GridID'] for g in valid_anchors]
    
    tree = cKDTree(anchor_coords)
    final_records = []
    
    # 1. Add ALL City Lookups first
    final_records.extend(all_city_lookups)
    
    # 2. Generate Weather for Grids
    # WRAPPED WITH TQDM
    for grid in tqdm(all_grids, desc="Interpolating", unit="grid"):
        # Find nearest anchor
        dist, idx = tree.query([grid['Lat'], grid['Lon']], k=1)
        nearest_anchor_id = anchor_ids[idx]
        source_data = real_data_cache[nearest_anchor_id]
        
        is_satellite = (grid['GridID'] != nearest_anchor_id)
        
        noise_factor = 0
        if is_satellite:
            noise_factor = np.random.normal(0, 0.5) + (dist * 2.0 * np.random.uniform(-1, 1))

        # Hourly
        h_data = source_data['hourly']
        for h in range(24):
            final_records.append({
                "GridID": grid['GridID'],
                "Timestamp": h_data['time'][h], 
                "Type": "Hourly",
                "LocationName": grid['LocationName'], # Main city name
                "Country": grid.get('Country', 'Unknown'), # <--- PASS COUNTRY TO WEATHER DATA TOO
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
                "Country": grid.get('Country', 'Unknown'), # <--- PASS COUNTRY TO WEATHER DATA TOO
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
    grids, lookups = generate_unique_grids()
    if grids:
        real_data = fetch_anchors(grids)
        interpolate_and_save(grids, real_data, lookups)
