import pandas as pd
import math

# --- CONFIGURATION ---
# Same file path as your generate_data.py
INPUT_FILE = '/home-local/tockier/weather/database_cities/worldcities.csv'
GRID_DEG = 0.18
ANCHOR_PERCENTAGE = 0.10  # 10% (5% Spatial + 5% Population)

# Open-Meteo Free Tier Limits
LIMIT_DAILY = 10000
LIMIT_HOURLY = 5000

def analyze_global_grids():
    print(f"🌍 READING GLOBAL DATA from {INPUT_FILE}...")
    
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print("❌ Error: 'worldcities.csv' not found.")
        return

    total_cities = len(df)
    print(f"   Found {total_cities:,} cities in total.")

    # --- SIMULATE GRIDDING PROCESS ---
    print("   Calculating unique grids (0.18°)...")
    grid_map = {}

    for idx, row in df.iterrows():
        lat, lon = row['lat'], row['lng']
        
        # Exact same logic as generate_data.py
        gx = math.floor(lon / GRID_DEG)
        gy = math.floor(lat / GRID_DEG)
        grid_id = f"GRID#{gx}#{gy}"
        
        # We only need to know it exists, we don't need the payload for this count
        grid_map[grid_id] = True

    unique_grid_count = len(grid_map)
    
    # --- CALCULATE ANCHORS ---
    # In generate_data.py, you take 5% spatial + 5% population ~ 10% total
    projected_anchors = int(unique_grid_count * ANCHOR_PERCENTAGE)
    
    # --- ESTIMATE RUNTIME & LIMITS ---
    # Current sleep is 0.5s
    current_sleep = 0.5
    calls_per_hour = 3600 / (current_sleep + 0.2) # assuming 0.2s network latency
    
    print("\n" + "="*40)
    print("📊 GLOBAL GRID ANALYSIS REPORT")
    print("="*40)
    print(f"🏙️  Total Cities:          {total_cities:,}")
    print(f"📦 Unique Grids (Buckets): {unique_grid_count:,}")
    print(f"⚓ Projected Anchors (API): {projected_anchors:,} (approx 10%)")
    print("-" * 40)
    
    # --- DAILY LIMIT CHECK ---
    print(f"🔍 DAILY LIMIT CHECK ({LIMIT_DAILY} calls/day):")
    if projected_anchors < LIMIT_DAILY:
        print(f"   ✅ SAFE. You are using {projected_anchors / LIMIT_DAILY:.1%} of your daily quota.")
    else:
        print(f"   ❌ DANGER. {projected_anchors} calls exceeds the limit of {LIMIT_DAILY}.")
        print(f"      Recommendation: Reduce ANCHOR_PERCENTAGE to {LIMIT_DAILY / unique_grid_count:.2f} or less.")

    # --- HOURLY LIMIT CHECK ---
    print(f"\n⚡ HOURLY LIMIT CHECK ({LIMIT_HOURLY} calls/hour):")
    if projected_anchors < LIMIT_HOURLY:
        print("   ✅ SAFE. Total calls are less than the hourly limit.")
    elif calls_per_hour > LIMIT_HOURLY:
        print(f"   ⚠️  CAUTION. With sleep({current_sleep}s), you might hit ~{int(calls_per_hour)} calls/hour.")
        print(f"      The limit is {LIMIT_HOURLY}.")
        suggested_sleep = (3600 / LIMIT_HOURLY) - 0.1
        print(f"      👉 RECOMMENDATION: Increase time.sleep() to at least {suggested_sleep:.2f} seconds.")
    else:
        print("   ✅ SAFE. Your speed is within hourly limits.")

if __name__ == "__main__":
    analyze_global_grids()