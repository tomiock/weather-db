import folium
import json
import os
from tqdm import tqdm

# --- CONFIGURATION ---
INPUT_FILE = '/data/users/tockier/world_weather_final.json'
GRID_DEG = 0.18
LIMIT_DRAW = None  # ⚠️ Browser may crash if you draw all 30k grids. Set to None for full map.

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

def generate_map_from_json():
    print(f"🚀 Loading {INPUT_FILE} (this might take a moment)...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)

    print(f"   Loaded {len(data)} records. Extracting unique grids...")
    
    # Deduplicate: We only need one record per GridID to know its location/type
    unique_grids = {}
    
    # We prefer to find a record that has "IsAnchor" flag populated
    for item in tqdm(data, desc="Processing"):
        gid = item.get('GridID')
        
        # Skip CityLookup records if we want strictly grid visualization, 
        # but they are useful if they are the only record. 
        # Ideally we want the Weather records which have 'Temperature'
        if gid not in unique_grids and 'Lat' in item:
            unique_grids[gid] = {
                'GridID': gid,
                'LocationName': item.get('LocationName', 'Unknown'),
                'Lat': float(item['Lat']),
                'Lon': float(item['Lon']),
                'IsAnchor': item.get('IsAnchor', False)
            }

    grids = list(unique_grids.values())
    total_grids = len(grids)
    print(f"✅ Found {total_grids} unique grids.")

    # Calculate map center
    if not grids:
        print("❌ No grids found.")
        return

    avg_lat = sum(g['Lat'] for g in grids) / len(grids)
    avg_lon = sum(g['Lon'] for g in grids) / len(grids)

    print(f"📍 Centering map on {avg_lat:.2f}, {avg_lon:.2f}...")
    m = folium.Map(location=[avg_lat, avg_lon], zoom_start=3, prefer_canvas=True)

    # Sort so Anchors are drawn last (on top) if overlapping, or just to organize
    grids.sort(key=lambda x: x['IsAnchor'])

    draw_count = 0
    limit = LIMIT_DRAW if LIMIT_DRAW else total_grids

    print(f"🎨 Drawing {limit} grids...")
    
    for grid in tqdm(grids[:limit], desc="Drawing"):
        is_real = grid['IsAnchor']
        
        # Color Logic
        color = "#e74c3c" if is_real else "#3498db"  # Red = Real, Blue = Synthetic
        fill_color = color
        opacity = 0.6 if is_real else 0.3
        
        bounds = get_grid_bounds_from_id(grid['GridID'])
        
        if bounds:
            folium.Rectangle(
                bounds=bounds,
                color=color,
                weight=1,
                fill=True,
                fill_color=fill_color,
                fill_opacity=opacity,
                popup=folium.Popup(f"<b>{grid['LocationName']}</b><br>{'REAL' if is_real else 'SYNTHETIC'}", max_width=200)
            ).add_to(m)
            draw_count += 1

    output_file = "global_weather_coverage.html"
    m.save(output_file)
    print(f"✅ Map generated: {output_file}")
    if total_grids > limit:
        print(f"⚠️  NOTE: Only showed {limit} out of {total_grids} grids to prevent browser crash.")

if __name__ == "__main__":
    generate_map_from_json()