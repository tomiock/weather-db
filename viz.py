import folium
import json
import os
from tqdm import tqdm

# --- CONFIGURATION ---
INPUT_FILE = '../weather_data/world_weather_final.json' # Updated to relative path for portability
GRID_DEG = 0.18
LIMIT_DRAW = None  # Set to None for full map, or integer (e.g. 5000) to test

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

def get_temp_color(temp):
    """Returns a color hex code based on temperature (Celsius)"""
    if temp is None:
        return "#808080" # Gray for no data
    
    if temp < -20: return "#2c003e" # Deep Purple
    if temp < -10: return "#00008b" # Dark Blue
    if temp < 0:   return "#0000ff" # Blue
    if temp < 10:  return "#00bfff" # Deep Sky Blue (Cold/Cool)
    if temp < 20:  return "#2ecc71" # Green (Pleasant)
    if temp < 25:  return "#f1c40f" # Yellow (Warm)
    if temp < 30:  return "#e67e22" # Orange (Hot)
    if temp < 40:  return "#e74c3c" # Red (Very Hot)
    return "#c0392b" # Dark Red (Extreme)

def generate_map_from_json():
    print(f"🚀 Loading {INPUT_FILE} (this might take a moment)...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)

    print(f"   Loaded {len(data)} records. Extracting unique grids...")
    
    unique_grids = {}
    
    # SCAN DATA: We need to find a record with 'Temperature' for each grid.
    # The JSON usually has Metadata first (No temp), then Weather (Has temp).
    for item in tqdm(data, desc="Processing"):
        gid = item.get('GridID')
        
        # Check if this record has temperature data
        has_temp = 'Temperature' in item
        
        # 1. New Grid found
        if gid not in unique_grids:
            unique_grids[gid] = {
                'GridID': gid,
                'LocationName': item.get('LocationName', 'Unknown'),
                'Lat': float(item['Lat']),
                'Lon': float(item['Lon']),
                'IsAnchor': item.get('IsAnchor', False),
                'Temperature': float(item['Temperature']) if has_temp else None
            }
        
        # 2. Existing Grid, but we didn't have a temperature yet (was created by a CityLookup record)
        elif unique_grids[gid]['Temperature'] is None and has_temp:
            unique_grids[gid]['Temperature'] = float(item['Temperature'])

    grids = list(unique_grids.values())
    total_grids = len(grids)
    print(f"✅ Found {total_grids} unique grids.")

    if not grids:
        print("❌ No grids found.")
        return

    # Calculate map center
    avg_lat = sum(g['Lat'] for g in grids) / len(grids)
    avg_lon = sum(g['Lon'] for g in grids) / len(grids)

    print(f"📍 Centering map on {avg_lat:.2f}, {avg_lon:.2f}...")
    m = folium.Map(location=[avg_lat, avg_lon], zoom_start=3, prefer_canvas=True)

    # Sort by Temp so the drawing order is consistent
    # (Optional: Sort so warmer or colder is on top if they overlap, though grids shouldn't overlap)
    grids.sort(key=lambda x: x['Temperature'] if x['Temperature'] is not None else -999)

    limit = LIMIT_DRAW if LIMIT_DRAW else total_grids
    print(f"🎨 Drawing {limit} grids...")
    
    for grid in tqdm(grids[:limit], desc="Drawing"):
        temp = grid['Temperature']
        is_real = grid['IsAnchor']
        
        # Get Color based on Temperature
        color = get_temp_color(temp)
        
        # Bounds logic
        bounds = get_grid_bounds_from_id(grid['GridID'])
        
        # Popup Text
        source_text = "REAL" if is_real else "SYNTHETIC"
        temp_text = f"{temp}°C" if temp is not None else "N/A"
        popup_html = f"<b>{grid['LocationName']}</b><br>Temp: {temp_text}<br>Source: {source_text}"

        if bounds:
            folium.Rectangle(
                bounds=bounds,
                color=color,       # Border color
                weight=1,
                fill=True,
                fill_color=color,  # Fill color
                fill_opacity=0.6,
                popup=folium.Popup(popup_html, max_width=200)
            ).add_to(m)

    # Add a Legend (Simple HTML overlay)
    legend_html = '''
     <div style="position: fixed; 
     bottom: 50px; left: 50px; width: 150px; height: 230px; 
     border:2px solid grey; z-index:9999; font-size:14px;
     background-color:white; opacity: 0.8;">
     &nbsp;<b>Temperature</b> <br>
     &nbsp;<i style="background:#e74c3c;width:10px;height:10px;display:inline-block;"></i>&nbsp; > 30°C<br>
     &nbsp;<i style="background:#e67e22;width:10px;height:10px;display:inline-block;"></i>&nbsp; 20-30°C<br>
     &nbsp;<i style="background:#f1c40f;width:10px;height:10px;display:inline-block;"></i>&nbsp; 20-25°C<br>
     &nbsp;<i style="background:#2ecc71;width:10px;height:10px;display:inline-block;"></i>&nbsp; 10-20°C<br>
     &nbsp;<i style="background:#00bfff;width:10px;height:10px;display:inline-block;"></i>&nbsp; 0-10°C<br>
     &nbsp;<i style="background:#0000ff;width:10px;height:10px;display:inline-block;"></i>&nbsp; -10-0°C<br>
     &nbsp;<i style="background:#00008b;width:10px;height:10px;display:inline-block;"></i>&nbsp; < -10°C<br>
     &nbsp;<i style="background:#2c003e;width:10px;height:10px;display:inline-block;"></i>&nbsp; < -20°C<br>
     </div>
     '''
    m.get_root().html.add_child(folium.Element(legend_html))

    output_file = "global_weather_temp_map.html"
    m.save(output_file)
    print(f"✅ Map generated: {output_file}")
    if total_grids > limit:
        print(f"⚠️  NOTE: Only showed {limit} out of {total_grids} grids.")

if __name__ == "__main__":
    generate_map_from_json()
