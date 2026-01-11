import folium
import json
import os
from tqdm import tqdm

# --- CONFIGURATION ---
INPUT_FILE = '../weather_data/world_weather_final.json'
GRID_DEG = 0.18
LIMIT_DRAW = None  # Set to None for full map

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

def get_precip_color(precip):
    """Returns a color hex code based on precipitation (mm)"""
    if precip is None:
        return "#808080" # Gray for no data
    
    if precip <= 0.1: return "#ecf0f1" # Off White/Gray (Dry)
    if precip < 2:    return "#aed6f1" # Very Light Blue (Drizzle)
    if precip < 5:    return "#5dade2" # Light Blue (Light Rain)
    if precip < 10:   return "#2e86c1" # Blue (Moderate)
    if precip < 20:   return "#1b4f72" # Dark Blue (Heavy)
    return "#8e44ad"                   # Purple (Violent/Storm)

def generate_precip_map_from_json():
    print(f"🚀 Loading {INPUT_FILE} (this might take a moment)...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)

    print(f"   Loaded {len(data)} records. Extracting unique grids...")
    
    unique_grids = {}
    
    # SCAN DATA: Find records with 'Precipitation'
    for item in tqdm(data, desc="Processing"):
        gid = item.get('GridID')
        
        has_precip = 'Precipitation' in item
        
        # 1. New Grid found
        if gid not in unique_grids:
            unique_grids[gid] = {
                'GridID': gid,
                'LocationName': item.get('LocationName', 'Unknown'),
                'Lat': float(item['Lat']),
                'Lon': float(item['Lon']),
                'IsAnchor': item.get('IsAnchor', False),
                'Precipitation': float(item['Precipitation']) if has_precip else None
            }
        
        # 2. Existing Grid, update if we found precip data and didn't have it before
        elif unique_grids[gid]['Precipitation'] is None and has_precip:
            unique_grids[gid]['Precipitation'] = float(item['Precipitation'])

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

    # Sort: Draw heavy rain on top if overlap occurs (unlikely for grids, but good practice)
    grids.sort(key=lambda x: x['Precipitation'] if x['Precipitation'] is not None else -1)

    limit = LIMIT_DRAW if LIMIT_DRAW else total_grids
    print(f"🎨 Drawing {limit} grids...")
    
    for grid in tqdm(grids[:limit], desc="Drawing"):
        precip = grid['Precipitation']
        is_real = grid['IsAnchor']
        
        color = get_precip_color(precip)
        
        bounds = get_grid_bounds_from_id(grid['GridID'])
        
        source_text = "REAL" if is_real else "SYNTHETIC"
        precip_text = f"{precip} mm" if precip is not None else "N/A"
        popup_html = f"<b>{grid['LocationName']}</b><br>Rain: {precip_text}<br>Source: {source_text}"

        if bounds:
            folium.Rectangle(
                bounds=bounds,
                color=color,
                weight=1,
                fill=True,
                fill_color=color,
                fill_opacity=0.6,
                popup=folium.Popup(popup_html, max_width=200)
            ).add_to(m)

    # Precipitation Legend
    legend_html = '''
     <div style="position: fixed; 
     bottom: 50px; left: 50px; width: 150px; height: 180px; 
     border:2px solid grey; z-index:9999; font-size:14px;
     background-color:white; opacity: 0.8;">
     &nbsp;<b>Precipitation</b> <br>
     &nbsp;<i style="background:#8e44ad;width:10px;height:10px;display:inline-block;"></i>&nbsp; > 20 mm<br>
     &nbsp;<i style="background:#1b4f72;width:10px;height:10px;display:inline-block;"></i>&nbsp; 10-20 mm<br>
     &nbsp;<i style="background:#2e86c1;width:10px;height:10px;display:inline-block;"></i>&nbsp; 5-10 mm<br>
     &nbsp;<i style="background:#5dade2;width:10px;height:10px;display:inline-block;"></i>&nbsp; 2-5 mm<br>
     &nbsp;<i style="background:#aed6f1;width:10px;height:10px;display:inline-block;"></i>&nbsp; 0.1-2 mm<br>
     &nbsp;<i style="background:#ecf0f1;width:10px;height:10px;display:inline-block;border:1px solid #ccc;"></i>&nbsp; Dry<br>
     </div>
     '''
    m.get_root().html.add_child(folium.Element(legend_html))

    output_file = "global_weather_precip_map.html"
    m.save(output_file)
    print(f"✅ Map generated: {output_file}")
    if total_grids > limit:
        print(f"⚠️  NOTE: Only showed {limit} out of {total_grids} grids.")

if __name__ == "__main__":
    generate_precip_map_from_json()
