import boto3
from boto3.dynamodb.conditions import Key
import math
import sys

# --- CONFIGURATION ---
USE_LOCAL_DB = False
TABLE_NAME = 'WeatherForecast'
GRID_DEG = 0.18

def get_db():
    """Connects to Local or Cloud DynamoDB based on flag"""
    if USE_LOCAL_DB:
        print("🔌 Connecting to LOCAL DynamoDB...")
        return boto3.resource('dynamodb', endpoint_url='http://localhost:8000', 
                              region_name='us-east-1',
                              aws_access_key_id='fake', aws_secret_access_key='fake')
    else:
        print("☁️ Connecting to REAL AWS DynamoDB...")
        return boto3.resource('dynamodb', region_name='us-east-1')

def fetch_weather_by_grid(table, grid_id):
    """
    Step 2: Given a GridID, fetch everything (Weather + City Metadata).
    """
    print(f"   🔎 Fetching all data for {grid_id}...")
    response = table.query(
        KeyConditionExpression=Key('GridID').eq(grid_id)
    )
    return response.get('Items', [])

def search_by_city(table, city_name):
    """
    Step 1 (City Mode): Query the GSI. Handles duplicates (Disambiguation).
    """
    print(f"   🔎 Searching Index for city: '{city_name}'...")
    
    response = table.query(
        IndexName='CityNameIndex',
        KeyConditionExpression=Key('LocationName').eq(city_name)
    )
    
    items = response.get('Items', [])
    
    # --- FILTERING STEP ---
    # Critical: The GSI returns ALL records with this name (Metadata + Weather).
    # We only want the 'CityLookup' metadata records for the menu.
    items = [i for i in items if i.get('Type') == 'CityLookup']

    if not items:
        return None, None
    
    # --- DISAMBIGUATION LOGIC ---
    if len(items) > 1:
        print(f"\n   ⚠️  Found {len(items)} cities named '{city_name}'. Please select:")
        # Widened formatting to include Country
        print(f"      {'No.':<4} | {'City (Country)':<35} | {'Population':<12} | {'Lat/Lon':<18} | {'Grid ID'}")
        print("-" * 100)
        
        # Sort by Population descending to show most likely candidate first
        items.sort(key=lambda x: float(x.get('Population', 0)), reverse=True)
        
        for i, item in enumerate(items):
            pop = f"{int(float(item.get('Population', 0))):,}"
            coords = f"{float(item['Lat']):.2f}, {float(item['Lon']):.2f}"
            
            # Retrieve Country (defaults to empty if running on old DB)
            country = item.get('Country', '')
            if country:
                display_name = f"{item['LocationName']} ({country})"
            else:
                display_name = item['LocationName']
                
            print(f"      {i+1:<4} | {display_name:<35} | {pop:<12} | {coords:<18} | {item['GridID']}")
            
        selection = input(f"\n   Select city (1-{len(items)}): ")
        try:
            idx = int(selection) - 1
            if 0 <= idx < len(items):
                return items[idx]['GridID'], items[idx]['LocationName']
        except ValueError:
            pass
        print("   ❌ Invalid selection. Defaulting to Option 1.")
        return items[0]['GridID'], items[0]['LocationName']
    
    return items[0]['GridID'], items[0]['LocationName']

def search_by_coords(lat, lon):
    """Step 1 (Coord Mode): Calculate GridID mathematically."""
    gx = math.floor(lon / GRID_DEG)
    gy = math.floor(lat / GRID_DEG)
    return f"GRID#{gx}#{gy}"

def display_results(items, searched_term=None):
    if not items:
        print("❌ No weather data found for this location.")
        return

    # --- SEPARATE DATA TYPES ---
    # 1. Metadata records (CityLookup)
    city_lookups = [i for i in items if i.get('Type') == 'CityLookup']
    
    # 2. Weather records (Hourly/Daily)
    hourlies = sorted([i for i in items if i.get('Type') == 'Hourly'], key=lambda x: x['Timestamp'])
    dailies = sorted([i for i in items if i.get('Type') == 'Daily'], key=lambda x: x['Timestamp'])
    
    if not hourlies and not dailies:
        print("❌ Grid found, but no WEATHER data exists (Generation Error?).")
        return

    # Use the first weather record to identify the "Anchor" city (The one providing the data)
    ref = hourlies[0] if hourlies else dailies[0]
    is_real = ref.get('IsAnchor', False)
    anchor_name = ref.get('LocationName', 'Unknown')
    anchor_country = ref.get('Country', '')
    if anchor_country:
        anchor_name = f"{anchor_name} ({anchor_country})"
    
    # --- HEADER REPORT ---
    print("\n" + "="*60)
    print(f"📍 WEATHER REPORT FOR GRID: {ref['GridID']}")
    print(f"   Primary Location: {anchor_name} (Grid Anchor)")
    print(f"   Data Source:      {'🟢 REAL TIME SENSOR' if is_real else '🔵 ESTIMATED (NEIGHBOR INTERPOLATION)'}")
    print("="*60)

    # --- TRANSPARENCY SECTION ---
    if city_lookups:
        total_cities = len(city_lookups)
        print(f"\n🏘️  GRID CONTEXT (Why am I seeing this?)")
        print(f"   This 20km x 20km grid contains {total_cities} registered locations.")
        print(f"   The weather is calculated based on the center of this grid.")
        
        # Sort cities by population to show the important ones
        city_lookups.sort(key=lambda x: float(x.get('Population', 0)), reverse=True)
        
        print(f"   Top cities in this grid:")
        for c in city_lookups[:5]:
            name = c['LocationName']
            country = c.get('Country', '')
            display_name = f"{name} ({country})" if country else name
            
            pop = int(float(c.get('Population', 0)))
            marker = " 📍 (You searched this)" if searched_term and name == searched_term else ""
            marker = " ⭐ (Anchor)" if c['LocationName'] == ref.get('LocationName') else marker
            print(f"   - {display_name} (Pop: {pop:,}){marker}")
            
        if total_cities > 5:
            print(f"   ... and {total_cities - 5} others.")

    # --- FORECAST SECTION ---
    if dailies:
        print("\n📅 7-DAY FORECAST:")
        print(f"   {'DATE':<12} | {'TEMP':<8} | {'RAIN %':<8} | {'WIND':<8}")
        print("-" * 50)
        for d in dailies:
            date_str = d['Timestamp'].split('T')[0]
            print(f"   {date_str:<12} | {d['Temperature']:>5} °C | {d['ChanceOfRain']:>5} % | {d['WindSpeed']:>5} km/h")

    if hourlies:
        print("\n🕒 HOURLY FORECAST (First 12 Hours):")
        for h in hourlies[:12]:
            time_str = h['Timestamp'].split('T')[1]
            print(f"   {time_str} -> Temp: {h['Temperature']}°C, Hum: {h['Humidity']}%, Rain: {h['ChanceOfRain']}%")
    print("\n")

def main():
    dynamodb = get_db()
    table = dynamodb.Table(TABLE_NAME)
    
    while True:
        print("\n--- WEATHER SYSTEM ---")
        print("1. Search by City Name")
        print("2. Search by Coordinates (Lat, Lon)")
        print("q. Quit")
        choice = input("Select option: ")

        if choice == '1':
            city = input("Enter city name (Case Sensitive): ").strip()
            
            # 1. GSI Lookup (returns tuple)
            grid_id, exact_name = search_by_city(table, city)
            
            if grid_id:
                # 2. Main Table Fetch
                data = fetch_weather_by_grid(table, grid_id)
                display_results(data, searched_term=exact_name)
            else:
                print(f"❌ City '{city}' not found in database.")

        elif choice == '2':
            try:
                lat = float(input("Enter Latitude (e.g., 35.68): "))
                lon = float(input("Enter Longitude (e.g., 139.69): "))
                
                # 1. Math Calculation
                grid_id = search_by_coords(lat, lon)
                print(f"   Calculated Grid: {grid_id}")
                
                # 2. Main Table Fetch
                data = fetch_weather_by_grid(table, grid_id)
                display_results(data)
            except ValueError:
                print("❌ Invalid coordinates.")

        elif choice.lower() == 'q':
            break

if __name__ == "__main__":
    main()
