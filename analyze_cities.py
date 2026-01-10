import pandas as pd

# --- CONFIGURATION: TWEAK THESE NUMBERS ---
FILE_PATH = '/home-local/tockier/weather/database_cities/worldcities.csv'  # Your downloaded file name

# The column names in your specific CSV (Adjust if yours are different)
COL_POPULATION = 'population' 
COL_CITY_NAME = 'city_ascii'   # or 'city'

# The Classification Thresholds
LIMIT_MEDIUM = 300_000   # 500k
LIMIT_LARGE = 2_800_000  # 2.5M

def analyze_distribution():
    # 1. Load Data
    try:
        df = pd.read_csv(FILE_PATH)
        print(f"✅ Loaded {len(df)} cities.")
    except FileNotFoundError:
        print("❌ File not found. Please ensure 'worldcities.csv' is in the folder.")
        return

    # 2. Clean Data (Remove cities with no population data)
    df = df.dropna(subset=[COL_POPULATION])
    print(f"ℹ️  Analyzing {len(df)} cities with valid population data...\n")

    # 3. Apply Classification Logic
    def classify(pop):
        if pop > LIMIT_LARGE:
            return 'Tier 3 (Large)', 9  # 9 grids
        elif pop > LIMIT_MEDIUM:
            return 'Tier 2 (Medium)', 5 # 5 grids
        else:
            return 'Tier 1 (Small)', 1  # 1 grid

    # Apply the function to create new columns
    df[['Class', 'Grid_Count']] = df[COL_POPULATION].apply(
        lambda x: pd.Series(classify(x))
    )

    # 4. Generate Report
    stats = df.groupby('Class').agg(
        City_Count=('Class', 'count'),
        Total_Grids=('Grid_Count', 'sum'),
        Min_Pop=(COL_POPULATION, 'min'),
        Max_Pop=(COL_POPULATION, 'max')
    )

    # Calculate Totals
    total_grids = stats['Total_Grids'].sum()
    total_cities = stats['City_Count'].sum()
    
    # 5. Print Results
    print("--- 📊 CITY DISTRIBUTION ANALYSIS ---")
    print(f"Thresholds :: Medium > {LIMIT_MEDIUM:,} | Large > {LIMIT_LARGE:,}\n")
    print(stats.to_string())
    print("-" * 60)
    print(f"🌍 TOTAL CITIES: {total_cities:,}")
    print(f"📦 TOTAL GRIDS TO GENERATE: {total_grids:,}")
    
    # Cost Estimation (Assuming 1 week of hourly data = 168 records per grid)
    total_records = total_grids * 30
    print(f"📝 TOTAL RECORDS (1 Week): {total_records:,}")
    
    # Write Write Cost Estimate ($1.25 per million)
    cost = (total_records / 1_000_000) * 1.25
    print(f"💰 ESTIMATED WRITE COST: ${cost:.2f}")

if __name__ == "__main__":
    analyze_distribution()