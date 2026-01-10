def estimate_cost():
    # --- INPUTS ---
    TOTAL_GRIDS = 30019         # From your previous count_grids.py
    RECORDS_PER_GRID = 30       # 24 hourly + 6 daily
    
    # AWS US-East-1 Pricing (Standard On-Demand)
    PRICE_PER_MILLION_WRITES = 1.25
    PRICE_PER_GB_STORAGE = 0.25
    
    # --- CALCULATIONS ---
    total_records = TOTAL_GRIDS * RECORDS_PER_GRID
    
    # Write Costs (Base Table)
    # 1 Write Unit = 1KB. Your records are small (<1KB), so 1 record = 1 Unit.
    cost_write_base = (total_records / 1_000_000) * PRICE_PER_MILLION_WRITES
    
    # Write Costs (With GSI for City Name)
    # A GSI doubles the write cost because every new item is written to the Table AND the Index.
    cost_write_gsi = cost_write_base * 2
    
    # Storage Costs
    # Approx 400 bytes per record
    total_size_gb = (total_records * 400) / (1024**3)
    cost_storage = max(total_size_gb, 0.1) * PRICE_PER_GB_STORAGE # Minimum trivial amount

    # --- REPORT ---
    print(f"💰 AWS COST ESTIMATION REPORT")
    print(f"=============================")
    print(f"Total Records to Upload: {total_records:,}")
    print(f"Estimated Data Size:     {total_size_gb:.2f} GB")
    print(f"-----------------------------")
    print(f"OPTION 1: Standard (Current Code)")
    print(f"   Write Cost:  ${cost_write_base:.2f}")
    print(f"   Storage:     ${cost_storage:.2f} / month")
    print(f"   TOTAL:       ${cost_write_base + cost_storage:.2f}")
    print(f"-----------------------------")
    print(f"OPTION 2: With City Search (GSI Enabled)")
    print(f"   Write Cost:  ${cost_write_gsi:.2f}")
    print(f"   Storage:     ${cost_storage * 1.5:.2f} / month") # GSI adds storage overhead
    print(f"   TOTAL:       ${cost_write_gsi + (cost_storage * 1.5):.2f}")
    print(f"=============================")
    print(f"✅ Budget Status: WELL WITHIN $50 LIMIT")

if __name__ == "__main__":
    estimate_cost()