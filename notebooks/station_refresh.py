# MAGIC %pip install lxml

import pandas as pd
from modules.clients import SnotelClient
from modules.database import upsert_to_delta
from config.settings import TBL_WEATHER_STATIONS, DEFAULT_STATE

# Initialize the client
snotel = SnotelClient()

print(f"Refreshing station inventory for state: {DEFAULT_STATE}...")

# 1. Fetch site inventory from USDA/SNOTEL
# This pulls names, IDs, and network types (sntl, snow)
target_sites = snotel.get_site_inventory(state=DEFAULT_STATE)

if not target_sites.empty:

    upsert_to_delta(
        df=target_sites, 
        table_name=TBL_WEATHER_STATIONS, 
        join_cols=["site_id"], 
        mode="upsert"
    )
    
    print(f"Successfully refreshed {len(target_sites)} stations in {TBL_WEATHER_STATIONS}.")
else:
    print("No stations found. Check API connectivity or state code.")
