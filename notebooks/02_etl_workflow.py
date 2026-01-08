# snowfall_pipeline/notebooks/02_etl_workflow.py
# MAGIC %pip install lxml  # Ensure dependency is available

import pandas as pd
from modules.clients import SnotelClient, NWSClient
from modules.database import upsert_to_delta, get_table_watermark
from modules.transformer import generate_combined_forecast
from config.settings import *

# 1. Setup Parameters
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"])
run_mode = dbutils.widgets.get("mode")

snotel = SnotelClient()
nws = NWSClient()

# 2. Determine Dates
if run_mode == "incremental":
    last_date = get_table_watermark(TBL_SNOW_OBS, "date")
    start_date = (last_date).strftime('%Y-%m-%d') if last_date else "2025-12-01"
    end_date = (pd.Timestamp.now() + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
else:
    start_date = "2025-12-01"
    end_date = pd.Timestamp.now().strftime('%Y-%m-%d')

# 3. Process Observations
stations = spark.table(TBL_WEATHER_STATIONS).toPandas()
all_obs = []
for _, row in stations.iterrows():
    data = snotel.fetch_historical_data(row['site_id'], row['ntwk'], start_date, end_date)
    if data is not None: all_obs.append(data)

if all_obs:
    obs_df = pd.concat(all_obs)
    # Note: add your transform_historical_data logic here
    upsert_to_delta(obs_df, TBL_SNOW_OBS, ["date", "site_id"], mode=run_mode)

# 4. Process Forecasts (Always upsert/refresh)
all_hourly = []
all_snow_grid = []
for _, row in stations.iterrows():
    all_hourly.append(nws.get_hourly_forecast(row['lat'], row['lon']).assign(site_id=row['site_id']))
    all_snow_grid.append(nws.get_snow_grid_data(row['lat'], row['lon']).assign(site_id=row['site_id']))

upsert_to_delta(pd.concat(all_hourly), TBL_WEATHER_FCST, ["startTime", "site_id"], mode="overwrite")
upsert_to_delta(pd.concat(all_snow_grid), TBL_SNOW_FCST, ["snow_start", "site_id"], mode="overwrite")

# 5. Aggregate (Efficient Spark Join)
generate_combined_forecast()
