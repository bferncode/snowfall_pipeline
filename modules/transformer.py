from pyspark.sql import functions as F
from modules.database import get_spark
from config.settings import TBL_WEATHER_FCST, TBL_SNOW_FCST, TBL_COMBINED_FCST

def generate_combined_forecast():
    """Joins hourly and snow tables already in Delta to create the gold summary."""
    spark = get_spark()
    
    weather = spark.table(TBL_WEATHER_FCST).alias("w")
    snow = spark.table(TBL_SNOW_FCST).alias("s")
    
    # Spark Join: Weather record is within the Snow window
    combined = snow.join(
        weather,
        (F.col("w.site_id") == F.col("s.site_id")) & 
        (F.col("w.startTime") >= F.col("s.snow_start")) & 
        (F.col("w.startTime") < F.col("s.snow_end")),
        "left"
    ).groupBy(
        "s.site_id", "s.snow_start", "s.snow_end", "s.total_snow_in"
    ).agg(
        F.round(F.avg("temperature"), 1).alias("avg_temp"),
        F.max("probabilityOfPrecipitation.value").alias("max_pop"),
        F.concat_ws(", ", F.collect_set("shortForecast")).alias("conditions")
    )
    
    combined.write.format("delta").mode("overwrite").saveAsTable(TBL_COMBINED_FCST)

def transform_historical_data(pd_df):
    """
    Standardizes column names and adds date part columns 
    for the historical snow data.
    """
    pd_df.columns = pd_df.columns.str.strip().str.lower()

    # Update map to match cleaned names
    col_map = {
        'date': 'date_hr',
        'station id': 'site_id',
        'station name': 'site_name',
        'snow depth (in)': 'snow_depth_in',
        'snow water equivalent (in)': 'swe_in'
    }
    
    # Rename only columns that exist
    pd_df = pd_df.rename(columns=col_map)
    
    # Date processing
    pd_df['date_hr'] = pd.to_datetime(pd_df['date_hr'])
    pd_df['date'] = pd_df['date_hr'].dt.date
    pd_df['year'] = pd_df['date_hr'].dt.year
    pd_df['month'] = pd_df['date_hr'].dt.month
    pd_df['day'] = pd_df['date_hr'].dt.day
    pd_df['hour'] = pd_df['date_hr'].dt.hour
    
    return pd_df
