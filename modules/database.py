from pyspark.sql import SparkSession, functions as F
from delta.tables import DeltaTable

def get_spark():
    return SparkSession.builder.getOrCreate()

def get_table_watermark(table_name, date_col):
    """Finds the max date in the table to determine incremental start."""
    spark = get_spark()
    if spark.catalog.tableExists(table_name):
        res = spark.table(table_name).select(F.max(date_col)).collect()[0][0]
        return res
    return None

def upsert_to_delta(df, table_name, join_cols, mode="upsert"):
    """Performs Delta Merge for incremental or Overwrite for full refresh."""
    spark = get_spark()
    if df.empty: return
    
    spark_df = spark.createDataFrame(df)
    
    # Full Refresh Mode
    if mode == "overwrite" or not spark.catalog.tableExists(table_name):
        spark_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)
        return

    # Incremental Mode (Merge)
    target_table = DeltaTable.forName(spark, table_name)
    join_cond = " AND ".join([f"t.{c} = s.{c}" for c in join_cols])
    
    target_table.alias("t").merge(
        spark_df.alias("s"), join_cond
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
