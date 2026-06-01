# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "yd_etl_dev"
SCHEMA = "etl_sandbox"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.agent_pipeline_rca_test_vendor_feed"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

rows = [
    ("vendor_a", "2026-05-30", 100),
    ("vendor_b", "2026-05-30", 200),
]

df = spark.createDataFrame(rows, "vendor_id STRING, feed_date STRING, row_count INT").withColumn(
    "feed_date", F.to_date("feed_date")
)
df.write.mode("overwrite").saveAsTable(SOURCE_TABLE)

# Real-world pattern: an expected daily feed partition is empty/stale. The job
# raises a data quality error before publishing incomplete downstream data.
today_count = spark.sql(
    f"""
    SELECT count(*) AS cnt
    FROM {SOURCE_TABLE}
    WHERE feed_date = current_date()
    """
).collect()[0]["cnt"]

if today_count == 0:
    raise ValueError(
        f"Data quality check failed: expected current_date rows in {SOURCE_TABLE}, found 0. "
        "Likely stale or missing upstream vendor feed."
    )
