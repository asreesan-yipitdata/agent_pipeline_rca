# Databricks notebook source
CATALOG = "yd_etl_dev"
SCHEMA = "etl_sandbox"
MISSING_TABLE = f"{CATALOG}.{SCHEMA}.agent_pipeline_rca_test_missing_daily_orders"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.agent_pipeline_rca_test_missing_table_output"

spark.sql(f"DROP TABLE IF EXISTS {MISSING_TABLE}")

# Real-world pattern: a pipeline expects an upstream ingestion table to exist,
# but the upstream job did not materialize the partition/table for the day.
spark.sql(
    f"""
    SELECT merchant_id, order_date, gross_sales
    FROM {MISSING_TABLE}
    WHERE order_date >= current_date() - 1
    """
).write.mode("overwrite").saveAsTable(TARGET_TABLE)
