# Databricks notebook source
from pyspark.sql import functions as F

CATALOG = "yd_etl_dev"
SCHEMA = "etl_sandbox"
SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.agent_pipeline_rca_test_transactions"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

rows = [
    ("txn_001", "Acme Coffee", 18.45, "2026-05-30"),
    ("txn_002", "Northwind Market", 72.10, "2026-05-30"),
    ("txn_003", "Contoso Fuel", 44.85, "2026-05-31"),
]

df = spark.createDataFrame(
    rows,
    "transaction_id STRING, merchant_name STRING, amount_usd DOUBLE, transaction_date STRING",
).withColumn("transaction_date", F.to_date("transaction_date"))

df.write.mode("overwrite").saveAsTable(SOURCE_TABLE)

print(f"Created source table: {SOURCE_TABLE}")
spark.table(SOURCE_TABLE).printSchema()

# Real-world pattern: a source schema changed from transaction_amount to amount_usd,
# but the downstream feature transform still references the old column name.
spark.sql(
    f"""
    SELECT
      transaction_id,
      merchant_name,
      transaction_amount * 100 AS amount_cents,
      transaction_date
    FROM {SOURCE_TABLE}
    """
).write.mode("overwrite").saveAsTable(
    f"{CATALOG}.{SCHEMA}.agent_pipeline_rca_test_transaction_features"
)
