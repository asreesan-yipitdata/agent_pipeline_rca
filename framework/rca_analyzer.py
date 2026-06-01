# Databricks notebook source
# MAGIC %run ./rca_lib

# COMMAND ----------

def widget(name: str, default: str = "") -> str:
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name).strip()


result = run_rca_analysis(
    spark=spark,
    source_job_id=widget("source_job_id"),
    source_run_id=widget("source_run_id"),
    slack_channel=widget("slack_channel", DEFAULT_SLACK_CHANNEL),
)

print(f"RCA report written for source_run_id={result['source_run_id']} event_id={result['event_id']}")
