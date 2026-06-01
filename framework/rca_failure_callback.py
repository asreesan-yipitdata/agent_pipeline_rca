# Databricks notebook source
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("analyzer_job_id", "")
dbutils.widgets.text("source_job_id", "")
dbutils.widgets.text("source_run_id", "")
dbutils.widgets.text("slack_channel", "C0B6ZN6C9ST")

analyzer_job_id = dbutils.widgets.get("analyzer_job_id").strip()
if not analyzer_job_id:
    raise ValueError("Missing required widget: analyzer_job_id")

params = {
    "source_job_id": dbutils.widgets.get("source_job_id"),
    "source_run_id": dbutils.widgets.get("source_run_id"),
    "slack_channel": dbutils.widgets.get("slack_channel"),
}

print("Triggering RCA analyzer job")

w = WorkspaceClient()
run = w.jobs.run_now(job_id=int(analyzer_job_id), notebook_params=params)
print(f"Started RCA analyzer run_id={run.run_id}")
