# Databricks notebook source
RESTRICTED_TABLE = "yd_sensitive_corporate.finance.payroll_transactions"

# Real-world pattern: a service principal or job owner lacks privileges on a
# sensitive catalog/table required by the pipeline.
try:
    spark.table(RESTRICTED_TABLE).limit(1).collect()
except Exception as exc:
    raise PermissionError(
        f"Permission denied while reading required upstream table {RESTRICTED_TABLE}: {exc}"
    ) from exc

raise PermissionError(
    f"Permission denied while reading required upstream table {RESTRICTED_TABLE}"
)
