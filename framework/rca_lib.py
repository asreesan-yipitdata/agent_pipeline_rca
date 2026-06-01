# Databricks notebook source
import json
import os
import re
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

CATALOG = "yd_etl_dev"
SCHEMA = "etl_sandbox"
EVENTS_TABLE = f"{CATALOG}.{SCHEMA}.agent_pipeline_rca_events"
EVIDENCE_TABLE = f"{CATALOG}.{SCHEMA}.agent_pipeline_rca_evidence"
REPORTS_TABLE = f"{CATALOG}.{SCHEMA}.agent_pipeline_rca_reports"
PROMPTS_TABLE = f"{CATALOG}.{SCHEMA}.agent_pipeline_rca_prompts"
DEFAULT_PROMPT_NAME = "agent_pipeline_rca_default"
DEFAULT_PROMPT_ALIAS = "production"
DEFAULT_MODEL_ENDPOINT = "gpt-4o-mini"
DEFAULT_SLACK_CHANNEL = "C0B6ZN6C9ST"
SLACK_TOKEN_ENV = "SLACK_BOT_TOKEN"
SLACK_TOKEN_SECRET_SCOPE_ENV = "RCA_SLACK_TOKEN_SECRET_SCOPE"
SLACK_TOKEN_SECRET_KEY_ENV = "RCA_SLACK_TOKEN_SECRET_KEY"
DEFAULT_SLACK_TOKEN_SECRET_SCOPE = "agent_pipeline_rca"
DEFAULT_SLACK_TOKEN_SECRET_KEY = "slack_bot_token"
SLACK_WEBHOOK_URL_ENV = "SLACK_WEBHOOK_URL"
SLACK_WEBHOOK_SECRET_SCOPE_ENV = "RCA_SLACK_WEBHOOK_SECRET_SCOPE"
SLACK_WEBHOOK_SECRET_KEY_ENV = "RCA_SLACK_WEBHOOK_SECRET_KEY"
DEFAULT_SLACK_WEBHOOK_SECRET_SCOPE = "agent_pipeline_rca"
DEFAULT_SLACK_WEBHOOK_SECRET_KEY = "slack_webhook_url"

DEFAULT_SYSTEM_PROMPT = """You are an engineering RCA assistant for Databricks job failures.
Use only the supplied evidence. Do not invent causes, owners, table names, or fixes.
If the evidence is incomplete, say what is missing and lower confidence.
Return only strict JSON."""

DEFAULT_USER_PROMPT_TEMPLATE = """Create a concise, engineering-ready RCA for the failed Databricks run.

Common issue patterns to check first:
- schema drift or unresolved columns
- missing table, missing file, or missing path
- permission denied or credential errors
- import, package, or library resolution errors
- SQL syntax, Python syntax, or notebook execution errors
- cluster startup, init script, runtime, or library install failures
- timeout, OOM, executor loss, shuffle, skew, or resource exhaustion
- upstream task, dependency, or run_job_task failure
- data quality expectation failures or unexpected empty/stale inputs
- external API, rate limit, network, or secret/configuration failures

Return a JSON object with these fields:
title, summary, failed_job, failed_task, primary_error, likely_root_cause,
error_type, error_description, possible_fixes, confidence, evidence,
recommended_fix, owner_next_steps, databricks_links, missing_context.

Rules:
- Cite concrete evidence from the evidence bundle.
- Prefer actionable fixes over generic debugging advice.
- Use "low" confidence when the root cause is inferred from limited evidence.
- Keep recommended_fix specific enough for an engineer to act on.

Context:
source_job_id={source_job_id}
source_run_id={source_run_id}
source_job_name={source_job_name}
failed_task_key={failed_task_key}
failed_task_run_id={failed_task_run_id}
discovered_code_paths={discovered_code_paths}
discovered_tables={discovered_tables}

Evidence bundle:
{evidence_bundle_json}
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def to_json(value: Any) -> str:
    try:
        if hasattr(value, "as_dict"):
            value = value.as_dict()
        return json.dumps(value, default=str, sort_keys=True)
    except Exception:
        return json.dumps({"repr": repr(value)}, default=str)


def sql_string(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def insert_row(spark, table: str, columns: dict[str, str | None]) -> None:
    column_names = ", ".join(columns.keys())
    values = ", ".join(sql_string(value) for value in columns.values())
    spark.sql(f"INSERT INTO {table} ({column_names}) VALUES ({values})")


def ensure_tables(spark) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {EVENTS_TABLE} (
          event_id STRING,
          source_job_id STRING,
          source_run_id STRING,
          source_job_name STRING,
          failed_task_key STRING,
          trigger_source STRING,
          payload_json STRING,
          created_at STRING
        ) USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {EVIDENCE_TABLE} (
          event_id STRING,
          source_job_id STRING,
          source_run_id STRING,
          collector_name STRING,
          evidence_json STRING,
          created_at STRING
        ) USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {REPORTS_TABLE} (
          event_id STRING,
          source_job_id STRING,
          source_run_id STRING,
          source_job_name STRING,
          failed_task_key STRING,
          failed_task_run_id STRING,
          error_type STRING,
          error_description STRING,
          likely_root_cause STRING,
          possible_fixes STRING,
          recommended_fix STRING,
          confidence STRING,
          evidence_json STRING,
          missing_context STRING,
          report_json STRING,
          created_at STRING,
          slack_channel_id STRING,
          slack_notification_status STRING,
          slack_message_ts STRING,
          slack_error STRING
        ) USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {PROMPTS_TABLE} (
          prompt_name STRING,
          prompt_alias STRING,
          prompt_version STRING,
          is_active STRING,
          model_endpoint STRING,
          temperature STRING,
          max_tokens STRING,
          system_prompt STRING,
          user_prompt_template STRING,
          notes STRING,
          created_at STRING,
          updated_at STRING
        ) USING DELTA
        """
    )


def seed_default_prompt(spark) -> None:
    rows = spark.sql(
        f"""
        SELECT prompt_version
        FROM {PROMPTS_TABLE}
        WHERE prompt_name = {sql_string(DEFAULT_PROMPT_NAME)}
          AND prompt_alias = {sql_string(DEFAULT_PROMPT_ALIAS)}
          AND lower(is_active) = 'true'
          AND cast(prompt_version AS INT) >= 2
        LIMIT 1
        """
    ).collect()
    if rows:
        return

    now = utc_now()
    insert_row(
        spark,
        PROMPTS_TABLE,
        {
            "prompt_name": DEFAULT_PROMPT_NAME,
            "prompt_alias": DEFAULT_PROMPT_ALIAS,
            "prompt_version": "2",
            "is_active": "true",
            "model_endpoint": DEFAULT_MODEL_ENDPOINT,
            "temperature": "0.1",
            "max_tokens": "1600",
            "system_prompt": DEFAULT_SYSTEM_PROMPT,
            "user_prompt_template": DEFAULT_USER_PROMPT_TEMPLATE,
            "notes": "Default RCA prompt for Databricks job failures.",
            "created_at": now,
            "updated_at": now,
        },
    )


def load_prompt(spark, prompt_name: str = DEFAULT_PROMPT_NAME, prompt_alias: str = DEFAULT_PROMPT_ALIAS) -> dict[str, Any]:
    seed_default_prompt(spark)
    rows = spark.sql(
        f"""
        SELECT *
        FROM {PROMPTS_TABLE}
        WHERE prompt_name = {sql_string(prompt_name)}
          AND prompt_alias = {sql_string(prompt_alias)}
          AND lower(is_active) = 'true'
        ORDER BY updated_at DESC, prompt_version DESC
        LIMIT 1
        """
    ).collect()
    if not rows:
        raise ValueError(f"No active RCA prompt found for {prompt_name}@{prompt_alias}")

    row = rows[0].asDict()
    row["temperature"] = float(row.get("temperature") or "0.1")
    row["max_tokens"] = int(row.get("max_tokens") or "1600")
    return row


def truncate(text: str | None, limit: int = 12000) -> str:
    if not text:
        return ""
    text = str(text)
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...[truncated]..."


def add_evidence(
    spark,
    event_id: str,
    source_job_id: str,
    source_run_id: str,
    collector_name: str,
    evidence: Any,
) -> dict[str, Any]:
    if hasattr(evidence, "as_dict"):
        evidence = evidence.as_dict()
    insert_row(
        spark,
        EVIDENCE_TABLE,
        {
            "event_id": event_id,
            "source_job_id": source_job_id,
            "source_run_id": source_run_id,
            "collector_name": collector_name,
            "evidence_json": to_json(evidence),
            "created_at": utc_now(),
        },
    )
    return {"collector_name": collector_name, "evidence": evidence}


def collect_run_output(w: WorkspaceClient, run_id: int) -> dict[str, Any]:
    try:
        output = w.jobs.get_run_output(run_id=run_id).as_dict()
        for key in ["notebook_output", "logs", "error"]:
            if key in output:
                output[key] = truncate(to_json(output[key]), 12000)
        return output
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}", "traceback": traceback.format_exc(limit=3)}


def find_failed_task(run_details: dict[str, Any]) -> dict[str, Any] | None:
    tasks = run_details.get("tasks", []) or []
    for task in tasks:
        state = task.get("state") or {}
        status = task.get("status") or {}
        termination = status.get("termination_details") or {}
        if (
            state.get("result_state") == "FAILED"
            or termination.get("type") == "CLIENT_ERROR"
            or termination.get("code") == "RUN_EXECUTION_ERROR"
        ):
            return task
    return tasks[0] if tasks else None


def find_task_settings(job_settings: dict[str, Any], task_key: str) -> dict[str, Any]:
    settings = job_settings.get("settings") or {}
    for task in settings.get("tasks", []) or []:
        if task.get("task_key") == task_key:
            return task
    return {}


def task_code_path(task: dict[str, Any]) -> str:
    if task.get("notebook_task"):
        return task["notebook_task"].get("notebook_path") or ""
    if task.get("spark_python_task"):
        return task["spark_python_task"].get("python_file") or ""
    if task.get("python_wheel_task"):
        return task["python_wheel_task"].get("package_name") or ""
    return ""


def collect_table_schema(spark, table_name: str) -> dict[str, Any]:
    if not table_name:
        return {"warning": "No discovered table provided"}
    try:
        columns = spark.sql(f"DESCRIBE TABLE {table_name}").collect()
        detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()
        history = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 5").collect()
        return {
            "table": table_name,
            "schema": [row.asDict() for row in columns],
            "detail": [row.asDict() for row in detail],
            "recent_history": [row.asDict() for row in history],
        }
    except Exception as exc:
        return {"table": table_name, "error": f"{type(exc).__name__}: {exc}"}


def collect_workspace_code(w: WorkspaceClient, path: str) -> dict[str, Any]:
    if not path:
        return {"warning": "No task code path discovered"}
    try:
        exported = w.workspace.export(path=path, format="SOURCE")
        content = exported.content
        if isinstance(content, bytes):
            content = content.decode("utf-8", errors="replace")
        return {"path": path, "source": truncate(content, 16000)}
    except Exception as exc:
        return {"path": path, "error": f"{type(exc).__name__}: {exc}"}


TABLE_REF_PATTERN = re.compile(r"`?([A-Za-z_][\w-]*)`?\.`?([A-Za-z_][\w-]*)`?\.`?([A-Za-z_][\w-]*)`?")


def discover_table_refs(*values: Any) -> list[str]:
    refs: set[str] = set()
    for value in values:
        text = to_json(value) if not isinstance(value, str) else value
        for catalog, schema, table in TABLE_REF_PATTERN.findall(text):
            refs.add(f"{catalog}.{schema}.{table}")
    return sorted(refs)


def summarize_error(output: dict[str, Any], failed_task: dict[str, Any] | None) -> dict[str, str]:
    text = to_json(output)
    state = (failed_task or {}).get("state") or {}
    status = (failed_task or {}).get("status") or {}
    termination = status.get("termination_details") or {}
    if "UNRESOLVED_COLUMN" in text:
        error_type = "UNRESOLVED_COLUMN"
    elif "PERMISSION_DENIED" in text or "PermissionDenied" in text:
        error_type = "PERMISSION_DENIED"
    elif "TABLE_OR_VIEW_NOT_FOUND" in text or "not found" in text.lower():
        error_type = "NOT_FOUND"
    elif termination.get("code"):
        error_type = str(termination.get("code"))
    elif state.get("result_state"):
        error_type = str(state.get("result_state"))
    else:
        error_type = "UNKNOWN"
    return {
        "error_type": error_type,
        "error_description": truncate(
            output.get("error")
            or output.get("notebook_output")
            or termination.get("message")
            or state.get("state_message")
            or text,
            3000,
        ),
    }


def extract_json_object(text: str, source_job_name: str, failed_task_key: str) -> dict[str, Any]:
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except Exception:
            pass
    return {
        "title": "Databricks job RCA",
        "summary": text[:2000],
        "failed_job": source_job_name,
        "failed_task": failed_task_key,
        "primary_error": "See collected evidence.",
        "error_type": "UNKNOWN",
        "error_description": "Model returned non-JSON text.",
        "likely_root_cause": "The model did not return valid JSON.",
        "confidence": "low",
        "evidence": ["Model returned non-JSON text."],
        "possible_fixes": ["Review evidence rows for this event and regenerate the RCA."],
        "recommended_fix": "Review evidence rows for this event and regenerate the RCA.",
        "owner_next_steps": ["Inspect failed_task_output and failure_code evidence."],
        "databricks_links": [],
        "missing_context": [],
    }


def render_user_prompt(prompt_row: dict[str, Any], context: dict[str, str], evidence_bundle: list[dict[str, Any]]) -> str:
    compact = [
        {
            "collector_name": item["collector_name"],
            "evidence_json_excerpt": truncate(to_json(item["evidence"]), 18000),
        }
        for item in evidence_bundle
    ]
    return prompt_row["user_prompt_template"].format(
        source_job_id=context["source_job_id"],
        source_run_id=context["source_run_id"],
        source_job_name=context["source_job_name"],
        failed_task_key=context["failed_task_key"],
        failed_task_run_id=context["failed_task_run_id"],
        discovered_code_paths=context["discovered_code_paths"],
        discovered_tables=context["discovered_tables"],
        evidence_bundle_json=json.dumps(compact, default=str, indent=2),
    )


def call_agent(
    w: WorkspaceClient,
    prompt_row: dict[str, Any],
    user_prompt: str,
    source_job_name: str,
    failed_task_key: str,
) -> dict[str, Any]:
    try:
        response = w.serving_endpoints.query(
            name=prompt_row["model_endpoint"],
            messages=[
                ChatMessage(role=ChatMessageRole.SYSTEM, content=prompt_row["system_prompt"]),
                ChatMessage(role=ChatMessageRole.USER, content=user_prompt),
            ],
            temperature=prompt_row["temperature"],
            max_tokens=prompt_row["max_tokens"],
        ).as_dict()
        content = ""
        if response.get("choices"):
            content = response["choices"][0].get("message", {}).get("content", "")
        elif response.get("predictions"):
            content = str(response["predictions"][0])
        elif response.get("outputs"):
            content = str(response["outputs"][0])
        return extract_json_object(content, source_job_name, failed_task_key)
    except Exception as exc:
        return {
            "title": "Databricks job failed: RCA model call failed",
            "summary": "Evidence was collected, but the serving endpoint call failed.",
            "failed_job": source_job_name,
            "failed_task": failed_task_key,
            "primary_error": f"{type(exc).__name__}: {exc}",
            "error_type": "MODEL_CALL_FAILED",
            "error_description": f"{type(exc).__name__}: {exc}",
            "likely_root_cause": "Unable to infer because the model endpoint call failed.",
            "confidence": "low",
            "evidence": ["See rca_evidence rows for this event_id."],
            "possible_fixes": ["Check serving endpoint permissions and retry RCA generation."],
            "recommended_fix": "Check serving endpoint permissions and retry RCA generation.",
            "owner_next_steps": ["Grant CAN QUERY on the serving endpoint to this job principal."],
            "databricks_links": [],
            "missing_context": ["Model response"],
        }


def get_slack_token() -> tuple[str | None, str]:
    token = os.getenv(SLACK_TOKEN_ENV)
    if token:
        return token, f"env:{SLACK_TOKEN_ENV}"

    candidates = [
        (
            os.getenv(SLACK_TOKEN_SECRET_SCOPE_ENV, DEFAULT_SLACK_TOKEN_SECRET_SCOPE),
            os.getenv(SLACK_TOKEN_SECRET_KEY_ENV, DEFAULT_SLACK_TOKEN_SECRET_KEY),
        ),
        ("slack", "bot_token"),
        ("slack", "slack_bot_token"),
        ("slack", "token"),
    ]
    errors = []
    for scope, key in candidates:
        try:
            if "dbutils" in globals():
                token = dbutils.secrets.get(scope=scope, key=key)
                if token:
                    return token, f"secret:{scope}/{key}"
        except Exception as exc:
            errors.append(f"{scope}/{key}: {type(exc).__name__}")
    return None, f"missing:{SLACK_TOKEN_ENV} or secrets:{', '.join(errors)}"


def get_slack_webhook_url() -> tuple[str | None, str]:
    url = os.getenv(SLACK_WEBHOOK_URL_ENV)
    if url:
        return url, f"env:{SLACK_WEBHOOK_URL_ENV}"

    candidates = [
        (
            os.getenv(SLACK_WEBHOOK_SECRET_SCOPE_ENV, DEFAULT_SLACK_WEBHOOK_SECRET_SCOPE),
            os.getenv(SLACK_WEBHOOK_SECRET_KEY_ENV, DEFAULT_SLACK_WEBHOOK_SECRET_KEY),
        ),
        ("slack", "webhook_url"),
        ("slack", "slack_webhook_url"),
    ]
    errors = []
    for scope, key in candidates:
        try:
            if "dbutils" in globals():
                url = dbutils.secrets.get(scope=scope, key=key)
                if url:
                    return url, f"secret:{scope}/{key}"
        except Exception as exc:
            errors.append(f"{scope}/{key}: {type(exc).__name__}")
    return None, f"missing:{SLACK_WEBHOOK_URL_ENV} or secrets:{', '.join(errors)}"


def slack_api_post(token: str, text: str, slack_channel: str) -> dict[str, str]:
    payload = {
        "channel": slack_channel,
        "text": text,
        "unfurl_links": False,
        "unfurl_media": False,
    }
    request = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return {
            "status": "failed",
            "channel_id": slack_channel,
            "message_ts": "",
            "error": f"HTTPError {exc.code}: {truncate(exc.read().decode('utf-8', errors='replace'), 1000)}",
        }
    except Exception as exc:
        return {
            "status": "failed",
            "channel_id": slack_channel,
            "message_ts": "",
            "error": f"{type(exc).__name__}: {exc}",
        }

    if body.get("ok"):
        return {
            "status": "sent",
            "channel_id": body.get("channel") or slack_channel,
            "message_ts": body.get("ts") or "",
            "error": "",
        }
    return {
        "status": "failed",
        "channel_id": slack_channel,
        "message_ts": "",
        "error": str(body.get("error") or body),
    }


def slack_webhook_post(webhook_url: str, text: str, slack_channel: str) -> dict[str, str]:
    payload = {"text": text}
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read().decode("utf-8", errors="replace")
            ok = 200 <= response.status < 300 and body.strip().lower() == "ok"
    except urllib.error.HTTPError as exc:
        return {
            "status": "failed",
            "channel_id": slack_channel,
            "message_ts": "",
            "error": f"Webhook HTTPError {exc.code}: {truncate(exc.read().decode('utf-8', errors='replace'), 1000)}",
        }
    except Exception as exc:
        return {
            "status": "failed",
            "channel_id": slack_channel,
            "message_ts": "",
            "error": f"Webhook {type(exc).__name__}: {exc}",
        }

    return {
        "status": "sent_webhook" if ok else "failed",
        "channel_id": slack_channel,
        "message_ts": "",
        "error": "" if ok else f"Unexpected webhook response: {truncate(body, 1000)}",
    }


def as_lines(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item) for item in parsed]
        except Exception:
            pass
        return [value] if value else []
    return [str(value)] if value is not None else []


def slack_message_text(context: dict[str, str], report: dict[str, Any]) -> str:
    fixes = as_lines(report.get("possible_fixes") or report.get("recommended_fix"))[:3]
    fixes_text = "\n".join(f"- {fix}" for fix in fixes) if fixes else "- Review RCA report table for details."
    links = as_lines(report.get("databricks_links"))[:2]
    links_text = "\n".join(links) if links else "No run link returned by RCA model."
    return f"""Databricks job RCA generated
Job: {context.get("source_job_name") or context.get("source_job_id")}
Run: {context.get("source_run_id")}
Failed task: {context.get("failed_task_key")}
Error type: {report.get("error_type", "UNKNOWN")}
Confidence: {report.get("confidence", "unknown")}

Error:
{truncate(str(report.get("error_description") or report.get("primary_error") or ""), 900)}

Likely root cause:
{truncate(str(report.get("likely_root_cause") or ""), 900)}

Possible fixes:
{fixes_text}

Links:
{links_text}
"""


def slack_message_blocks(context: dict[str, str], report: dict[str, Any]) -> list[dict[str, Any]]:
    fixes = as_lines(report.get("possible_fixes") or report.get("recommended_fix"))[:3]
    fixes_text = "\n".join(f"- {fix}" for fix in fixes) if fixes else "- Review RCA report table for details."
    links = as_lines(report.get("databricks_links"))[:2]
    links_text = "\n".join(links) if links else "No run link returned by RCA model."
    return [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Databricks job RCA generated",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*Job:* `{context.get('source_job_name') or context.get('source_job_id')}`\n"
                    f"*Run:* `{context.get('source_run_id')}`\n"
                    f"*Failed task:* `{context.get('failed_task_key')}`\n"
                    f"*Error type:* `{report.get('error_type', 'UNKNOWN')}`\n"
                    f"*Confidence:* `{report.get('confidence', 'unknown')}`"
                ),
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Likely root cause:*\n{truncate(str(report.get('likely_root_cause') or ''), 1400)}",
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Possible fixes:*\n{fixes_text}",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"*Links:* {links_text}",
                }
            ],
        },
    ]


def send_slack_via_yipit_helper(context: dict[str, str], report: dict[str, Any], slack_channel: str) -> dict[str, str]:
    from yipit_databricks_utils.helpers.notifications import send_slack_notification

    send_slack_notification(
        channel=slack_channel,
        text=slack_message_text(context, report),
        blocks=slack_message_blocks(context, report),
    )
    return {
        "status": "sent_yipit_helper",
        "channel_id": slack_channel,
        "message_ts": "",
        "error": "",
    }


def send_slack_notification(context: dict[str, str], report: dict[str, Any], slack_channel: str) -> dict[str, str]:
    slack_channel = slack_channel or DEFAULT_SLACK_CHANNEL
    try:
        return send_slack_via_yipit_helper(context, report, slack_channel)
    except Exception as exc:
        helper_error = f"yipit helper failed: {type(exc).__name__}: {exc}"
        if "not_in_channel" in helper_error:
            return {
                "status": "failed_not_in_channel",
                "channel_id": slack_channel,
                "message_ts": "",
                "error": helper_error,
            }
        if "channel_not_found" in helper_error:
            return {
                "status": "failed_channel_not_found",
                "channel_id": slack_channel,
                "message_ts": "",
                "error": helper_error,
            }

    text = slack_message_text(context, report)
    token, token_source = get_slack_token()
    if token:
        result = slack_api_post(token, text, slack_channel)
        result["error"] = result["error"] or token_source
        if result["status"] == "sent":
            result["error"] = ""
        else:
            result["error"] = f"{helper_error}; fallback {result['error']}"
        return result

    webhook_url, webhook_source = get_slack_webhook_url()
    if webhook_url:
        result = slack_webhook_post(webhook_url, text, slack_channel)
        result["error"] = result["error"] or webhook_source
        if result["status"] == "sent_webhook":
            result["error"] = ""
        else:
            result["error"] = f"{helper_error}; fallback {result['error']}"
        return result

    return {
        "status": "skipped_missing_slack_config",
        "channel_id": slack_channel,
        "message_ts": "",
        "error": f"{helper_error}; {token_source}; {webhook_source}",
    }


def run_rca_analysis(
    spark,
    source_job_id: str,
    source_run_id: str,
    slack_channel: str = DEFAULT_SLACK_CHANNEL,
    prompt_name: str = DEFAULT_PROMPT_NAME,
    prompt_alias: str = DEFAULT_PROMPT_ALIAS,
) -> dict[str, str]:
    if not source_job_id or not source_run_id:
        raise ValueError("source_job_id and source_run_id are required")

    ensure_tables(spark)
    w = WorkspaceClient()
    run_details = w.jobs.get_run(run_id=int(source_run_id)).as_dict()
    job_settings = w.jobs.get(job_id=int(source_job_id)).as_dict()
    source_job_name = run_details.get("run_name") or (job_settings.get("settings") or {}).get("name") or ""
    failed_task = find_failed_task(run_details) or {}
    failed_task_key = failed_task.get("task_key") or "unknown_task"
    failed_task_run_id = failed_task.get("run_id")
    task_settings = find_task_settings(job_settings, failed_task_key)
    failure_code_path = task_code_path(task_settings) or task_code_path(failed_task)
    event_id = f"{source_job_id}:{source_run_id}:{failed_task_key}:{int(datetime.now(timezone.utc).timestamp())}"
    context = {
        "source_job_id": source_job_id,
        "source_run_id": source_run_id,
        "source_job_name": source_job_name,
        "failed_task_key": failed_task_key,
        "failed_task_run_id": str(failed_task_run_id or ""),
        "failure_code_path": failure_code_path,
        "trigger_source": "job_failure_callback",
        "slack_channel": slack_channel or DEFAULT_SLACK_CHANNEL,
    }

    insert_row(
        spark,
        EVENTS_TABLE,
        {
            "event_id": event_id,
            "source_job_id": source_job_id,
            "source_run_id": source_run_id,
            "source_job_name": source_job_name,
            "failed_task_key": failed_task_key,
            "trigger_source": context["trigger_source"],
            "payload_json": to_json(context),
            "created_at": utc_now(),
        },
    )

    evidence_bundle = []
    evidence_bundle.append(add_evidence(spark, event_id, source_job_id, source_run_id, "job_run", run_details))
    evidence_bundle.append(add_evidence(spark, event_id, source_job_id, source_run_id, "job_settings", job_settings))
    evidence_bundle.append(add_evidence(spark, event_id, source_job_id, source_run_id, "failed_task_settings", task_settings))
    evidence_bundle.append(
        add_evidence(spark, event_id, source_job_id, source_run_id, "root_run_output", collect_run_output(w, int(source_run_id)))
    )

    if failed_task_run_id:
        failed_task_output = collect_run_output(w, int(failed_task_run_id))
        evidence_bundle.append(add_evidence(spark, event_id, source_job_id, source_run_id, "failed_task_output", failed_task_output))
    else:
        failed_task_output = {"warning": "Failed task run id not found"}
        evidence_bundle.append(
            add_evidence(spark, event_id, source_job_id, source_run_id, "failed_task_output", failed_task_output)
        )

    failure_code = collect_workspace_code(w, failure_code_path)
    evidence_bundle.append(add_evidence(spark, event_id, source_job_id, source_run_id, "failure_code", failure_code))

    discovered_tables = discover_table_refs(run_details, job_settings, failed_task_output, failure_code)
    context["discovered_code_paths"] = json.dumps([failure_code_path] if failure_code_path else [])
    context["discovered_tables"] = json.dumps(discovered_tables)
    for table_name in discovered_tables[:10]:
        evidence_bundle.append(
            add_evidence(
                spark,
                event_id,
                source_job_id,
                source_run_id,
                "discovered_table_schema",
                collect_table_schema(spark, table_name),
            )
        )

    error_summary = summarize_error(failed_task_output, failed_task)
    evidence_bundle.append(add_evidence(spark, event_id, source_job_id, source_run_id, "error_summary", error_summary))

    prompt_row = load_prompt(spark, prompt_name, prompt_alias)
    user_prompt = render_user_prompt(prompt_row, context, evidence_bundle)
    report = call_agent(w, prompt_row, user_prompt, source_job_name, failed_task_key)
    report["prompt_metadata"] = {
        "prompt_name": prompt_row["prompt_name"],
        "prompt_alias": prompt_row["prompt_alias"],
        "prompt_version": prompt_row["prompt_version"],
        "model_endpoint": prompt_row["model_endpoint"],
    }
    slack_result = send_slack_notification(context, report, context["slack_channel"])
    report["slack_notification"] = slack_result
    evidence_bundle.append(
        add_evidence(spark, event_id, source_job_id, source_run_id, "slack_notification", slack_result)
    )

    insert_row(
        spark,
        REPORTS_TABLE,
        {
            "event_id": event_id,
            "source_job_id": source_job_id,
            "source_run_id": source_run_id,
            "source_job_name": source_job_name,
            "failed_task_key": failed_task_key,
            "failed_task_run_id": str(failed_task_run_id or ""),
            "error_type": str(report.get("error_type") or error_summary["error_type"]),
            "error_description": str(report.get("error_description") or report.get("primary_error") or error_summary["error_description"]),
            "likely_root_cause": str(report.get("likely_root_cause") or ""),
            "possible_fixes": to_json(report.get("possible_fixes") or report.get("owner_next_steps") or []),
            "recommended_fix": str(report.get("recommended_fix") or ""),
            "confidence": str(report.get("confidence") or ""),
            "evidence_json": to_json(report.get("evidence") or []),
            "missing_context": to_json(report.get("missing_context") or []),
            "slack_channel_id": slack_result["channel_id"],
            "slack_notification_status": slack_result["status"],
            "slack_message_ts": slack_result["message_ts"],
            "slack_error": slack_result["error"],
            "report_json": to_json(report),
            "created_at": utc_now(),
        },
    )

    return {"event_id": event_id, "source_run_id": source_run_id}
