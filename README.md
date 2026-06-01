# Agent Pipeline RCA Framework

Databricks-jobs-only RCA framework for failed Databricks job runs.

The reusable framework is independent from the test workloads. Existing jobs integrate by adding a failure callback task that passes only the failed job id, failed run id, and optional Slack channel to the RCA analyzer job.

## Repository Layout

- `framework/rca_lib.py`: reusable evidence collection, prompt loading, LLM RCA generation, structured report writing, and Slack notification.
- `framework/rca_analyzer.py`: thin analyzer notebook that calls `run_rca_analysis(...)`.
- `framework/rca_failure_callback.py`: generic callback notebook that starts the analyzer job after an upstream task fails.
- `test/`: intentionally failing Databricks notebooks used to validate the framework against common pipeline failures.

## Runtime Flow

1. A monitored job task fails.
2. A downstream `rca_failure_callback` task runs with `run_if = AT_LEAST_ONE_FAILED`.
3. The callback triggers `agent_pipeline_rca_analyzer` with:
   - `source_job_id`
   - `source_run_id`
   - `slack_channel`
4. `rca_analyzer` calls `run_rca_analysis` from `rca_lib`.
5. The library derives failed task, task output, code path, table references, and table schemas from Databricks run metadata, logs, and source code.
6. The analyzer calls the configured model endpoint and writes a structured RCA report.
7. The analyzer sends a Slack message through `yipit_databricks_utils.helpers.notifications.send_slack_notification`.

## Databricks Assets

Workspace paths:

- Framework: `/Workspace/Users/asreesan@yipitdata.com/agent_pipeline_rca/framework`
- Test notebooks: `/Workspace/Users/asreesan@yipitdata.com/agent_pipeline_rca/test`

Jobs:

- RCA analyzer job: `64152640991709`
- Test jobs: see [Test Workloads](#test-workloads)

Tables:

- `yd_etl_dev.etl_sandbox.agent_pipeline_rca_events`
- `yd_etl_dev.etl_sandbox.agent_pipeline_rca_evidence`
- `yd_etl_dev.etl_sandbox.agent_pipeline_rca_reports`
- `yd_etl_dev.etl_sandbox.agent_pipeline_rca_prompts`

Test-only tables:

- `yd_etl_dev.etl_sandbox.agent_pipeline_rca_test_transactions`
- `yd_etl_dev.etl_sandbox.agent_pipeline_rca_test_transaction_features`
- `yd_etl_dev.etl_sandbox.agent_pipeline_rca_test_vendor_feed`
- `yd_etl_dev.etl_sandbox.agent_pipeline_rca_test_missing_table_output`

## RCA Report Table

`agent_pipeline_rca_reports` stores structured RCA fields:

- `error_type`
- `error_description`
- `likely_root_cause`
- `possible_fixes`
- `recommended_fix`
- `confidence`
- `evidence_json`
- `missing_context`
- `slack_channel_id`
- `slack_notification_status`
- `slack_message_ts`
- `slack_error`

Latest report query:

```sql
SELECT
  event_id,
  source_run_id,
  created_at,
  error_type,
  error_description,
  likely_root_cause,
  possible_fixes,
  recommended_fix,
  confidence,
  slack_channel_id,
  slack_notification_status,
  slack_message_ts,
  slack_error
FROM yd_etl_dev.etl_sandbox.agent_pipeline_rca_reports
ORDER BY created_at DESC
LIMIT 1;
```

## Integrating An Existing Job

Add a task after the task or task group you want to monitor:

- task notebook: `/Users/asreesan@yipitdata.com/agent_pipeline_rca/framework/rca_failure_callback`
- `run_if`: `AT_LEAST_ONE_FAILED`
- parameters:
  - `analyzer_job_id = 64152640991709`
  - `source_job_id = {{job.id}}`
  - `source_run_id = {{job.run_id}}`
  - `slack_channel = <channel name or id>`

The source job does not need to pass notebook paths, table names, failed task keys, or error details. The analyzer derives those from Databricks metadata and logs.

## Slack Configuration

The framework follows the same pattern as `central_card_workflow/card_feed/watchdog`:

```python
from yipit_databricks_utils.helpers.notifications import send_slack_notification
send_slack_notification(channel=slack_channel, text=text, blocks=blocks)
```

That helper uses the `WORKSPACE_CONFIGURATION/SLACK_CLIENT_TOKEN` Databricks secret and the Slack app `DB Alert`.

Slack setup requirements:

- Add the `DB Alert` Slack app to the target channel.
- Pass the Slack channel as the `slack_channel` job parameter. Channel names are preferred when using the helper, though channel ids can work when the app is already a member.

## Prompt Updates

The analyzer reads the active prompt from:

`yd_etl_dev.etl_sandbox.agent_pipeline_rca_prompts`

To roll out a prompt change, insert a new row with:

- `prompt_name = 'agent_pipeline_rca_default'`
- `prompt_alias = 'production'`
- `is_active = 'true'`
- a higher `prompt_version`
- updated `system_prompt` or `user_prompt_template`

Mark older rows inactive if only one prompt should be active.

## Test Workloads

The `test/` notebooks are intentionally failing workloads for validating model output on realistic pipeline issues. Each test should be run as its own Databricks job with the same failure callback task, so the analyzer receives one failed source run and can derive context from the run metadata and logs.

| Test job | Test notebook | Scenario | Expected model signal |
| --- | --- | --- | --- |
| `849463537683049` (`agent_pipeline_rca_test_schema_drift`) | `test/schema_drift_unresolved_column.py` | Source schema has `amount_usd`, downstream SQL still references `transaction_amount`. | Classify as schema drift or unresolved column; recommend updating the transform or adding a compatibility alias. |
| `909090987640198` (`agent_pipeline_rca_test_missing_table`) | `test/missing_table.py` | Pipeline reads a missing upstream Delta table. | Classify as missing table/dependency; recommend validating upstream materialization, table name, schema, and job dependency order. |
| `460414350986683` (`agent_pipeline_rca_test_data_quality_empty_input`) | `test/data_quality_empty_input.py` | Current-date partition has no rows and the pipeline raises a data quality failure. | Classify as data quality or empty input; recommend checking feed arrival, partition date logic, and upstream SLA. |
| `991517156600829` (`agent_pipeline_rca_test_python_import_error`) | `test/python_import_error.py` | Job imports a package that is not installed in the task environment. | Classify as dependency/import error; recommend adding the library to the job environment or fixing the import. |
| `219166312756087` (`agent_pipeline_rca_test_permission_denied`) | `test/permission_denied.py` | Job cannot read a required restricted upstream table. | Classify as permissions/access issue; recommend granting `USE CATALOG`, `USE SCHEMA`, and `SELECT`, or changing the run-as identity. |

The expected test workflow state is `SUCCESS_WITH_FAILURES`: the failure task fails by design and the RCA callback task succeeds.

## GitHub Sync

This repository can be pushed to GitHub and used directly by Databricks jobs through job-level Git source.

Create and push the GitHub repository:

```bash
git remote add origin git@github.com:<org>/agent_pipeline_rca.git
git push -u origin main
```

If using HTTPS instead of SSH:

```bash
git remote add origin https://github.com/<org>/agent_pipeline_rca.git
git push -u origin main
```

After the repo is pushed, configure each Databricks job to use Git source:

- `git_source.git_url = https://github.com/<org>/agent_pipeline_rca.git`
- `git_source.git_provider = gitHub`
- `git_source.git_branch = main`
- notebook task `source = GIT`
- notebook paths become repo-relative paths without `.py`

Path mapping:

| Workspace path | Git-source path |
| --- | --- |
| `/Users/asreesan@yipitdata.com/agent_pipeline_rca/framework/rca_analyzer` | `framework/rca_analyzer` |
| `/Users/asreesan@yipitdata.com/agent_pipeline_rca/framework/rca_failure_callback` | `framework/rca_failure_callback` |
| `/Users/asreesan@yipitdata.com/agent_pipeline_rca/test/schema_drift_unresolved_column` | `test/schema_drift_unresolved_column` |
| `/Users/asreesan@yipitdata.com/agent_pipeline_rca/test/missing_table` | `test/missing_table` |
| `/Users/asreesan@yipitdata.com/agent_pipeline_rca/test/data_quality_empty_input` | `test/data_quality_empty_input` |
| `/Users/asreesan@yipitdata.com/agent_pipeline_rca/test/python_import_error` | `test/python_import_error` |
| `/Users/asreesan@yipitdata.com/agent_pipeline_rca/test/permission_denied` | `test/permission_denied` |

Use `databricks jobs reset` or the Jobs UI to make this change. Preserve each job's existing parameters, run-as identity, task dependencies, and environment dependencies when switching from workspace source to Git source.
