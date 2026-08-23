# Operations Runbook

## Local Startup

The sibling `customer-analytics-warehouse` repository must be next to this
repository because Compose mounts it at `/opt/airflow/dbt`.

```bash
cp ../customer-analytics-warehouse/profiles.yml.example \
   ../customer-analytics-warehouse/profiles.yml
# Configure the GCP project and authenticate.
printf 'AIRFLOW_UID=%s\n' "$(id -u)" > .env
make init
make up
```

Open `http://localhost:8080`, sign in as the local `admin` user, and enable
`northwind_daily_warehouse_refresh`.

## Daily Success Criteria

A successful run confirms:

1. all three raw sources are marked ready in the manifest;
2. the manifest is within the configured freshness window;
3. dbt source freshness passes;
4. staging and core warehouse models build;
5. finance and marketing marts build;
6. all mart tests pass;
7. the structured completion summary is logged.

## Failure Triage

| Failed task | First checks |
| --- | --- |
| `validate_raw_delivery` | File existence, timestamp, source statuses, row counts |
| `check_source_freshness` | Upstream load time and BigQuery raw tables |
| `build_staging` | Source schema drift and type conversion |
| `build_core` | Incremental merge, key relationships, BigQuery quota |
| BI mart task | Compiled SQL and changed business logic |
| `validate_warehouse` | Failing record set and affected dashboard metrics |

The structured failure log includes DAG, task, run, attempt, exception, and log
URL. Production should forward this payload to Slack or PagerDuty.

## Retry and Rerun

Airflow retries transient failures twice with exponential backoff. After fixing
a persistent issue, clear only the failed task and its downstream tasks. Do not
clear successful upstream extraction checks unless the raw delivery changed.

## Common Commands

```bash
make logs
make dag-list
make dag-test
docker compose ps
docker compose down
```

## Credentials

Never place GCP credentials in the repository. For local development, use
Application Default Credentials. Production should use workload identity or an
Airflow secret backend and grant only the BigQuery permissions needed by dbt.

