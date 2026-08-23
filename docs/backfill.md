# Backfill Guide

Backfills intentionally reprocess historical daily intervals. They should be
bounded, monitored, and validated because they can increase BigQuery cost and
change historical BI results.

## Before Starting

1. Confirm raw partitions exist for every requested business date.
2. Estimate bytes scanned by the affected dbt models.
3. Notify BI owners if historical metrics may change.
4. Verify no overlapping scheduled or backfill run is active.
5. Choose the appropriate reprocessing behavior.

## Airflow 3 Command

Create missing and failed daily runs:

```bash
airflow backfill create \
  --dag-id northwind_daily_warehouse_refresh \
  --from-date 2026-07-01 \
  --to-date 2026-07-07 \
  --reprocess-behavior failed \
  --max-active-runs 1
```

Keep `max-active-runs` at one because the order fact uses incremental merges.
For a large historical correction, a controlled dbt full refresh may be safer
and cheaper than hundreds of daily merge runs.

## Validation

After completion:

- confirm every requested interval succeeded;
- reconcile raw and fact row counts;
- run dbt uniqueness and relationship tests;
- compare revenue and order totals before and after;
- record the reason, date range, and metric impact.

