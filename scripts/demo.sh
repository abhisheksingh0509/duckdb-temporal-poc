#!/usr/bin/env bash
# Guided tour. Every command it runs is one you can run yourself; nothing here
# is magic, and nothing here is hidden.
set -uo pipefail

C=$(printf '\033[36m'); B=$(printf '\033[1m'); N=$(printf '\033[0m')
CLI="docker compose exec -T worker-compute python -m duckflow.cli"

step() { echo; echo "${B}${C}== $1${N}"; echo "   $2"; echo; }
pause() { echo; read -rp "   [enter to continue] " _ </dev/tty || true; }

step "1. The DAG" "Steps are data, not code. This is the whole pipeline definition."
$CLI steps
pause

step "2. A clean run" "AUTO probes each step's input and sizes DuckDB for it."
$CLI run --date 2026-08-01 --meters 20000 --mode auto --no-approval --wait
pause

step "3. What landed" "Two gold tables, merged into warehouse.duckdb by the single writer."
$CLI gold
$CLI warehouse
pause

step "4. What it cost" "Per-step metrics, the profile AUTO chose, and every quality check."
$CLI report --date 2026-08-01 --limit 3
pause

step "5. The lake" "Every run's Parquet is still there, run-scoped and immutable."
$CLI lake
pause

step "6. A quality failure" "A negative tariff price. Non-retryable: the input cannot improve."
$CLI run --date 2026-10-01 --meters 500 --inject bad_tariff --no-approval --wait
echo
echo "   The run failed. Nothing was published, and what had been published was restored."
$CLI report --date 2026-10-01 --limit 2
pause

step "7. The single-writer rule" "What DuckDB refuses, and what it merely conflicts on."
$CLI contention
pause

step "8. Replacing a date" "Re-run 2026-08-01. DELETE+INSERT, so the row count does not move."
$CLI run --date 2026-08-01 --meters 20000 --no-approval --wait
$CLI query "select run_id, table_name, rows_deleted, rows_written, previous_run_id
            from meta.publish_log where business_date = date '2026-08-01'
            order by published_at"
echo
echo "${B}Done.${N} Temporal UI: http://localhost:8234   MinIO: http://localhost:9201"
echo "Next: LEARN.md for the concepts, ASSIGNMENTS.md to actually learn them."
