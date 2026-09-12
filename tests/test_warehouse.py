"""The single-writer properties, tested without Temporal.

These are the two guarantees the workflow leans on. If either breaks, the
orchestration above it is telling a comfortable lie.
"""

from __future__ import annotations

import subprocess
import sys

import duckdb
import pytest

from duckflow.duck import session, warehouse
from duckflow.shared import (
    DuckSettings,
    MetaBatch,
    PublishRequest,
    PublishTarget,
    RunRecord,
    StepMetric,
)

DATE = "2026-08-01"


def _parquet(lake: str, name: str, kwh: float, rows: int = 3) -> str:
    uri = session.ensure_writable(f"{lake}/gold/{name}/data.parquet")
    with session.connect(DuckSettings(threads=1, memory_limit="256MB"), s3=False) as con:
        con.execute(
            f"""
            COPY (
                SELECT DATE '{DATE}' AS business_date,
                       'r' || i::VARCHAR AS region,
                       {kwh} + i AS kwh
                FROM range({rows}) t(i)
            ) TO '{uri}' (FORMAT parquet)
            """
        )
    return uri


def _rows(path: str, table: str) -> list[tuple]:
    con = session.open_read_only(path)
    try:
        return con.execute(f"SELECT region, kwh FROM {table} ORDER BY region").fetchall()
    finally:
        con.close()


def test_publish_is_idempotent(lake, warehouse_path):
    """Temporal guarantees at-least-once, so the second attempt must be a no-op
    on the data -- not a doubling."""
    warehouse.ensure_schema()
    uri = _parquet(lake, "v1", 100.0)
    req = PublishRequest(
        run_id="run-1",
        targets=[PublishTarget(table="gold.t", source_uri=uri, business_date=DATE)],
    )

    first = warehouse.publish(req)
    second = warehouse.publish(req)

    assert first.total_rows == 3
    assert second.total_rows == 3
    assert len(_rows(warehouse_path, "gold.t")) == 3
    # And the retry must not record itself as its own predecessor, or
    # compensation would restore the broken state.
    assert second.tables[0].previous_run_id == ""


def test_compensation_restores_the_previous_run(lake, warehouse_path):
    warehouse.ensure_schema()
    good = _parquet(lake, "good", 100.0)
    bad = _parquet(lake, "bad", 999.0)

    warehouse.publish(PublishRequest(
        run_id="run-good",
        targets=[PublishTarget("gold.t", good, DATE)],
    ))
    warehouse.publish(PublishRequest(
        run_id="run-bad",
        targets=[PublishTarget("gold.t", bad, DATE)],
    ))
    assert _rows(warehouse_path, "gold.t")[0][1] == 999.0

    result = warehouse.compensate("run-bad", DATE, "quality gate failed")

    assert result.restored and not result.deleted
    assert _rows(warehouse_path, "gold.t")[0][1] == 100.0


def test_compensation_of_a_first_ever_publish_leaves_the_date_absent(lake, warehouse_path):
    warehouse.ensure_schema()
    uri = _parquet(lake, "only", 42.0)
    warehouse.publish(PublishRequest("run-1", [PublishTarget("gold.t", uri, DATE)]))

    result = warehouse.compensate("run-1", DATE, "boom")

    assert result.deleted and not result.restored
    assert _rows(warehouse_path, "gold.t") == []


def test_compensation_is_itself_idempotent(lake, warehouse_path):
    warehouse.ensure_schema()
    uri = _parquet(lake, "only", 42.0)
    warehouse.publish(PublishRequest("run-1", [PublishTarget("gold.t", uri, DATE)]))

    warehouse.compensate("run-1", DATE, "boom")
    again = warehouse.compensate("run-1", DATE, "boom")

    assert "nothing had been published" in again.note


def test_metadata_writes_upsert_rather_than_append(warehouse_path):
    warehouse.ensure_schema()
    batch = MetaBatch(
        run=RunRecord("r1", "wf1", DATE, "auto", "running", "2026-08-01 00:00:00"),
        step_metrics=[StepMetric(run_id="r1", step_key="bronze_readings", layer="bronze",
                                 state="succeeded", rows_out=10)],
    )
    warehouse.write_meta(batch)
    batch.run.status = "succeeded"
    batch.step_metrics[0].rows_out = 20
    warehouse.write_meta(batch)

    summary = warehouse.summary()
    assert summary["meta.runs"] == 1
    assert summary["meta.step_metrics"] == 1

    con = session.open_read_only(warehouse_path)
    try:
        assert con.execute("SELECT status FROM meta.runs").fetchone()[0] == "succeeded"
        assert con.execute("SELECT rows_out FROM meta.step_metrics").fetchone()[0] == 20
    finally:
        con.close()


def test_duckdb_refuses_any_other_process_while_a_writer_holds_the_file(warehouse_path):
    """The constraint the WRITER task queue exists to honour.

    Note what is actually refused: another *process*, and not only another
    writer -- a read-only open is refused too. That is why every read path in
    this repo goes through `session.open_read_only`, which backs off and retries
    instead of assuming the file is free.
    """
    warehouse.ensure_schema()
    held = duckdb.connect(warehouse_path)
    try:
        for read_only in (False, True):
            probe = subprocess.run(
                [sys.executable, "-c",
                 f"import duckdb; duckdb.connect({warehouse_path!r}, read_only={read_only})"],
                capture_output=True, text=True,
            )
            assert probe.returncode != 0
            assert "lock" in probe.stderr.lower()
    finally:
        held.close()


def test_two_connections_in_one_process_are_allowed_but_conflict_on_write(warehouse_path):
    """The nuance that makes `max_concurrent_activities=1` the right knob.

    DuckDB's lock is per process. Inside one process, connections share an
    instance and are *allowed* -- so a writer worker with concurrency raised
    would not hit the file lock at all. It would hit this instead: an optimistic
    transaction conflict, at commit time, on the row range two publishes of the
    same business date both try to replace.

    A retry would paper over it. Serialising the queue removes it.
    """
    warehouse.ensure_schema()
    a = duckdb.connect(warehouse_path)
    b = duckdb.connect(warehouse_path)   # allowed: same process
    try:
        a.execute("CREATE TABLE gold.t (business_date DATE, v INTEGER)")
        a.execute("INSERT INTO gold.t VALUES (DATE '2026-08-01', 1)")
        a.execute("BEGIN")
        b.execute("BEGIN")
        a.execute("DELETE FROM gold.t WHERE business_date = DATE '2026-08-01'")
        with pytest.raises(duckdb.Error, match="(?i)conflict"):
            b.execute("DELETE FROM gold.t WHERE business_date = DATE '2026-08-01'")
            b.execute("COMMIT")
    finally:
        a.close()
        b.close()
