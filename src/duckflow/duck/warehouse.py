"""The single-writer warehouse.

Everything in this module runs on the WRITER task queue, in a worker configured
with `max_concurrent_activities=1`. That is not tidiness, it is the storage
contract: DuckDB permits one read-write process per database file, full stop.

Two properties are load-bearing and both come from Temporal's execution model
rather than from DuckDB:

*Idempotence.* Temporal guarantees an activity runs **at least** once. A worker
that finishes a publish and then dies before reporting completion will be asked
to publish again. So every write here is expressed as "make the world look like
this", never "add this": `DELETE` the business date and `INSERT` it, upsert the
metadata rows on their primary keys. Running any of it twice is a no-op.

*Reversibility.* `publish_log` records, for each (table, date), the run that
owned it before this one and the Parquet URI that run wrote. Compensation is
then a re-publish of that URI, which works because step outputs are immutable
and run-scoped. Plain Parquet has no time travel; this table is the substitute.
"""

from __future__ import annotations

import logging
from typing import Iterable

import duckdb

from duckflow.config import settings
from duckflow.duck import session
from duckflow.shared import (
    CompensateResult,
    DuckSettings,
    MetaBatch,
    PublishedTable,
    PublishRequest,
    PublishResult,
)
from duckflow.steps import PUBLISHED, dataset

log = logging.getLogger("duckflow.warehouse")

WRITER_DUCK = DuckSettings(threads=2, memory_limit="1GB", temp_directory=True)

_DDL = """
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.runs (
    run_id          VARCHAR PRIMARY KEY,
    workflow_id     VARCHAR,
    business_date   DATE,
    mode            VARCHAR,
    status          VARCHAR,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    rows_published  BIGINT,
    error           VARCHAR
);

CREATE TABLE IF NOT EXISTS meta.step_metrics (
    run_id           VARCHAR,
    step_key         VARCHAR,
    layer            VARCHAR,
    state            VARCHAR,
    attempts         INTEGER,
    rows_in          BIGINT,
    rows_out         BIGINT,
    rows_rejected    BIGINT,
    duration_seconds DOUBLE,
    bytes_out        BIGINT,
    threads          INTEGER,
    memory_limit     VARCHAR,
    sla_seconds      INTEGER,
    sla_breached     BOOLEAN,
    output_uri       VARCHAR,
    error            VARCHAR,
    PRIMARY KEY (run_id, step_key)
);

CREATE TABLE IF NOT EXISTS meta.dq_results (
    run_id     VARCHAR,
    step_key   VARCHAR,
    check_name VARCHAR,
    kind       VARCHAR,
    dataset    VARCHAR,
    column_name VARCHAR,
    severity   VARCHAR,
    passed     BOOLEAN,
    observed   VARCHAR,
    expected   VARCHAR,
    detail     VARCHAR,
    PRIMARY KEY (run_id, step_key, check_name)
);

CREATE TABLE IF NOT EXISTS meta.lineage (
    run_id     VARCHAR,
    step_key   VARCHAR,
    upstream   VARCHAR,
    downstream VARCHAR,
    columns    VARCHAR,
    PRIMARY KEY (run_id, step_key, upstream, downstream)
);

CREATE TABLE IF NOT EXISTS meta.publish_log (
    run_id              VARCHAR,
    table_name          VARCHAR,
    business_date       DATE,
    source_uri          VARCHAR,
    rows_deleted        BIGINT,
    rows_written        BIGINT,
    previous_run_id     VARCHAR,
    previous_source_uri VARCHAR,
    published_at        TIMESTAMP,
    compensated         BOOLEAN DEFAULT false,
    PRIMARY KEY (run_id, table_name, business_date)
);
"""


def _writer(path: str | None = None):
    cfg = settings()
    return session.connect(WRITER_DUCK, database=path or cfg.warehouse_path, read_only=False)


def ensure_schema(path: str | None = None) -> None:
    with _writer(path) as con:
        con.execute(_DDL)


def _ensure_gold_table(con: duckdb.DuckDBPyConnection, table: str, source_uri: str) -> None:
    """Create the target from the Parquet schema on first publish.

    `LIMIT 0` gives the columns and types without reading a data page. The
    alternative -- hand-written DDL duplicated from steps.py -- drifts the first
    time someone adds a column to a gold SELECT.
    """
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {table} AS "
        f"SELECT * FROM read_parquet('{source_uri}') LIMIT 0"
    )


def publish(req: PublishRequest) -> PublishResult:
    result = PublishResult(run_id=req.run_id)
    with _writer() as con:
        con.execute(_DDL)
        for target in req.targets:
            table = target.table
            with _transaction(con):
                _ensure_gold_table(con, table, target.source_uri)

                # Whoever owned this date before us -- excluding ourselves, so a
                # retried publish does not record itself as its own predecessor
                # and make compensation a no-op.
                prev = con.execute(
                    f"""
                    SELECT run_id, source_uri
                    FROM meta.publish_log
                    WHERE table_name = ? AND business_date = ?
                      AND run_id <> ? AND NOT compensated
                    ORDER BY published_at DESC
                    LIMIT 1
                    """,
                    [table, target.business_date, req.run_id],
                ).fetchone()
                prev_run, prev_uri = (prev[0], prev[1]) if prev else ("", "")

                deleted = con.execute(
                    f"DELETE FROM {table} WHERE business_date = ?", [target.business_date]
                ).fetchone()
                rows_deleted = int(deleted[0]) if deleted else 0

                # BY NAME tolerates column reordering between runs, which will
                # happen the first time someone edits a gold SELECT.
                con.execute(
                    f"INSERT INTO {table} BY NAME "
                    f"SELECT * FROM read_parquet('{target.source_uri}')"
                )
                written = con.execute(
                    f"SELECT count(*) FROM {table} WHERE business_date = ?",
                    [target.business_date],
                ).fetchone()
                rows_written = int(written[0]) if written else 0

                con.execute(
                    """
                    INSERT OR REPLACE INTO meta.publish_log
                        (run_id, table_name, business_date, source_uri, rows_deleted,
                         rows_written, previous_run_id, previous_source_uri,
                         published_at, compensated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, now()::TIMESTAMP, false)
                    """,
                    [req.run_id, table, target.business_date, target.source_uri,
                     rows_deleted, rows_written, prev_run, prev_uri],
                )

            result.tables.append(
                PublishedTable(
                    table=table,
                    business_date=target.business_date,
                    rows_deleted=rows_deleted,
                    rows_written=rows_written,
                    previous_run_id=prev_run,
                    previous_source_uri=prev_uri,
                )
            )
            result.total_rows += rows_written
            log.info(
                "published %s dt=%s: -%d +%d (previous run %s)",
                table, target.business_date, rows_deleted, rows_written, prev_run or "none",
            )
    return result


def compensate(run_id: str, business_date: str, reason: str) -> CompensateResult:
    """Undo this run's publishes, table by table.

    Restoring means re-inserting the *previous* run's Parquet, which is still
    sitting untouched at its own run-scoped URI. If there was no previous run
    for this date, the correct undo is to leave the date absent rather than to
    leave half of it behind.
    """
    out = CompensateResult(note=reason)
    with _writer() as con:
        con.execute(_DDL)
        rows = con.execute(
            """
            SELECT table_name, business_date, previous_run_id, previous_source_uri
            FROM meta.publish_log
            WHERE run_id = ? AND business_date = ? AND NOT compensated
            ORDER BY published_at
            """,
            [run_id, business_date],
        ).fetchall()

        for table, bdate, prev_run, prev_uri in rows:
            with _transaction(con):
                con.execute(f"DELETE FROM {table} WHERE business_date = ?", [bdate])
                if prev_uri:
                    con.execute(
                        f"INSERT INTO {table} BY NAME "
                        f"SELECT * FROM read_parquet('{prev_uri}')"
                    )
                    out.restored.append(f"{table}@{bdate}->run {prev_run}")
                else:
                    out.deleted.append(f"{table}@{bdate}")
                con.execute(
                    """
                    UPDATE meta.publish_log SET compensated = true
                    WHERE run_id = ? AND table_name = ? AND business_date = ?
                    """,
                    [run_id, table, bdate],
                )
        if not rows:
            out.note = f"{reason} (nothing had been published yet)"
    return out


def write_meta(batch: MetaBatch) -> int:
    """One trip, every bookkeeping row. Returns rows written.

    Every statement is an upsert on a primary key, so this activity is safe to
    replay -- which it will be, because it is the last thing a failing workflow
    does and failing workflows are exactly the ones that get retried.
    """
    written = 0
    with _writer() as con:
        con.execute(_DDL)
        with _transaction(con):
            if batch.run:
                r = batch.run
                con.execute(
                    """
                    INSERT INTO meta.runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (run_id) DO UPDATE SET
                        status = excluded.status,
                        finished_at = excluded.finished_at,
                        rows_published = excluded.rows_published,
                        error = excluded.error
                    """,
                    [r.run_id, r.workflow_id, r.business_date, r.mode, r.status,
                     r.started_at or None, r.finished_at or None, r.rows_published, r.error],
                )
                written += 1

            for m in batch.step_metrics:
                con.execute(
                    """
                    INSERT OR REPLACE INTO meta.step_metrics VALUES
                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [m.run_id, m.step_key, m.layer, m.state, m.attempts, m.rows_in,
                     m.rows_out, m.rows_rejected, m.duration_seconds, m.bytes_out,
                     m.threads, m.memory_limit, m.sla_seconds, m.sla_breached,
                     m.output_uri, m.error],
                )
                written += 1

            for c in batch.dq_checks:
                con.execute(
                    "INSERT OR REPLACE INTO meta.dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [batch.dq_run_id, batch.dq_step_key, c.name, c.kind, c.dataset,
                     c.column, c.severity, c.passed, c.observed, c.expected, c.detail],
                )
                written += 1

            for e in batch.lineage:
                con.execute(
                    "INSERT OR REPLACE INTO meta.lineage VALUES (?, ?, ?, ?, ?)",
                    [e.run_id, e.step_key, e.upstream, e.downstream, e.columns],
                )
                written += 1
    return written


class _transaction:
    """BEGIN/COMMIT with a rollback on the way out.

    DuckDB has no nested transactions, so this is deliberately not reentrant --
    each publish target gets its own, which means a failure on the second table
    leaves the first one committed and the publish_log consistent with that.
    Compensation then undoes exactly what landed.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self._con = con

    def __enter__(self) -> duckdb.DuckDBPyConnection:
        self._con.execute("BEGIN TRANSACTION")
        return self._con

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            self._con.execute("COMMIT")
        else:
            self._con.execute("ROLLBACK")
        return False


def summary(path: str | None = None) -> dict[str, int]:
    """Row counts of everything the warehouse holds. Read-only, so it does not
    take the write lock."""
    cfg = settings()
    con = session.open_read_only(path or cfg.warehouse_path)
    try:
        out: dict[str, int] = {}
        tables = con.execute(
            """
            SELECT table_schema || '.' || table_name
            FROM information_schema.tables
            WHERE table_schema IN ('gold', 'meta')
            ORDER BY 1
            """
        ).fetchall()
        for (name,) in tables:
            row = con.execute(f"SELECT count(*) FROM {name}").fetchone()
            out[name] = int(row[0]) if row else 0
        return out
    finally:
        con.close()


def published_targets(run_id: str, business_date: str, uri_for) -> Iterable[tuple[str, str]]:
    """(warehouse table, run-scoped Parquet URI) for every gold dataset."""
    for ds_name, table in PUBLISHED.items():
        yield table, uri_for(dataset(ds_name), business_date, run_id)
