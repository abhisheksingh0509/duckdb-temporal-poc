"""Every activity in the pipeline.

An activity is the only place in this repo allowed to touch the outside world.
Two groups, split by which task queue registers them:

    COMPUTE_ACTIVITIES  stateless DuckDB over object storage. Safe to run many
                        at once, on many workers, twice.
    WRITER_ACTIVITIES   the warehouse file. One worker, one slot, ever.

The shape shared by all of them:

* blocking DuckDB work runs in a worker thread via `asyncio.to_thread`, so the
  activity's event loop stays free to heartbeat;
* the heartbeat carries the current stage, so the Temporal UI shows which SQL
  statement a slow step is on without anyone reading a log;
* on cancellation the connection is interrupted -- the only way to stop DuckDB
  mid-query, and it must come from a different thread than the blocked one,
  which is exactly what this arrangement gives for free.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable

import duckdb
from temporalio import activity
from temporalio.exceptions import ApplicationError

from duckflow.config import settings
from duckflow.data import generator
from duckflow.duck import session, warehouse
from duckflow.quality import checks
from duckflow.shared import (
    CompensateRequest,
    CompensateResult,
    DQReport,
    DQRequest,
    DuckSettings,
    MetaBatch,
    OutputRef,
    ProbeRequest,
    ProbeResult,
    PublishRequest,
    PublishResult,
    SeedRequest,
    SeedResult,
    StepRequest,
    StepResult,
    StepState,
    Severity,
)
from duckflow.steps import DATASETS, STEPS, render_context

log = logging.getLogger("duckflow.activities")

HEARTBEAT_SECONDS = 5.0


class _Live:
    """Handle on the running query, shared between the event loop and the
    worker thread. The loop reads `stage` to heartbeat and calls `interrupt` to
    cancel; the thread writes `stage` and `con`."""

    def __init__(self) -> None:
        self.con: duckdb.DuckDBPyConnection | None = None
        self.stage: str = "starting"

    def interrupt(self) -> None:
        if self.con is not None:
            try:
                self.con.interrupt()
            except duckdb.Error:
                pass


async def _with_heartbeat(fn: Callable[[_Live], Any]) -> Any:
    """Run blocking DuckDB work in a thread while heartbeating.

    Temporal cancels an activity by throwing CancelledError into this coroutine.
    That does not stop the thread -- nothing in Python can -- so we interrupt the
    query instead, which makes the thread raise and unwind on its own.
    """
    live = _Live()
    stop = asyncio.Event()

    async def beat() -> None:
        while not stop.is_set():
            activity.heartbeat(live.stage)
            try:
                await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_SECONDS)
            except asyncio.TimeoutError:
                continue

    beater = asyncio.create_task(beat())
    try:
        return await asyncio.to_thread(fn, live)
    except asyncio.CancelledError:
        log.warning("activity cancelled during %r, interrupting DuckDB", live.stage)
        live.interrupt()
        raise
    finally:
        stop.set()
        beater.cancel()


# --------------------------------------------------------------------------
# COMPUTE
# --------------------------------------------------------------------------


@activity.defn
async def seed_landing(req: SeedRequest) -> SeedResult:
    """Generate the landing zone. Idempotent: same date, same files, overwritten."""
    return await _with_heartbeat(lambda live: _seed(req, live))


def _seed(req: SeedRequest, live: _Live) -> SeedResult:
    live.stage = f"generating landing zone for {req.business_date}"
    return generator.generate(req)


@activity.defn
async def probe_inputs(req: ProbeRequest) -> ProbeResult:
    """Read Parquet footers to size a step's input before running it.

    This is what makes Mode.AUTO more than a guess: `parquet_file_metadata`
    touches the footer only, so sizing 400 MB of input costs a few object-store
    range requests rather than a scan.
    """
    return await _with_heartbeat(lambda live: _probe(req, live))


def _probe(req: ProbeRequest, live: _Live) -> ProbeResult:
    out = ProbeResult()
    with session.connect(DuckSettings(threads=2, memory_limit="512MB")) as con:
        live.con = con
        for uri in req.uris:
            live.stage = f"probing {uri.rsplit('/', 3)[-3] if '/' in uri else uri}"
            try:
                row = con.execute(
                    f"""
                    SELECT coalesce(sum(num_rows), 0), count(*)
                    FROM parquet_file_metadata('{uri}')
                    """
                ).fetchone()
                rows, files = (int(row[0]), int(row[1])) if row else (0, 0)
                size = session.parquet_bytes(con, uri)
            except duckdb.Error:
                rows, files, size = 0, 0, 0
            out.total_rows += rows
            out.files += files
            out.total_bytes += size
            out.per_uri[uri] = size
    return out


@activity.defn
async def run_step(req: StepRequest) -> StepResult:
    """Execute one step of the catalog.

    The activity has no knowledge of what the step does. It registers inputs,
    runs the step's `pre` statements, copies each declared output to Parquet and
    reports counts. All the domain logic is data, in steps.py.
    """
    if req.fail:
        # Chaos switch. Retryable on purpose: the point of the demo is to watch
        # Temporal re-attempt, back off, and give up according to the policy.
        raise ApplicationError(
            f"injected failure in {req.step_key}", type="ChaosError"
        )
    result = await _with_heartbeat(lambda live: _run_step(req, live))
    result.attempts = activity.info().attempt
    return result


def _run_step(req: StepRequest, live: _Live) -> StepResult:
    spec = STEPS[req.step_key]
    started = time.monotonic()
    result = StepResult(step_key=req.step_key, state=StepState.RUNNING.value)
    ctx = render_context(req.business_date, req.run_id)
    pre_sql, selects = spec.render(ctx)

    with session.connect(req.duck) as con:
        live.con = con

        for name, uri in req.inputs.items():
            live.stage = f"register {name}"
            session.register_source(con, name, uri, DATASETS[name].fmt)
            result.rows_in += _cheap_count(con, uri, DATASETS[name].fmt)

        for i, stmt in enumerate(pre_sql, start=1):
            live.stage = f"pre {i}/{len(pre_sql)}"
            con.execute(stmt)

        for idx, name in enumerate(spec.outputs):
            uri = req.outputs[name]
            live.stage = f"write {name}"
            rows = session.copy_to_parquet(con, selects[name], uri)
            size = session.parquet_bytes(con, uri)
            result.outputs.append(OutputRef(dataset=name, uri=uri, rows=rows, bytes=size))
            # The first declared output is the step's product; anything after it
            # is a side channel -- quarantine today, rejects tomorrow.
            if idx == 0:
                result.rows_out = rows
            else:
                result.rows_rejected += rows

        live.stage = "memory"
        result.peak_memory_note = _memory_note(con)

    result.state = StepState.SUCCEEDED.value
    result.duration_seconds = round(time.monotonic() - started, 3)
    log.info(
        "step %s done in %.2fs: in=%d out=%d rejected=%d",
        req.step_key, result.duration_seconds, result.rows_in,
        result.rows_out, result.rows_rejected,
    )
    return result


def _cheap_count(con: duckdb.DuckDBPyConnection, uri: str, fmt: str) -> int:
    """Row count without a scan where the format allows one."""
    if fmt == "csv":
        return session.count_rows(con, uri, "csv")
    try:
        row = con.execute(
            f"SELECT coalesce(sum(num_rows), 0) FROM parquet_file_metadata('{uri}')"
        ).fetchone()
        return int(row[0]) if row else 0
    except duckdb.Error:
        return 0


def _memory_note(con: duckdb.DuckDBPyConnection) -> str:
    """What DuckDB actually used. `duckdb_memory()` is the only honest answer to
    'did this step spill?', and spilling is the thing a LEAN run is testing."""
    try:
        row = con.execute(
            """
            SELECT
                coalesce(sum(memory_usage_bytes), 0),
                coalesce(sum(temporary_storage_bytes), 0)
            FROM duckdb_memory()
            """
        ).fetchone()
        if not row:
            return ""
        mem, tmp = int(row[0]), int(row[1])
        return f"memory={mem / 1e6:.1f}MB spill={tmp / 1e6:.1f}MB"
    except duckdb.Error:
        return ""


@activity.defn
async def run_quality_gate(req: DQRequest) -> DQReport:
    """Evaluate the step's expectations and fail non-retryably on an ERROR.

    Non-retryable matters: the input Parquet is immutable, so a second attempt
    reads the same bytes and reaches the same verdict. Retrying would turn a
    five-second failure into a ninety-second one and tell nobody anything.
    """
    report = await _with_heartbeat(lambda live: _gate(req, live))
    errors = [c for c in report.checks if not c.passed and c.severity == Severity.ERROR.value]
    if errors:
        detail = "; ".join(
            f"{c.name}: observed {c.observed}, expected {c.expected}{' -- ' + c.detail if c.detail else ''}"
            for c in errors
        )
        raise ApplicationError(
            f"quality gate failed for {req.step_key}: {detail}",
            report,
            type="DataQualityError",
            non_retryable=True,
        )
    return report


def _gate(req: DQRequest, live: _Live) -> DQReport:
    live.stage = f"quality gate {req.step_key}"
    return checks.evaluate(req.step_key, req.uris)


# --------------------------------------------------------------------------
# WRITER  -- everything below runs one-at-a-time on the writer task queue
# --------------------------------------------------------------------------


@activity.defn
async def ensure_warehouse() -> str:
    """Create schemas and metadata tables. Runs once per workflow, before
    anything else needs the file, so the first real write is never also the
    first DDL."""
    def go(live: _Live) -> str:
        live.stage = "DDL"
        warehouse.ensure_schema()
        return settings().warehouse_path
    return await _with_heartbeat(go)


@activity.defn
async def publish_gold(req: PublishRequest) -> PublishResult:
    """Merge this run's gold Parquet into the warehouse. See warehouse.py for
    why DELETE+INSERT rather than INSERT, and what publish_log is for."""
    return await _with_heartbeat(lambda live: _publish(req, live))


def _publish(req: PublishRequest, live: _Live) -> PublishResult:
    live.stage = f"publishing {len(req.targets)} tables"
    return warehouse.publish(req)


@activity.defn
async def compensate_publish(req: CompensateRequest) -> CompensateResult:
    """Saga compensation: put every published table back to the run before this
    one. Step outputs in object storage are left alone deliberately -- they are
    immutable, run-scoped and the only evidence of what went wrong."""
    def go(live: _Live) -> CompensateResult:
        live.stage = "compensating"
        return warehouse.compensate(req.run_id, req.business_date, req.reason)
    return await _with_heartbeat(go)


@activity.defn
async def write_metadata(batch: MetaBatch) -> int:
    """One batched trip to the writer for run, step, quality and lineage rows.

    Called on the happy path *and* from the failure path, which is why every
    statement inside is an upsert.
    """
    def go(live: _Live) -> int:
        live.stage = "metadata"
        return warehouse.write_meta(batch)
    return await _with_heartbeat(go)


COMPUTE_ACTIVITIES = [
    seed_landing,
    probe_inputs,
    run_step,
    run_quality_gate,
]

WRITER_ACTIVITIES = [
    ensure_warehouse,
    publish_gold,
    compensate_publish,
    write_metadata,
]
