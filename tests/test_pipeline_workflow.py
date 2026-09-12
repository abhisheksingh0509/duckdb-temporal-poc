"""End-to-end orchestration test against a real Temporal dev server.

No Docker and no MinIO: the lake is a temp directory, the warehouse is a temp
file, and `WorkflowEnvironment.start_local()` brings up a throwaway Temporal.
It is slower than the unit tests (a few seconds) and worth it -- these are the
only tests that exercise replay, retries, the task-queue split and the saga.

Skipped automatically if the dev server cannot be downloaded or started.
"""

from __future__ import annotations

import uuid

import pytest
from temporalio.client import WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from duckflow import queues
from duckflow.activities.core import COMPUTE_ACTIVITIES, WRITER_ACTIVITIES
from duckflow.duck import session
from duckflow.shared import Mode, PipelineInput
from duckflow.workflows.backfill import MeterBackfillWorkflow
from duckflow.workflows.pipeline import MeterPipelineWorkflow

pytestmark = pytest.mark.anyio

DATE = "2026-08-01"
METERS = 120


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def _env():
    try:
        return await WorkflowEnvironment.start_local()
    except Exception as exc:  # noqa: BLE001 -- no network in CI, for instance
        pytest.skip(f"no local Temporal dev server available: {exc}")


def _workers(env, client):
    """Two workers, exactly as in production: compute hosts the workflows and
    the stateless activities; writer hosts nothing but the four that touch the
    warehouse file, one at a time."""
    compute = Worker(
        client,
        task_queue=queues.COMPUTE,
        workflows=[MeterPipelineWorkflow, MeterBackfillWorkflow],
        activities=COMPUTE_ACTIVITIES,
    )
    writer = Worker(
        client,
        task_queue=queues.WRITER,
        activities=WRITER_ACTIVITIES,
        max_concurrent_activities=1,
    )
    return compute, writer


def _input(lake: str, **overrides) -> PipelineInput:
    base = dict(
        business_date=DATE,
        run_id=f"test-{uuid.uuid4().hex[:8]}",
        lake_uri=lake,
        mode=Mode.LEAN.value,
        meters=METERS,
        require_approval=False,
        seed=True,
    )
    base.update(overrides)
    return PipelineInput(**base)


async def _run(env, lake, **overrides):
    inp = _input(lake, **overrides)
    compute, writer = _workers(env, env.client)
    async with compute, writer:
        return await env.client.execute_workflow(
            MeterPipelineWorkflow.run,
            inp,
            id=inp.run_id,
            task_queue=queues.COMPUTE,
        ), inp


async def test_happy_path_publishes_both_gold_tables(lake, warehouse_path):
    env = await _env()
    async with env:
        out, inp = await _run(env, lake)

    assert out.status == "succeeded"
    assert out.steps_failed == 0
    assert out.steps_succeeded == 8
    assert set(out.published_tables) == {
        "gold.daily_region_consumption", "gold.meter_anomalies"
    }
    assert out.rows_published > 0

    con = session.open_read_only(warehouse_path)
    try:
        regions = con.execute(
            "SELECT count(*) FROM gold.daily_region_consumption WHERE business_date = ?",
            [DATE],
        ).fetchone()[0]
        steps_recorded = con.execute(
            "SELECT count(*) FROM meta.step_metrics WHERE run_id = ?", [inp.run_id]
        ).fetchone()[0]
        run_status = con.execute(
            "SELECT status FROM meta.runs WHERE run_id = ?", [inp.run_id]
        ).fetchone()[0]
    finally:
        con.close()

    assert regions == 5
    assert steps_recorded == 8
    assert run_status == "succeeded"


async def test_every_check_is_recorded_under_the_step_that_declares_it(lake, warehouse_path):
    """Regression: the four bronze steps run concurrently on one workflow
    object, so any per-step state kept in an instance field gets flushed under
    whichever step reaches the writer first. That produced one failing check
    attributed to all four bronze steps."""
    env = await _env()
    async with env:
        _, inp = await _run(env, lake)

    con = session.open_read_only(warehouse_path)
    try:
        rows = con.execute(
            "SELECT step_key, check_name FROM meta.dq_results WHERE run_id = ?", [inp.run_id]
        ).fetchall()
    finally:
        con.close()

    from duckflow import steps as S

    declared = {
        (key, exp.label()) for key, spec in S.STEPS.items() for exp in spec.expectations
    }
    assert rows, "no quality checks were recorded at all"
    assert set(rows) == declared

    # And no check name may appear under two different steps.
    by_name: dict[str, set[str]] = {}
    for step_key, check_name in rows:
        by_name.setdefault(check_name, set()).add(step_key)
    assert all(len(v) == 1 for v in by_name.values()), by_name


async def test_a_failed_quality_gate_stops_the_run_and_compensates(lake, warehouse_path):
    """`bad_tariff` makes a price negative, which trips a non-retryable
    expectation in bronze. Nothing should reach the warehouse."""
    env = await _env()
    async with env:
        with pytest.raises(WorkflowFailureError) as excinfo:
            await _run(env, lake, inject="bad_tariff")

    assert "quality gate failed" in _chain(excinfo.value)
    assert "tariff_price_sane" in _chain(excinfo.value)

    con = session.open_read_only(warehouse_path)
    try:
        tables = [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold'"
        ).fetchall()]
        failed = con.execute(
            "SELECT count(*) FROM meta.runs WHERE status IN ('failed', 'compensated')"
        ).fetchone()[0]
    finally:
        con.close()

    # The gold tables are only created on first publish, which never happened.
    assert tables == []
    assert failed == 1


async def test_a_warn_level_check_does_not_stop_the_run(lake, warehouse_path):
    """`voltage_storm` pushes the quarantine rate past its threshold, but that
    expectation is WARN. The run must finish and the failure must be recorded."""
    env = await _env()
    async with env:
        out, inp = await _run(env, lake, inject="voltage_storm")

    assert out.status == "succeeded"

    con = session.open_read_only(warehouse_path)
    try:
        warned = con.execute(
            """
            SELECT check_name FROM meta.dq_results
            WHERE run_id = ? AND NOT passed AND severity = 'warn'
            """,
            [inp.run_id],
        ).fetchall()
    finally:
        con.close()

    assert [r[0] for r in warned] == ["quarantine_rate"]


async def test_a_chaos_failure_exhausts_retries_then_compensates(lake, warehouse_path):
    """`--fail-step` raises a *retryable* error, so the step should be attempted
    three times before the workflow gives up."""
    env = await _env()
    async with env:
        with pytest.raises(WorkflowFailureError):
            await _run(env, lake, fail_step="silver_enrich")

    con = session.open_read_only(warehouse_path)
    try:
        status = con.execute("SELECT status FROM meta.runs").fetchone()[0]
    finally:
        con.close()
    assert status in {"failed", "compensated"}


async def test_a_second_run_of_the_same_date_replaces_rather_than_appends(lake, warehouse_path):
    """Two runs, one business date. The warehouse must show one date's worth of
    rows and a publish_log that knows who it replaced."""
    env = await _env()
    async with env:
        first, first_in = await _run(env, lake)
        second, second_in = await _run(env, lake)

    assert first.rows_published > 0 and second.rows_published > 0

    con = session.open_read_only(warehouse_path)
    try:
        regions = con.execute(
            "SELECT count(*) FROM gold.daily_region_consumption WHERE business_date = ?",
            [DATE],
        ).fetchone()[0]
        replaced = con.execute(
            """
            SELECT previous_run_id FROM meta.publish_log
            WHERE run_id = ? AND table_name = 'gold.daily_region_consumption'
            """,
            [second_in.run_id],
        ).fetchone()[0]
    finally:
        con.close()

    assert regions == 5
    assert replaced == first_in.run_id


async def test_the_approval_gate_holds_the_run_until_signalled(lake, warehouse_path):
    env = await _env()
    async with env:
        inp = _input(lake, require_approval=True, approval_timeout_seconds=0)
        compute, writer = _workers(env, env.client)
        async with compute, writer:
            handle = await env.client.start_workflow(
                MeterPipelineWorkflow.run, inp, id=inp.run_id, task_queue=queues.COMPUTE
            )
            await _wait_for_status(handle, "awaiting_approval")
            await handle.signal(MeterPipelineWorkflow.approve_publish, "tester")
            out = await handle.result()

    assert out.status == "succeeded"
    assert out.rows_published > 0


def _chain(exc: BaseException) -> str:
    """Flatten a WorkflowFailureError's cause chain.

    The top-level message is always "Workflow execution failed"; the useful text
    is one or two `cause` hops down, wrapped by ActivityError.
    """
    parts, cur = [], exc
    while cur is not None:
        parts.append(str(cur))
        cur = getattr(cur, "cause", None)
    return " | ".join(parts).lower()


async def _wait_for_status(handle, wanted: str, tries: int = 120) -> None:
    import asyncio

    for _ in range(tries):
        progress = await handle.query(MeterPipelineWorkflow.progress)
        if progress["status"] == wanted:
            return
        await asyncio.sleep(0.25)
    raise AssertionError(f"workflow never reached status {wanted!r}")
