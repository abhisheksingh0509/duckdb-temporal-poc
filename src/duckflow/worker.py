"""Worker entrypoint.

Two roles, two task queues, one codebase:

  compute  Hosts the workflows and every stateless activity. DuckDB runs
           in-memory against object storage, so any number of these can run
           concurrently on any number of machines. Scale this on CPU.

  writer   Hosts nothing but the four activities that touch warehouse.duckdb,
           with `max_concurrent_activities=1`. Run exactly one. Scaling it is
           not a tuning decision you get to make -- DuckDB allows one read-write
           process per file, so a second writer is a corruption bug, not a
           throughput win.

That asymmetry is the argument this repo exists to make. The embedded engine
gives up concurrent writers; a task queue with one slot gives back a *durable,
observable, backpressured* queue in front of that single writer, which is a far
better answer than a mutex in application code.

    python -m duckflow.worker compute
    python -m duckflow.worker writer
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from datetime import timedelta

from temporalio.client import Client
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig
from temporalio.worker import Worker

from duckflow import queues, steps
from duckflow.activities.core import COMPUTE_ACTIVITIES, WRITER_ACTIVITIES
from duckflow.config import settings
from duckflow.observability import logs
from duckflow.workflows.backfill import MeterBackfillWorkflow
from duckflow.workflows.pipeline import MeterPipelineWorkflow

log = logging.getLogger("duckflow.worker")

ROLES: dict[str, dict] = {
    "compute": {
        "task_queue": queues.COMPUTE,
        "activities": COMPUTE_ACTIVITIES,
        # Only the compute role hosts workflows. Keeping workflow code off the
        # writer means a long publish can never starve the orchestrator, and a
        # writer restart never replays a workflow.
        "workflows": [MeterPipelineWorkflow, MeterBackfillWorkflow],
        "concurrency": None,
    },
    "writer": {
        "task_queue": queues.WRITER,
        "activities": WRITER_ACTIVITIES,
        "workflows": [],
        # The number that enforces DuckDB's single-writer rule. Raise it and
        # `make demo-contention` shows you what you bought.
        "concurrency": 1,
    },
}


async def connect() -> Client:
    cfg = settings()
    runtime = None
    if cfg.prometheus_port:
        # The SDK exposes worker, activity and workflow metrics itself, so the
        # optional observability profile needs no instrumentation of our own.
        runtime = Runtime(
            telemetry=TelemetryConfig(
                metrics=PrometheusConfig(bind_address=f"0.0.0.0:{cfg.prometheus_port}")
            )
        )
        log.info("prometheus metrics on :%s/metrics", cfg.prometheus_port)

    last: Exception | None = None
    for attempt in range(1, 31):
        try:
            return await Client.connect(
                cfg.temporal_address, namespace=cfg.temporal_namespace, runtime=runtime
            )
        except Exception as exc:  # noqa: BLE001
            last = exc
            log.warning("temporal not reachable (%d/30): %s", attempt, exc)
            await asyncio.sleep(2)
    raise RuntimeError(f"could not reach Temporal at {cfg.temporal_address}") from last


async def run(role: str) -> None:
    spec = ROLES[role]
    cfg = settings()
    logs.configure(component=f"worker-{role}")

    problems = steps.validate_catalog()
    if problems:
        # Fail at startup, not at 02:00 halfway through phase three.
        for p in problems:
            log.error("catalog: %s", p)
        raise SystemExit(f"step catalog is invalid ({len(problems)} problems)")

    if role == "writer":
        # Create the schema once here so the first workflow's DDL is not also
        # its first contention.
        from duckflow.duck import warehouse

        try:
            await asyncio.to_thread(warehouse.ensure_schema)
            log.info("warehouse ready at %s", cfg.warehouse_path)
        except Exception as exc:  # noqa: BLE001
            log.warning("warehouse bootstrap deferred to first activity: %s", exc)

    client = await connect()
    concurrency = spec["concurrency"] or cfg.max_concurrent_activities
    log.info(
        "worker starting role=%s queue=%s activities=%d workflows=%s concurrency=%d",
        role, spec["task_queue"], len(spec["activities"]),
        [w.__name__ for w in spec["workflows"]], concurrency,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    # Every activity is `async def` and offloads its blocking DuckDB work with
    # asyncio.to_thread, so no activity_executor is needed.
    worker = Worker(
        client,
        task_queue=spec["task_queue"],
        workflows=spec["workflows"],
        activities=spec["activities"],
        max_concurrent_activities=concurrency,
        # Give an in-flight publish a chance to commit or roll back rather than
        # being killed with the transaction open.
        graceful_shutdown_timeout=timedelta(seconds=30),
    )
    async with worker:
        log.info("worker ready on %s", spec["task_queue"])
        await stop.wait()
        log.info("shutdown signal received, draining")


def main() -> int:
    parser = argparse.ArgumentParser(description="Temporal worker for the meter pipeline")
    parser.add_argument("role", choices=sorted(ROLES))
    args = parser.parse_args()
    try:
        asyncio.run(run(args.role))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
