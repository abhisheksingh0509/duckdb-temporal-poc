"""Backfill a date range as child workflows, with continue-as-new checkpointing.

Two Temporal mechanics that only show up at this scale:

**Child workflows, not activities.** Each date gets its own `MeterPipeline`
execution, with its own history, its own retries, its own approval gate and its
own compensation. A 90-day backfill is then 90 independently inspectable runs
plus one coordinator, rather than one history with 30 000 events in it.

**Continue-as-new.** A workflow history is capped (hard limit 51 200 events; the
server starts warning around 10 000), and a long backfill would sail past that.
`continue_as_new` ends the current execution and starts a fresh one, same
workflow id, carrying only the small state that matters: where we got to. To the
outside it is still one workflow.

The concurrency limit is the other half of the story. Child workflows are
cheap, but each one publishes through the *single* writer queue, so launching
90 at once just makes 90 things queue behind one slot -- with longer timers and
a worse failure mode than launching four at a time.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date, timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from duckflow.shared import Mode, PipelineInput, PipelineOutput

from duckflow.workflows.pipeline import MeterPipelineWorkflow


@dataclass
class BackfillInput:
    start_date: str
    end_date: str
    mode: str = Mode.AUTO.value
    meters: int = 5000
    lake_uri: str = "s3://lake"
    parallelism: int = 2
    checkpoint_every: int = 10
    """Dates per execution before continue-as-new. Keep it well under the point
    where the coordinator's own history gets large."""
    seed: bool = True
    completed: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    rows_published: int = 0


@dataclass
class BackfillOutput:
    start_date: str
    end_date: str
    completed: list[str]
    failed: list[str]
    rows_published: int


def _dates(start: str, end: str) -> list[str]:
    a, b = date.fromisoformat(start), date.fromisoformat(end)
    if b < a:
        raise ValueError(f"end_date {end} is before start_date {start}")
    return [(a + timedelta(days=i)).isoformat() for i in range((b - a).days + 1)]


@workflow.defn(name="MeterBackfill")
class MeterBackfillWorkflow:
    def __init__(self) -> None:
        self._current: list[str] = []
        self._done: list[str] = []
        self._failed: list[str] = []

    @workflow.query
    def progress(self) -> dict:
        return {"in_flight": self._current, "completed": self._done, "failed": self._failed}

    @workflow.run
    async def run(self, inp: BackfillInput) -> BackfillOutput:
        remaining = [d for d in _dates(inp.start_date, inp.end_date) if d not in inp.completed]
        self._done = list(inp.completed)
        self._failed = list(inp.failed)
        rows = inp.rows_published

        batch = remaining[: inp.checkpoint_every]
        for group in _chunks(batch, max(1, inp.parallelism)):
            self._current = list(group)
            results = await asyncio.gather(
                *[self._one_date(inp, d) for d in group], return_exceptions=True
            )
            for d, res in zip(group, results):
                if isinstance(res, BaseException):
                    workflow.logger.error("backfill date %s failed: %s", d, res)
                    self._failed.append(d)
                else:
                    self._done.append(d)
                    rows += res.rows_published
            self._current = []

        still_left = [d for d in remaining[inp.checkpoint_every:]]
        if still_left:
            # Everything the next execution needs is in this one object. Note
            # that `completed` is what makes the backfill resumable: a crash
            # between checkpoints loses at most `checkpoint_every` dates of work
            # and never re-publishes a date that already landed.
            workflow.continue_as_new(
                BackfillInput(
                    start_date=inp.start_date,
                    end_date=inp.end_date,
                    mode=inp.mode,
                    meters=inp.meters,
                    lake_uri=inp.lake_uri,
                    parallelism=inp.parallelism,
                    checkpoint_every=inp.checkpoint_every,
                    seed=inp.seed,
                    completed=self._done,
                    failed=self._failed,
                    rows_published=rows,
                )
            )

        return BackfillOutput(
            start_date=inp.start_date,
            end_date=inp.end_date,
            completed=self._done,
            failed=self._failed,
            rows_published=rows,
        )

    async def _one_date(self, inp: BackfillInput, business_date: str) -> PipelineOutput:
        # A stable child id makes the backfill idempotent at the Temporal level:
        # re-issuing it while a date is still running is rejected rather than
        # silently duplicating the run.
        return await workflow.execute_child_workflow(
            MeterPipelineWorkflow.run,
            PipelineInput(
                business_date=business_date,
                run_id=f"backfill-{business_date}",
                lake_uri=inp.lake_uri,
                mode=inp.mode,
                meters=inp.meters,
                seed=inp.seed,
                require_approval=False,
            ),
            id=f"meter-backfill-{business_date}",
            retry_policy=RetryPolicy(maximum_attempts=1),
        )


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]
