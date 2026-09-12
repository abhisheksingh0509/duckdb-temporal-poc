"""The meter pipeline workflow.

bronze -> cleanse -> enrich -> gold -> publish, with the orchestration features
that are the actual reason to put Temporal in front of DuckDB:

  fan-out / fan-in      four bronze reads and two gold aggregates run in
                        parallel on whatever compute workers exist
  runtime sizing        AUTO probes each step's Parquet footprint and picks the
                        DuckDB memory limit and thread count for it
  durable timers        the per-step SLA is a workflow timer, not a cron
                        watchdog; it survives a worker restart mid-step
  typed retries         a failed quality gate is non-retryable; a spilled query
                        that ran out of temp space is not
  saga compensation     on failure every published table is restored to the run
                        that owned it before this one
  signals / queries     approval gate, pause, resume, abort, live progress
  updates               the SLA of a running step can be extended

Determinism rules observed here: no wall clock except `workflow.now()`, no
environment reads, no randomness, no IO. Every URI this workflow computes is a
pure function of (lake, dataset, business_date, run_id), so a replay recomputes
exactly the same strings.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from duckflow import queues
    from duckflow import steps as S
    from duckflow.activities.core import (
        compensate_publish,
        ensure_warehouse,
        probe_inputs,
        publish_gold,
        run_quality_gate,
        run_step,
        seed_landing,
        write_metadata,
    )
    from duckflow.shared import (
        CompensateRequest,
        DQRequest,
        DuckSettings,
        LineageEvent,
        MetaBatch,
        Mode,
        PipelineInput,
        PipelineOutput,
        ProbeRequest,
        PublishRequest,
        PublishTarget,
        RunRecord,
        RunStatus,
        SeedRequest,
        StepMetric,
        StepRequest,
        StepResult,
        StepState,
    )

# A transient object-store hiccup or a spilled query that hit a full temp
# directory deserves a retry. A failed expectation does not: the input Parquet
# is immutable, so attempt two reads the same bytes and reaches the same verdict.
STEP_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=["DataQualityError", "ConfigurationError"],
)

# The writer is a single slot. Its work is small, idempotent and on the critical
# path, so retry it harder and faster than compute.
WRITER_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=6,
)

STEP_TIMEOUT = timedelta(minutes=20)
WRITER_TIMEOUT = timedelta(minutes=10)
HEARTBEAT_TIMEOUT = timedelta(seconds=30)

#: Resolved DuckDB settings per mode. LEAN is deliberately small enough that the
#: bigger steps spill to disk -- that is the configuration worth testing,
#: because it is the one that tells you whether the box can survive a bad day.
#:
#: These numbers are sized against the *container*, not against the step, and
#: that arithmetic is easy to get wrong: `memory_limit` applies to one DuckDB
#: connection, and this repo opens one per activity. A compute worker running
#: `max_concurrent_activities=4` can therefore have four of these live at once,
#: so the real ceiling is `concurrency x memory_limit` versus the cgroup limit
#: in docker-compose.yml -- 4 x 1 GB against 4 GB here.
#:
#: A cap is not a reservation, so a modest over-commit is normal and usually
#: fine. What is not fine is not knowing the multiplier: exceed the cgroup limit
#: and the kernel kills the whole worker, which Temporal will faithfully retry
#: into the same wall.
PROFILES = {
    "lean": DuckSettings(threads=2, memory_limit="512MB"),
    "roomy": DuckSettings(threads=4, memory_limit="1GB"),
}


@dataclass
class StepProgress:
    """Per-step state exposed through the `progress` query, so an operator can
    watch a run without reading a 400-event history."""

    step_key: str
    layer: str
    title: str
    state: str = StepState.PENDING.value
    profile: str = "unresolved"
    threads: int = 0
    memory_limit: str = ""
    input_mb: float = 0.0
    attempts: int = 0
    rows_in: int = 0
    rows_out: int = 0
    rows_rejected: int = 0
    duration_seconds: float = 0.0
    sla_seconds: int = 0
    sla_breached: bool = False
    dq_run: int = 0
    dq_failed: int = 0
    outputs: list[str] = field(default_factory=list)
    error: str = ""


@workflow.defn(name="MeterPipeline")
class MeterPipelineWorkflow:
    def __init__(self) -> None:
        self._input: PipelineInput | None = None
        self._status: str = RunStatus.RUNNING.value
        self._phase: str = "init"
        self._progress: dict[str, StepProgress] = {}
        self._uris: dict[str, str] = {}
        self._approved: bool = False
        self._approved_by: str = ""
        self._paused: bool = False
        self._aborted: bool = False
        self._published: list[str] = []
        self._rows_published: int = 0
        self._sla_extra: dict[str, int] = {}
        self._sla_breaches: list[str] = []

    # ---------------------------------------------------------------- signals

    @workflow.signal
    def approve_publish(self, actor: str = "") -> None:
        """Release the human gate. Signals are durable: sending this to a
        workflow whose worker is down parks it in the history and delivers it
        when a worker comes back."""
        self._approved = True
        self._approved_by = actor

    @workflow.signal
    def pause(self) -> None:
        self._paused = True

    @workflow.signal
    def resume(self) -> None:
        self._paused = False

    @workflow.signal
    def abort(self, reason: str = "operator abort") -> None:
        self._aborted = True
        self._paused = False
        workflow.logger.warning("abort signalled: %s", reason)

    # ---------------------------------------------------------------- queries

    @workflow.query
    def progress(self) -> dict:
        return {
            "status": self._status,
            "phase": self._phase,
            "paused": self._paused,
            "approved": self._approved,
            "published": self._published,
            "rows_published": self._rows_published,
            "steps": [vars(p) for p in self._progress.values()],
        }

    @workflow.query
    def plan(self) -> dict:
        """What the workflow decided to do, before it has finished doing it."""
        return {
            "phases": [
                {"phase": name, "steps": list(keys)} for name, keys in S.PHASES
            ],
            "profiles": {k: v.profile for k, v in self._progress.items()},
            "sla_extensions": dict(self._sla_extra),
        }

    # ---------------------------------------------------------------- updates

    @workflow.update
    def extend_sla(self, step_key: str, extra_seconds: int) -> int:
        """Give a step more rope while it is running.

        An update, not a signal, because the caller wants the new value back --
        and because validation should reject a bad step key at call time rather
        than silently doing nothing.
        """
        self._sla_extra[step_key] = self._sla_extra.get(step_key, 0) + extra_seconds
        return self._sla_extra[step_key]

    @extend_sla.validator
    def _validate_extend_sla(self, step_key: str, extra_seconds: int) -> None:
        if step_key not in S.STEPS:
            raise ValueError(f"unknown step {step_key!r}")
        if extra_seconds <= 0:
            raise ValueError("extra_seconds must be positive")

    # ------------------------------------------------------------------- run

    @workflow.run
    async def run(self, inp: PipelineInput) -> PipelineOutput:
        self._input = inp
        started = workflow.now()
        self._seed_progress()

        # Every path is computed here, from the input, so the history records
        # exactly which files the run read and wrote -- and a replay recomputes
        # the same ones rather than asking the environment a second time.
        lake = inp.lake_uri

        await self._writer(ensure_warehouse)
        await self._record(
            MetaBatch(
                run=RunRecord(
                    run_id=inp.run_id,
                    workflow_id=workflow.info().workflow_id,
                    business_date=inp.business_date,
                    mode=inp.mode,
                    status=RunStatus.RUNNING.value,
                    started_at=started.isoformat(sep=" ", timespec="seconds"),
                )
            )
        )

        try:
            await self._materialise_landing(inp, lake)

            for phase_name, keys in S.PHASES:
                self._phase = phase_name
                await self._gate_on_control_signals()
                await self._run_phase(inp, lake, keys)

            self._phase = "publish"
            if inp.publish:
                await self._await_approval(inp)
                await self._publish(inp, lake)
                if inp.fail_after_publish:
                    raise ApplicationError(
                        "injected failure after a successful publish",
                        type="ChaosError",
                        non_retryable=True,
                    )
            else:
                workflow.logger.info("publish skipped by input")

            self._status = RunStatus.SUCCEEDED.value

        except Exception as exc:  # noqa: BLE001 -- re-raised after compensating
            self._status = RunStatus.FAILED.value
            message = _describe(exc)
            workflow.logger.error("pipeline failed: %s", message)
            await self._compensate(inp, message)
            await self._finalise(inp, started, error=message)
            raise

        await self._finalise(inp, started)
        return self._output(inp, started)

    # ------------------------------------------------------------- internals

    def _seed_progress(self) -> None:
        for _, keys in S.PHASES:
            for key in keys:
                spec = S.STEPS[key]
                self._progress[key] = StepProgress(
                    step_key=key,
                    layer=spec.layer,
                    title=spec.title,
                    sla_seconds=spec.sla_seconds,
                )

    async def _materialise_landing(self, inp: PipelineInput, lake: str) -> None:
        """Put the landing URIs in the map, generating the files if asked."""
        landing = {
            name: ds.uri(lake, inp.business_date)
            for name, ds in S.DATASETS.items()
            if ds.layer == "landing"
        }
        if inp.seed:
            result = await workflow.execute_activity(
                seed_landing,
                SeedRequest(
                    business_date=inp.business_date,
                    run_id=inp.run_id,
                    meters=inp.meters,
                    inject=inp.inject,
                ),
                task_queue=queues.COMPUTE,
                start_to_close_timeout=STEP_TIMEOUT,
                heartbeat_timeout=HEARTBEAT_TIMEOUT,
                retry_policy=STEP_RETRY,
            )
            landing.update(result.datasets)
        self._uris.update(landing)

    async def _run_phase(self, inp: PipelineInput, lake: str, keys: tuple[str, ...]) -> None:
        """Run every step of a phase concurrently, then merge their outputs.

        The URI map is updated only after the whole phase settles. Steps inside
        a phase are independent by construction, so nothing in flight can be
        reading a key another task is writing -- and keeping the mutation at the
        phase boundary means the map is identical on replay regardless of the
        order the activities happened to complete in.
        """
        results = await asyncio.gather(
            *[self._run_one(inp, lake, key) for key in keys]
        )
        for key, produced in zip(keys, results):
            self._uris.update(produced)

    async def _run_one(self, inp: PipelineInput, lake: str, key: str) -> dict[str, str]:
        spec = S.STEPS[key]
        prog = self._progress[key]

        inputs = {name: self._uris[name] for name in spec.inputs}
        outputs = {
            name: S.DATASETS[name].uri(lake, inp.business_date, inp.run_id)
            for name in spec.outputs
        }

        duck, profile, input_mb = await self._size_step(inp, key, inputs)
        prog.profile, prog.threads, prog.memory_limit = profile, duck.threads, duck.memory_limit
        prog.input_mb = input_mb
        prog.state = StepState.RUNNING.value

        request = StepRequest(
            run_id=inp.run_id,
            business_date=inp.business_date,
            step_key=key,
            duck=duck,
            inputs=inputs,
            outputs=outputs,
            fail=(inp.fail_step == key),
        )

        result = await self._with_sla(key, workflow.execute_activity(
            run_step,
            request,
            task_queue=queues.COMPUTE,
            start_to_close_timeout=STEP_TIMEOUT,
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            retry_policy=STEP_RETRY,
        ))

        prog.state = result.state
        prog.rows_in, prog.rows_out = result.rows_in, result.rows_out
        prog.rows_rejected = result.rows_rejected
        prog.duration_seconds = result.duration_seconds
        prog.outputs = [o.uri for o in result.outputs]

        produced = {o.dataset: o.uri for o in result.outputs}
        await self._quality_gate(inp, key, produced, result)
        return produced

    async def _size_step(
        self, inp: PipelineInput, key: str, inputs: dict[str, str]
    ) -> tuple[DuckSettings, str, float]:
        """Choose DuckDB's memory limit and thread count for this step.

        DuckDB has no cluster to size, so this is the whole tuning surface. In
        AUTO we probe the input's Parquet footers -- cheap, footer-only -- and
        give a big step room while leaving small steps lean so that four of them
        can share one worker without fighting over the same RAM.
        """
        if inp.mode == Mode.LEAN.value:
            return PROFILES["lean"], "lean", 0.0
        if inp.mode == Mode.ROOMY.value:
            return PROFILES["roomy"], "roomy", 0.0

        parquet_inputs = [
            uri for name, uri in inputs.items() if S.DATASETS[name].fmt == "parquet"
        ]
        if not parquet_inputs:
            return PROFILES["lean"], "lean(auto: no parquet input)", 0.0

        probe = await workflow.execute_activity(
            probe_inputs,
            ProbeRequest(uris=parquet_inputs),
            task_queue=queues.COMPUTE,
            start_to_close_timeout=timedelta(minutes=2),
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            retry_policy=STEP_RETRY,
        )
        mb = round(probe.total_bytes / 1e6, 2)
        if mb >= inp.auto_threshold_mb:
            return PROFILES["roomy"], f"roomy(auto: {mb}MB)", mb
        return PROFILES["lean"], f"lean(auto: {mb}MB)", mb

    async def _with_sla(self, key: str, coro):
        """Race a durable timer against the step and record a soft breach.

        This is a workflow timer, so it is part of the history: restart the
        worker mid-step and the deadline is still exactly where it was. A cron
        watchdog in a sidecar cannot say that.
        """
        budget = S.STEPS[key].sla_seconds + self._sla_extra.get(key, 0)
        task = asyncio.ensure_future(coro)
        timer = asyncio.ensure_future(asyncio.sleep(budget))
        done, _ = await workflow.wait([task, timer], return_when=asyncio.FIRST_COMPLETED)

        if timer in done and task not in done:
            self._progress[key].sla_breached = True
            self._sla_breaches.append(f"{key} over {budget}s")
            workflow.logger.warning("SLA breach: %s still running after %ss", key, budget)
            # Soft SLA: notice, record, keep waiting. Cancelling here would be a
            # hard SLA, and `task.cancel()` is all it would take -- see
            # ASSIGNMENTS #17.
            return await task

        timer.cancel()
        return await task

    async def _quality_gate(
        self, inp: PipelineInput, key: str, produced: dict[str, str], result: StepResult
    ) -> None:
        prog = self._progress[key]
        known = dict(self._uris)
        known.update(produced)

        try:
            report = await workflow.execute_activity(
                run_quality_gate,
                DQRequest(run_id=inp.run_id, step_key=key, uris=known),
                task_queue=queues.COMPUTE,
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=HEARTBEAT_TIMEOUT,
                retry_policy=STEP_RETRY,
            )
        except ActivityError as exc:
            # The gate encodes its verdict in the error payload so that the
            # failing checks reach the metadata tables even though the activity
            # did not return normally.
            prog.state = StepState.FAILED.value
            prog.error = _describe(exc)
            payload = [_as_check(c) for c in _dq_payload(exc)]
            prog.dq_run = len(payload)
            prog.dq_failed = sum(1 for c in payload if not c.passed)
            await self._record_step(inp, key, result, prog, payload, [])
            raise

        prog.dq_run = len(report.checks)
        prog.dq_failed = sum(1 for c in report.checks if not c.passed)

        lineage = [
            LineageEvent(run_id=inp.run_id, step_key=key, upstream=up, downstream=out_name)
            for out_name, upstreams in S.STEPS[key].lineage.items()
            for up in upstreams
        ]
        await self._record_step(inp, key, result, prog, list(report.checks), lineage)

    async def _await_approval(self, inp: PipelineInput) -> None:
        if not inp.require_approval:
            return
        self._status = RunStatus.AWAITING_APPROVAL.value
        workflow.logger.info("waiting for approve_publish")
        if inp.approval_timeout_seconds <= 0:
            # Zero means "wait forever". A durable wait costs nothing: the
            # workflow is not resident in any worker's memory while it waits.
            await workflow.wait_condition(lambda: self._approved or self._aborted)
        else:
            try:
                await workflow.wait_condition(
                    lambda: self._approved or self._aborted,
                    timeout=timedelta(seconds=inp.approval_timeout_seconds),
                )
            except asyncio.TimeoutError:
                raise ApplicationError(
                    f"no approval within {inp.approval_timeout_seconds}s",
                    type="ApprovalTimeout",
                    non_retryable=True,
                ) from None
        if self._aborted:
            raise ApplicationError("aborted before publish", type="Aborted", non_retryable=True)
        self._status = RunStatus.RUNNING.value

    async def _publish(self, inp: PipelineInput, lake: str) -> None:
        targets = [
            PublishTarget(
                table=table,
                source_uri=S.DATASETS[ds].uri(lake, inp.business_date, inp.run_id),
                business_date=inp.business_date,
            )
            for ds, table in S.PUBLISHED.items()
        ]
        result = await workflow.execute_activity(
            publish_gold,
            PublishRequest(run_id=inp.run_id, targets=targets),
            task_queue=queues.WRITER,
            start_to_close_timeout=WRITER_TIMEOUT,
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            retry_policy=WRITER_RETRY,
        )
        self._published = [t.table for t in result.tables]
        self._rows_published = result.total_rows

    async def _compensate(self, inp: PipelineInput, reason: str) -> None:
        try:
            result = await workflow.execute_activity(
                compensate_publish,
                CompensateRequest(
                    run_id=inp.run_id, business_date=inp.business_date, reason=reason
                ),
                task_queue=queues.WRITER,
                start_to_close_timeout=WRITER_TIMEOUT,
                heartbeat_timeout=HEARTBEAT_TIMEOUT,
                retry_policy=WRITER_RETRY,
            )
            self._status = RunStatus.COMPENSATED.value
            workflow.logger.info(
                "compensation restored=%s deleted=%s", result.restored, result.deleted
            )
        except Exception as exc:  # noqa: BLE001
            # A failed compensation must not mask the original failure -- the
            # run is already going to fail, and swallowing this would hide the
            # fact that the warehouse is now in an unknown state.
            workflow.logger.error("COMPENSATION FAILED, warehouse may be dirty: %s", exc)

    async def _gate_on_control_signals(self) -> None:
        if self._aborted:
            raise ApplicationError("aborted by operator", type="Aborted", non_retryable=True)
        if self._paused:
            self._status = RunStatus.PAUSED.value
            workflow.logger.info("paused at phase %s", self._phase)
            await workflow.wait_condition(lambda: not self._paused or self._aborted)
            self._status = RunStatus.RUNNING.value
            if self._aborted:
                raise ApplicationError("aborted while paused", type="Aborted", non_retryable=True)

    # -- writer helpers ---------------------------------------------------

    async def _writer(self, fn, *args):
        return await workflow.execute_activity(
            fn,
            *args,
            task_queue=queues.WRITER,
            start_to_close_timeout=WRITER_TIMEOUT,
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            retry_policy=WRITER_RETRY,
        )

    async def _record(self, batch: MetaBatch) -> None:
        await workflow.execute_activity(
            write_metadata,
            batch,
            task_queue=queues.WRITER,
            start_to_close_timeout=WRITER_TIMEOUT,
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            retry_policy=WRITER_RETRY,
        )

    async def _record_step(
        self,
        inp: PipelineInput,
        key: str,
        result: StepResult,
        prog: StepProgress,
        dq_checks: list,
        lineage: list[LineageEvent],
    ) -> None:
        """Write one step's bookkeeping.

        The checks and lineage arrive as arguments rather than from instance
        state, because the steps inside a phase run *concurrently* on one
        workflow object. An accumulator field here means step B's checks get
        flushed under step A's name -- which is exactly what happened, and what
        `meta.dq_results`' primary key then silently spread across four rows.
        """
        spec = S.STEPS[key]
        primary = result.primary()
        await self._record(
            MetaBatch(
                step_metrics=[
                    StepMetric(
                        run_id=inp.run_id,
                        step_key=key,
                        layer=spec.layer,
                        state=prog.state,
                        attempts=result.attempts or 1,
                        rows_in=result.rows_in,
                        rows_out=result.rows_out,
                        rows_rejected=result.rows_rejected,
                        duration_seconds=result.duration_seconds,
                        bytes_out=primary.bytes if primary else 0,
                        threads=prog.threads,
                        memory_limit=prog.memory_limit,
                        sla_seconds=prog.sla_seconds + self._sla_extra.get(key, 0),
                        sla_breached=prog.sla_breached,
                        output_uri=primary.uri if primary else "",
                        error=prog.error,
                    )
                ],
                dq_run_id=inp.run_id,
                dq_step_key=key,
                dq_checks=list(dq_checks),
                lineage=list(lineage),
            )
        )

    async def _finalise(self, inp: PipelineInput, started, error: str = "") -> None:
        await self._record(
            MetaBatch(
                run=RunRecord(
                    run_id=inp.run_id,
                    workflow_id=workflow.info().workflow_id,
                    business_date=inp.business_date,
                    mode=inp.mode,
                    status=self._status,
                    started_at=started.isoformat(sep=" ", timespec="seconds"),
                    finished_at=workflow.now().isoformat(sep=" ", timespec="seconds"),
                    rows_published=self._rows_published,
                    error=error,
                )
            )
        )

    def _output(self, inp: PipelineInput, started) -> PipelineOutput:
        ok = sum(1 for p in self._progress.values() if p.state == StepState.SUCCEEDED.value)
        bad = sum(1 for p in self._progress.values() if p.state == StepState.FAILED.value)
        return PipelineOutput(
            run_id=inp.run_id,
            business_date=inp.business_date,
            status=self._status,
            steps_succeeded=ok,
            steps_failed=bad,
            rows_published=self._rows_published,
            duration_seconds=round((workflow.now() - started).total_seconds(), 2),
            published_tables=list(self._published),
            sla_breaches=list(self._sla_breaches),
        )


# -- small helpers, kept out of the class so they stay obviously pure --------


def _describe(exc: BaseException) -> str:
    cause = getattr(exc, "cause", None)
    return f"{type(exc).__name__}: {exc}" + (f" <- {cause}" if cause else "")


def _dq_payload(exc: ActivityError) -> list[dict]:
    """Pull the DQReport out of a failed gate's ApplicationError details."""
    cause = getattr(exc, "cause", None)
    details = getattr(cause, "details", None) or ()
    for item in details:
        checks = getattr(item, "checks", None)
        if checks is None and isinstance(item, dict):
            checks = item.get("checks")
        if checks:
            return [c if isinstance(c, dict) else vars(c) for c in checks]
    return []


def _as_check(raw):
    from duckflow.shared import DQCheck

    return DQCheck(**raw) if isinstance(raw, dict) else raw
