"""Types that cross the workflow/activity boundary.

Everything here is a plain dataclass or a str-enum, because every one of these
objects is serialised into the Temporal event history and deserialised again on
replay, possibly by a different process running a newer build of this code.

Two rules that shape the whole module:

1. **Nothing big travels.** Temporal's default gRPC payload limit is 2 MB and
   the practical advice is to stay far under it. So an activity never returns a
   DataFrame or an Arrow table -- it returns *row counts and URIs*. The data
   stays in object storage; the history carries the receipt.
2. **Nothing unversioned travels.** Adding a field with a default is safe;
   removing or renaming one breaks the replay of in-flight workflows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Mode(str, Enum):
    """How much room DuckDB is given for a step.

    DuckDB has no cluster to size, so the only tuning surface is the process:
    `memory_limit`, `threads`, and whether a spill directory exists. AUTO picks
    per step from a probe of the input's Parquet footprint -- the DuckDB
    analogue of choosing an engine at runtime.
    """

    LEAN = "lean"
    ROOMY = "roomy"
    AUTO = "auto"


class StepState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    COMPENSATED = "compensated"


class RunStatus(str, Enum):
    RUNNING = "running"
    AWAITING_APPROVAL = "awaiting_approval"
    PAUSED = "paused"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    COMPENSATED = "compensated"
    ABORTED = "aborted"


class Severity(str, Enum):
    ERROR = "error"
    WARN = "warn"


# --------------------------------------------------------------------------
# Workflow input / output
# --------------------------------------------------------------------------


@dataclass
class PipelineInput:
    business_date: str
    run_id: str
    lake_uri: str = "s3://lake"
    """Where the run reads and writes. An *input*, not a setting read from the
    environment: every path in the history is then a pure function of the
    input, so a replay recomputes exactly the URIs the original run used."""
    mode: str = Mode.AUTO.value
    meters: int = 20000
    auto_threshold_mb: int = 16
    """Inputs at or above this many megabytes get ROOMY under Mode.AUTO.

    Sized for this fixture, where the biggest input is ~23 MB. In a real
    deployment the interesting threshold is in the hundreds of MB -- the
    number matters less than the fact that it is decided per step, at
    runtime, from a measurement rather than from a guess made months ago."""
    require_approval: bool = False
    approval_timeout_seconds: int = 900
    seed: bool = True
    """Generate the landing zone first. False for a rerun over existing data."""
    publish: bool = True
    fail_step: str = ""
    """Chaos switch: make this step raise on its first attempts."""
    inject: str = ""
    """Data defect to inject into the landing zone, e.g. `bad_tariff`."""
    fail_after_publish: bool = False
    """Chaos switch for the saga's most important branch.

    Every other failure mode in this pipeline happens *before* the publish, so
    compensation finds nothing to undo and correctly does nothing. Only a
    failure after a successful publish exercises the restore path -- and a code
    path that is never exercised is a code path that does not work."""


@dataclass
class PipelineOutput:
    run_id: str
    business_date: str
    status: str
    steps_succeeded: int
    steps_failed: int
    rows_published: int
    duration_seconds: float
    published_tables: list[str] = field(default_factory=list)
    sla_breaches: list[str] = field(default_factory=list)
    error: str = ""


# --------------------------------------------------------------------------
# Step execution
# --------------------------------------------------------------------------


@dataclass
class DuckSettings:
    """The per-activity DuckDB process configuration. Resolved by the workflow
    (so it is recorded in history and identical on replay) and applied by the
    activity."""

    threads: int = 4
    memory_limit: str = "2GB"
    temp_directory: bool = True
    preserve_insertion_order: bool = False
    """False lets DuckDB stream Parquet writes without buffering the whole
    result to preserve row order. Almost always right for a pipeline; wrong if
    a downstream consumer depends on file row order."""


@dataclass
class StepRequest:
    run_id: str
    business_date: str
    step_key: str
    duck: DuckSettings
    inputs: dict[str, str]
    """dataset name -> URI glob to read."""
    outputs: dict[str, str]
    """dataset name -> URI to write."""
    fail: bool = False
    attempt_hint: int = 0


@dataclass
class OutputRef:
    dataset: str
    uri: str
    rows: int = 0
    bytes: int = 0


@dataclass
class StepResult:
    step_key: str
    state: str
    rows_in: int = 0
    rows_out: int = 0
    rows_rejected: int = 0
    duration_seconds: float = 0.0
    attempts: int = 1
    """Which attempt produced this result. The workflow cannot know -- only the
    activity sees `activity.info().attempt` -- so it travels back in the result."""
    peak_memory_note: str = ""
    outputs: list[OutputRef] = field(default_factory=list)
    error: str = ""

    def primary(self) -> OutputRef | None:
        return self.outputs[0] if self.outputs else None


@dataclass
class ProbeRequest:
    uris: list[str]


@dataclass
class ProbeResult:
    total_bytes: int = 0
    total_rows: int = 0
    files: int = 0
    per_uri: dict[str, int] = field(default_factory=dict)


@dataclass
class SeedRequest:
    business_date: str
    run_id: str
    meters: int
    inject: str = ""


@dataclass
class SeedResult:
    datasets: dict[str, str] = field(default_factory=dict)
    rows: dict[str, int] = field(default_factory=dict)


# --------------------------------------------------------------------------
# Quality
# --------------------------------------------------------------------------


@dataclass
class DQRequest:
    run_id: str
    step_key: str
    uris: dict[str, str]
    """dataset name -> URI of the produced data, so checks read what was written
    rather than re-deriving it."""


@dataclass
class DQCheck:
    name: str
    kind: str
    dataset: str
    column: str = ""
    severity: str = Severity.ERROR.value
    passed: bool = True
    observed: str = ""
    expected: str = ""
    detail: str = ""


@dataclass
class DQReport:
    step_key: str
    checks: list[DQCheck] = field(default_factory=list)

    @property
    def failed_errors(self) -> list[DQCheck]:
        return [c for c in self.checks if not c.passed and c.severity == Severity.ERROR.value]


# --------------------------------------------------------------------------
# Publish / compensation -- the single-writer side
# --------------------------------------------------------------------------


@dataclass
class PublishTarget:
    table: str
    source_uri: str
    business_date: str


@dataclass
class PublishRequest:
    run_id: str
    targets: list[PublishTarget]


@dataclass
class PublishedTable:
    table: str
    business_date: str
    rows_deleted: int = 0
    rows_written: int = 0
    previous_run_id: str = ""
    previous_source_uri: str = ""


@dataclass
class PublishResult:
    run_id: str
    tables: list[PublishedTable] = field(default_factory=list)
    total_rows: int = 0


@dataclass
class CompensateRequest:
    run_id: str
    business_date: str
    reason: str


@dataclass
class CompensateResult:
    restored: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    note: str = ""


# --------------------------------------------------------------------------
# Metadata -- every one of these is written through the WRITER queue
# --------------------------------------------------------------------------


@dataclass
class RunRecord:
    run_id: str
    workflow_id: str
    business_date: str
    mode: str
    status: str
    started_at: str
    finished_at: str = ""
    rows_published: int = 0
    error: str = ""


@dataclass
class StepMetric:
    run_id: str
    step_key: str
    layer: str
    state: str
    attempts: int = 0
    rows_in: int = 0
    rows_out: int = 0
    rows_rejected: int = 0
    duration_seconds: float = 0.0
    bytes_out: int = 0
    threads: int = 0
    memory_limit: str = ""
    sla_seconds: int = 0
    sla_breached: bool = False
    output_uri: str = ""
    error: str = ""


@dataclass
class LineageEvent:
    run_id: str
    step_key: str
    upstream: str
    downstream: str
    columns: str = ""


@dataclass
class MetaBatch:
    """One trip to the writer carries everything the workflow has accumulated.

    Batching matters more here than in a server-backed warehouse: every write is
    a turn in a one-slot queue, so chatty bookkeeping becomes head-of-line
    blocking for the publish that actually matters.
    """

    run: RunRecord | None = None
    step_metrics: list[StepMetric] = field(default_factory=list)
    dq_checks: list[DQCheck] = field(default_factory=list)
    dq_run_id: str = ""
    dq_step_key: str = ""
    lineage: list[LineageEvent] = field(default_factory=list)
    sla_events: list[str] = field(default_factory=list)


@dataclass
class SLAEvent:
    step_key: str
    sla_seconds: int
    elapsed_seconds: float
    note: str = ""
