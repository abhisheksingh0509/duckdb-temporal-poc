# Learning DuckDB and Temporal with this repo

This is the study guide. It teaches two technologies that are unrelated in
theory and awkward together in practice, using this pipeline as the worked
example. Part A is DuckDB, Part B is Temporal, and **Part C is the part you
cannot get from either project's own documentation** — what happens when you put
them together.

Work through it with the stack running (`make up`) and the Temporal UI open at
<http://localhost:8234>. Exercises are in [ASSIGNMENTS.md](ASSIGNMENTS.md); each
section below names the ones that drill it. For the order to do all this in, see
[ROADMAP.md](ROADMAP.md).

Rough time: 3–4 hours to read and poke; 15–25 hours to do the assignments.

---

# Part A — DuckDB as a pipeline engine

## A0. The one idea

**DuckDB is a library, not a server.** There is no daemon, no cluster, no
connection pool, no network protocol. `import duckdb` and you have an
analytical database inside your process, with your process's memory and your
process's CPUs.

Everything good and everything painful follows from that:

| Because it is in-process… | …you get | …and you give up |
|---|---|---|
| no network between plan and data | microsecond query start, no serialisation tax | nothing to scale horizontally |
| the database is a file | copy it, ship it, diff it, delete it | one read-write process at a time |
| the database can be nothing at all | `:memory:` costs nothing, so every task can have one | no shared state between tasks |
| the engine is a dependency | version it in `requirements.txt` | you upgrade it, not an ops team |

The last row is the one people underrate. A DuckDB "cluster upgrade" is a pip
pin. That is a genuine operational difference, not a talking point.

**Assignments: 2, 4.**

## A1. Connections, and the order you configure them

Read `duck/session.py:42` (`_apply`) and `duck/session.py:95` (`connect`). The
order is not cosmetic:

```python
SET extension_directory = '…'   # before LOAD, or LOAD looks in the wrong place
SET threads = 4                  # before the first query builds a pipeline
SET memory_limit = '512MB'       # before the first query allocates
SET preserve_insertion_order = false
SET temp_directory = '/data/duck-tmp'
LOAD httpfs                      # before any s3:// path is parsed
CREATE OR REPLACE SECRET lake (…)  # before any s3:// path is *read*
```

Get it wrong and the errors are misleading — a missing secret reads as an HTTP
403, a missing `LOAD` reads as "unknown protocol", and a `memory_limit` set after
the first query silently does not apply to it.

Two design choices worth arguing about:

**One connection per activity, not per worker.** A `:memory:` connection costs
single-digit milliseconds. Owning one per unit of work means an activity can be
cancelled by interrupting *its* connection without touching anything else the
worker is doing — and, for the warehouse file, it means the file is unlocked
between activities so a reader can get in (§A7).

**`connect()` is a context manager.** DuckDB will not release a file lock held
by a connection that was never closed, and "the process is about to exit anyway"
stops being true the moment the process is a long-lived worker.

**Assignments: 4, 9.**

## A2. Reading: paths are relations

`duck/session.py:148` (`register_source`) turns a URI into a plain name:

```python
CREATE OR REPLACE VIEW bronze_readings AS
SELECT * FROM read_parquet('s3://lake/bronze/…/run=abc/data.parquet',
                            union_by_name = true)
```

That is why every SQL statement in `steps.py` reads `FROM bronze_readings` and
not a URI. The SQL stays about the data; the URI — which encodes the run id and
is therefore the interesting thing to log — stays in the activity.

Things worth knowing about the readers:

- `read_parquet` takes a glob, a list, or a Hive-partitioned root. `hive_partitioning = true` turns `dt=2026-08-01` in a path into a *column*.
- `union_by_name = true` matches columns by name across files instead of by position. Without it, one file with reordered columns corrupts the whole read silently.
- `read_csv` sniffs types from a sample. `sample_size = -1` reads the whole file to decide, which is what you want for a fixture and not what you want for a 40 GB file. `types = {…}` overrides the sniffer entirely, and in production that is usually the right answer.
- Reading Parquet costs only the columns you select and only the row groups whose statistics survive your `WHERE`. That is why `parquet_bytes()` (`session.py:201`) and the DQ checks are cheap.

**Assignments: 5, 10.**

## A3. Writing: COPY, and the flag that decides whether LEAN survives

`duck/session.py:182` (`copy_to_parquet`):

```sql
COPY (<select>) TO 's3://…/data.parquet'
     (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880)
```

- **ZSTD** — these files are read far more than written. Roughly 2× smaller than snappy, slightly slower to write, faster to read when you are I/O-bound, which over S3 you are.
- **`ROW_GROUP_SIZE`** pinned — a reader's parallelism is bounded by row-group count. Leaving it to default means downstream parallelism depends on how much memory the *writer* happened to have, which is a genuinely confusing bug to chase.
- **`preserve_insertion_order = false`** — DuckDB buffers a complete result to keep rows in order when writing. Turning it off lets the write stream. On the LEAN profile (512 MB) it is the difference between spilling politely and not finishing. It is safe here because every consumer sorts or aggregates; it is *not* safe if anything downstream depends on file row order.

Note what `copy_to_parquet` does *not* do: it does not append. Every step output
is one file at one immutable `run=<run_id>` path. That is what makes rollback
possible (§C3), and it is a deliberate choice, not a simplification.

**Assignments: 9, 10.**

## A4. The SQL that earns its keep

This is the part to steal. Each of these replaces a pattern you have written by
hand, and the replacement is shorter *and* harder to get wrong.

### `QUALIFY` — filter on a window function without a subquery
`steps.py:288` (`silver_cleanse`):

```sql
SELECT * FROM bronze_readings
QUALIFY row_number() OVER (
    PARTITION BY meter_id, reading_ts ORDER BY src_updated_at DESC
) = 1
```

The ANSI version needs a subquery or CTE purely to give the window function a
name to filter on. `QUALIFY` is to window functions what `HAVING` is to
aggregates. For "keep the latest version of each key" — the single most common
operation in a silver layer — it is the difference between five lines and one.

### Named `WINDOW` — say the frame once
```sql
SELECT *,
       lag(cumulative_kwh) OVER w AS prev_kwh,
       lag(reading_ts)     OVER w AS prev_ts,
       cumulative_kwh - lag(cumulative_kwh) OVER w AS interval_kwh
FROM deduped
WINDOW w AS (PARTITION BY meter_id ORDER BY reading_ts)
```
Three `lag()` calls that *must* share a frame. Written out three times, they can
silently drift apart in a future edit.

### `ASOF JOIN` — "the row in force at that moment"
`steps.py:373` (`silver_enrich`) — the reason this pipeline is worth writing in
DuckDB at all:

```sql
FROM with_meter w
ASOF JOIN bronze_tariffs t
      ON w.tariff_plan = t.tariff_plan
     AND w.reading_ts >= t.valid_from
```

Each reading gets the most recent tariff at or before its timestamp. Written
conventionally that is a correlated `max(valid_from)` subquery or a self-join
plus a dedupe; DuckDB compiles it to one sorted merge. Any time you join a fact
to a slowly-changing dimension, a price curve, an exchange rate or a
configuration history, this is the join you wanted.

**`ASOF LEFT JOIN` is not a detail.** The weather join uses `LEFT` because a
missing observation must not delete a consumption row. An inner ASOF JOIN there
is a textbook silent-data-loss bug, and the `enrich_preserves_volume`
expectation exists to catch exactly that class of mistake.

### `GROUP BY ALL` — group by everything you did not aggregate
```sql
SELECT business_date, region, count(DISTINCT meter_id) AS meters, sum(…) AS kwh
FROM silver_enriched
GROUP BY ALL
```
Removes the most common refactoring bug in analytics SQL: adding a dimension to
the SELECT and forgetting to add it to GROUP BY. (`ORDER BY ALL` exists too.)

### `* EXCLUDE` / `* REPLACE` — subtract from a star
```sql
SELECT * EXCLUDE (price_valid_from, weather_observed_at, src_updated_at),
       round(interval_kwh * price_per_kwh, 5) AS cost_eur
FROM with_weather
```
Dropping three columns from fourteen without listing eleven. `REPLACE` does the
same for overwriting one in place.

### `FILTER (WHERE …)` — a conditional aggregate that reads like one
```sql
sum(interval_kwh) FILTER (WHERE is_peak) AS peak_kwh
```
versus `sum(CASE WHEN is_peak THEN interval_kwh END)`. Same plan, and the first
one cannot be misread.

### `RANGE … INTERVAL` frames — a real rolling window
```sql
avg(kwh) OVER (PARTITION BY region ORDER BY business_date
               RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW)
```
`ROWS` counts rows; `RANGE` counts *values of the ordering column*. If a region
is missing a day, `ROWS BETWEEN 6 PRECEDING` quietly reaches back seven
calendar days. `RANGE … INTERVAL` does not. (In this repo the frame only ever
sees one partition — see §C7 and Assignment 13.)

### `ANTI JOIN` / `SEMI JOIN` — set membership that handles NULLs
```sql
SELECT count(*) FROM child c
ANTI JOIN parent p ON c.meter_id = p.meter_id
```
`NOT IN (SELECT …)` returns *nothing* if the subquery contains a single NULL.
That trap has broken more referential checks than any other single cause.
`ANTI JOIN` says what you meant.

### Also worth knowing, used in the fixture
`range()` and `generate_series()` as table functions, `setseed()` for
reproducible `random()`, list literals with 1-based indexing
(`['a','b','c'][2]`), `to_minutes()`/`to_hours()`/`to_days()` for interval
arithmetic on an expression, `any_value()` for a functionally-dependent column
you do not want to group by, and `SUMMARIZE <table>` — which you should run on
`silver_enriched` right now.

**Assignments: 5, 6, 7, 8, 11, 12, 13.**

## A5. Memory, spilling, and "larger than memory"

DuckDB can process more data than it has RAM, but only if you let it:

```python
SET memory_limit = '512MB'
SET temp_directory = '/data/duck-tmp'
SET max_temp_directory_size = '8GB'
```

Without `temp_directory`, a query that exceeds `memory_limit` fails. With it,
hash joins and sorts spill to disk and the query completes more slowly. Without
`max_temp_directory_size`, a runaway spill fills the volume and takes the
*writer* down with it — with it, the query fails and Temporal retries it, which
is a much better failure.

`make run-lean` vs `make run-roomy` is this knob, and `_memory_note()`
(`activities/core.py:237`) reports what actually happened:

```sql
SELECT sum(memory_usage_bytes), sum(temporary_storage_bytes) FROM duckdb_memory()
```

That second number is the only honest answer to "did this step spill?".

The important mental shift: with a cluster you scale *out* and the framework
decides memory per executor. With DuckDB you scale *the process*, and it is a
per-step decision — which is why `_size_step` (`workflows/pipeline.py:366`)
exists at all.

**Assignments: 4, 9.**

## A6. Metadata without scanning

```sql
SELECT sum(num_rows) FROM parquet_file_metadata('s3://…/*.parquet');  -- footers
SELECT sum(total_compressed_size) FROM parquet_metadata('s3://…');    -- row groups
SELECT * FROM glob('s3://lake/**');
```

`probe_inputs` (`activities/core.py:126`) uses the first two to size a step in
AUTO mode. Sizing 400 MB of input costs a few range requests instead of a scan —
which is what makes runtime sizing a measurement rather than a guess.

**Assignment: 10.**

## A7. The single-writer rule, precisely

This is the constraint the entire architecture is arranged around, and the
common one-line version of it is wrong in a way that matters.

Run `make demo-contention`. Two different things happen:

**Across processes: the file lock refuses the open.** Not only a second writer —
a *read-only* open is refused too, while a writer holds the file. So:

- the writer opens the database per activity and closes it (`warehouse.py:_writer`), leaving the file free between activities;
- every reader goes through `session.open_read_only()` (`session.py:123`), which backs off and retries rather than assuming.

**Within one process: connections share an instance and are allowed.** They go
through DuckDB's optimistic transaction manager instead, so the failure moves to
commit time:

```
TransactionContext Error: Conflict on tuple deletion!
```

This is the one that would actually bite a writer worker with
`max_concurrent_activities` turned up — not a lock error, a conflict error, at
commit, on the rows two publishes of the same date both tried to replace. And
retrying it would paper over a second problem: `publish()` reads the previous
owner of a date *before* deleting it, which is a read-modify-write that
serialisation removes and retries do not.

**Assignment: 17.**

## A8. Transactions, constraints and `INSERT … BY NAME`

`duck/warehouse.py` uses more of DuckDB-as-a-database than most pipelines do:

- **`PRIMARY KEY`** on `meta.step_metrics (run_id, step_key)` and friends. Not decoration: it is what makes `INSERT OR REPLACE` an upsert, which is what makes the metadata writes safe to replay (§C2).
- **`INSERT … ON CONFLICT … DO UPDATE`** for `meta.runs`, where only some columns should move.
- **`INSERT INTO … BY NAME SELECT * FROM read_parquet(…)`** — matches columns by name, so reordering a gold SELECT does not silently shift data into the wrong columns.
- **Explicit `BEGIN`/`COMMIT`/`ROLLBACK`** (`warehouse.py:317`). DuckDB has no nested transactions, so each publish target gets its own — which means a failure on the second table leaves the first committed *and* leaves `publish_log` agreeing with that, so compensation undoes exactly what landed.
- **`CREATE TABLE … AS SELECT * FROM read_parquet(…) LIMIT 0`** — schema-on-first-publish from the Parquet footer. The alternative is hand-written DDL that drifts the first time someone adds a column.

**Assignments: 18, 23.**

## A9. Extensions and secrets

```sql
CREATE OR REPLACE SECRET lake (
    TYPE s3, KEY_ID '…', SECRET '…', REGION '…',
    ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false
);
```

A secret rather than the legacy `SET s3_access_key_id`: secrets are scoped, are
not readable back out of the connection, and are the only form that supports
per-prefix credentials when the lake grows a second bucket. Note `ENDPOINT` takes
`host:port` with no scheme, and `URL_STYLE 'path'` is required for MinIO.

Extensions are baked into the image (`docker/worker.Dockerfile`), because an
activity that downloads one on first use fails in an air-gapped network and
times out in a slow one — once per fresh container.

## A10. What DuckDB is not

Say these out loud before proposing it anywhere:

- **Not a concurrent-write store.** One writer process. If your workload is many writers, this is the wrong tool and no orchestrator fixes it.
- **Not distributed.** No shuffle across machines. A join whose build side exceeds one machine's disk is out of scope.
- **Not a catalog.** No schema registry, no snapshots, no time travel. §C3 is what it costs to build the minimum substitute.
- **Not a server.** No connection pool, no query queue, no admission control. Those are *your* problem — and in this repo, Temporal is the answer to all three.

---

# Part B — Temporal as a data orchestrator

## B0. The one idea

Temporal is **durable execution**. You write ordinary async Python; Temporal
guarantees the function survives process death, machine death and multi-day
waits, resuming exactly where it was with its local variables intact.

The mechanism is worth internalising early, because every rule follows from it:

> Temporal does not snapshot your workflow's memory. It records an **event
> history** of everything that happened to it — activity scheduled, activity
> completed, timer fired, signal received — and when it needs the workflow's
> state back it **re-executes your workflow function from line 1**, feeding it
> the recorded results instead of doing the work again. When it runs out of
> history it hands control back to your code and the function continues live.

That re-execution is **replay**. It happens on worker restart, on a different
machine, on a query, on any resumption after the workflow was evicted from
cache. It is invisible and it is constant.

Two consequences you will meet in every section:

1. **Workflow code must be deterministic.** Replay must reproduce the same sequence of decisions. `datetime.now()`, `random`, reading a file, reading an environment variable, iterating an unordered set — all forbidden inside a workflow, because replay would take a different branch and diverge from the history.
2. **Everything non-deterministic is an activity.** An activity is a plain function that runs *outside* replay. Its result is recorded once; on replay the recorded result is returned without re-running it.

So: **the workflow decides, activities do.** Here, `workflows/pipeline.py`
decides (what runs, in what order, with what memory limit, what to do when it
breaks) and `activities/core.py` does (opens DuckDB, touches S3, writes the
warehouse).

**Assignment: 1.**

## B1. The four objects

| Object | What it is | Here |
|---|---|---|
| **Workflow** | Durable, deterministic, replayable orchestration function | `workflows/pipeline.py`, `workflows/backfill.py` |
| **Activity** | One unit of side-effecting work; retried, timed out, heartbeated | `activities/core.py` — 8 of them |
| **Worker** | A process polling a task queue, executing workflow tasks and/or activity tasks | `worker.py`, run twice: `compute` and `writer` |
| **Task queue** | Just a name. Workers poll it; callers target it | `queues.py` — `duck-compute-tq`, `duck-writer-tq` |

The Temporal **server** holds histories, timers and queues. It never runs your
code. Your workers do, and they connect *out* to the server — the server never
calls in. That is why a worker can live in a private network, and why "scaling
Temporal" usually means scaling workers.

A **workflow execution** is identified by `workflow_id` + `run_id`. The
`workflow_id` is yours and is unique among *running* workflows in a namespace —
this repo uses `meter-<date>-<hhmmss>` (`cli.py`), while the backfill uses a
stable `backfill-<start>-to-<end>` so re-issuing it collides deliberately. The
`run_id` changes on retry and on continue-as-new.

## B2. Reading this workflow

Open `workflows/pipeline.py:218` and read `run()`. It is short:

```python
await self._writer(ensure_warehouse)
await self._record(MetaBatch(run=RunRecord(...)))
await self._materialise_landing(inp, lake)
for phase_name, keys in S.PHASES:
    await self._gate_on_control_signals()
    await self._run_phase(inp, lake, keys)
if inp.publish:
    await self._await_approval(inp)
    await self._publish(inp, lake)
```

Everything else in that file is one of: fan-out (`_run_phase:307`), sizing
(`_size_step:366`), the SLA timer (`_with_sla:400`), the gate
(`_quality_gate:424`), the saga (`_compensate:510`), or bookkeeping. Read them
in that order.

Note what the workflow never does: open a connection, read an environment
variable, or look at a clock other than `workflow.now()`.

## B3. Determinism, concretely

The sandbox will catch some violations and not others. The ones that matter
here:

| Forbidden in workflow code | Use instead |
|---|---|
| `datetime.now()`, `time.time()` | `workflow.now()` |
| `random`, `uuid4()` | `workflow.random()`, `workflow.uuid4()` |
| `os.environ`, reading a file | pass it in the workflow input |
| `asyncio.sleep` from stdlib semantics | it *is* patched — it becomes a durable timer |
| iterating a `set` | sort it, or use a list |
| `threading`, real IO | an activity |

`steps.py` is imported *inside* the workflow, which is why it is stdlib-only and
why `render_context()` takes its values as arguments rather than reading
settings. That constraint turned out to improve the design: the SQL became pure
data with no configuration baked into it.

`config.py` carries a docstring saying it must never be imported by workflow
code. Believe it.

**Assignment: 21.**

## B4. Retries and error typing

`workflows/pipeline.py:72`:

```python
STEP_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=["DataQualityError", "ConfigurationError"],
)
```

The judgement is in that last line. A transient S3 error or a spilled query that
hit a full temp directory deserves a retry. **A failed expectation does not**:
the input Parquet is immutable, so attempt two reads the same bytes and reaches
the same verdict. Retrying would turn a five-second failure into a ninety-second
one and tell nobody anything.

The activity opts in by *typing* its error (`activities/core.py:258`):

```python
raise ApplicationError(
    f"quality gate failed for {req.step_key}: {detail}",
    report,                      # the payload travels with the failure
    type="DataQualityError",     # matched against non_retryable_error_types
    non_retryable=True,
)
```

Note the second positional argument: failure *details*. The DQ report reaches
the workflow through the exception, which is how the failing checks still get
recorded in `meta.dq_results` even though the activity did not return normally.

`WRITER_RETRY` is different on purpose — more attempts, shorter intervals. The
writer's work is small, idempotent and on the critical path.

**Assignment: 14.**

## B5. Timeouts and heartbeats

Four timeouts exist; two matter here.

- **`start_to_close_timeout`** — how long one attempt may take. Set generously (20 minutes for a step): too tight and a legitimately slow step is killed and retried, which is strictly worse than waiting.
- **`heartbeat_timeout`** — 30 s. The activity must call `activity.heartbeat()` more often than this, or Temporal declares it dead and retries it. This is how a crashed worker is detected in 30 seconds instead of 20 minutes.

`_with_heartbeat` (`activities/core.py:79`) is the machinery:

```python
beater = asyncio.create_task(beat())          # heartbeats every 5s on the loop
return await asyncio.to_thread(fn, live)      # DuckDB blocks in a worker thread
```

The heartbeat payload is the current stage (`"write silver_enriched"`), so the
Temporal UI shows which statement a slow step is on without anyone reading a
log. Try it: start a big run and look at the activity's heartbeat details.

## B6. Timers and SLAs

`_with_sla` (`workflows/pipeline.py:400`) races a timer against the step:

```python
task  = asyncio.ensure_future(coro)
timer = asyncio.ensure_future(asyncio.sleep(budget))
done, _ = await workflow.wait([task, timer], return_when=asyncio.FIRST_COMPLETED)
```

`asyncio.sleep` inside a workflow is not a sleep — it is a **durable timer**
recorded in the history and held by the server. Restart the worker mid-step and
the deadline is still exactly where it was. A sidecar cron watchdog cannot say
that, and neither can a `threading.Timer`.

This is a *soft* SLA: it records a breach and keeps waiting. Making it hard is
`task.cancel()`, which is Assignment 15 — and the reason the activity is
interruptible at all (§A7, §C5).

## B7. Signals, queries, updates

Three ways to talk to a running workflow, and they are not interchangeable.

| | direction | blocks? | can mutate? | here |
|---|---|---|---|---|
| **Signal** | in | no, fire-and-forget | yes | `approve_publish`, `pause`, `resume`, `abort` |
| **Query** | out | yes, returns a value | **no** | `progress`, `plan` |
| **Update** | both | yes, returns a value | yes | `extend_sla` |

- **Signals are durable.** Signal a workflow whose worker is down and it is parked in the history, delivered when a worker returns.
- **Queries must not mutate.** A query runs against a replayed workflow; mutating during one corrupts state. `progress()` only reads.
- **Updates can validate before accepting.** `extend_sla.validator` rejects an unknown step key at call time, so the caller gets an error instead of silence.

Try all three:

```bash
make run-approval                      # parks awaiting approval
make watch ID=approval-demo            # query
make extend-sla ID=approval-demo STEP=silver_enrich SECS=120   # update
make approve ID=approval-demo          # signal
```

The approval wait is the clearest demonstration of durable execution in the
repo: while parked, the workflow occupies no worker memory at all. `make down`,
go to lunch, `make up`, `make approve` — it resumes.

**Assignments: 3, 20.**

## B8. Saga and compensation

Temporal has no transactions across activities. What it has is: if you can
express the undo, it will durably run it.

```python
except Exception as exc:
    self._status = RunStatus.FAILED.value
    await self._compensate(inp, _describe(exc))
    await self._finalise(inp, started, error=...)
    raise
```

Two details that are easy to get wrong and are right here:

- **The compensation is itself an activity**, so it retries, times out and appears in the history. A compensation implemented as a `finally:` block in workflow code would be re-executed on replay and could not retry.
- **A failed compensation must not mask the original failure** (`pipeline.py:510`). It logs loudly and the original exception still propagates, because the run was going to fail anyway and swallowing this would hide that the warehouse is now in an unknown state.

What compensation actually does is §C3.

**Assignment: 19.**

## B9. Child workflows and continue-as-new

`workflows/backfill.py`. Two mechanics that only appear at scale:

**Child workflows, not activities.** Each date is its own `MeterPipeline`
execution with its own history, retries, gate and compensation. A 90-day
backfill is 90 independently inspectable runs plus one coordinator, rather than
one history with 30 000 events in it.

**Continue-as-new.** A history is capped (hard limit 51 200 events; the server
warns around 10 000). `workflow.continue_as_new(...)` ends the current execution
and starts a fresh one with the same workflow id, carrying only the small state
that matters — here, which dates are done. To the outside it is still one
workflow. `completed` is what makes the backfill resumable: a crash between
checkpoints loses at most `checkpoint_every` dates and never re-publishes a
date that already landed.

The concurrency limit is the other half. Child workflows are cheap, but each one
publishes through the *single* writer queue — so launching 90 at once just makes
90 things queue behind one slot, with longer timers and a worse failure mode
than launching four at a time.

**Assignment: 22.**

## B10. Schedules

`make schedule` creates a Temporal Schedule — durable cron with backfill,
overlap policy, pause, and a jitter window, all held by the server rather than
by a crontab on a box someone will eventually reimage. It is created **paused**,
because a schedule that starts firing the moment you create it is how a demo
becomes forty backfill runs.

## B11. Versioning, and the error you are about to cause

Change workflow code while a workflow is in flight and its replay will diverge
from its history:

```
NonDeterminismError: Workflow activation completion failed
```

This is not a bug, it is the system working. The standard responses:

1. **Terminate in-flight workflows** before changing workflow code. Fine in development, and what `make restart` assumes.
2. **`workflow.patched("my-change")`** — branch on it, let old histories take the old path, remove the patch once nothing old is running.
3. **Worker Versioning** — pin builds to workers. The right answer in production, out of scope here.

Activity code is unaffected: activities are not replayed, only their recorded
results are. So changing SQL in `steps.py`… is a workflow change, because
`steps.py` is imported into the workflow. Changing the *body* of an activity is
not. Knowing which is which is the skill.

**Assignment 21** makes you cause this on purpose.

## B12. Reading an event history

Open any run in the UI and switch to the full JSON history. Learn to find:

- `WorkflowExecutionStarted` — the input, verbatim.
- `WorkflowTaskScheduled` / `Started` / `Completed` triples — each one is a slice of your workflow code actually executing. Count them; they tell you how many times the function was re-entered.
- `ActivityTaskScheduled` / `Started` / `Completed` — the middle one carries the worker identity, the last one carries the result payload.
- `TimerStarted` / `TimerFired` / `TimerCanceled` — your SLAs.
- `WorkflowExecutionSignaled` — your approval.
- `MarkerRecorded` — side effects and patches.

The single most useful exercise in this repo is Assignment 1: read one history
end to end and work out, from the event order alone, when your workflow code was
running and when it was not.

---

# Part C — The intersection

Everything above is in the two projects' own documentation. This part is not.

## C1. A task queue is a concurrency contract

The idea worth taking away: **when your engine has a concurrency constraint, a
task queue is a good place to put it.**

```python
COMPUTE = "duck-compute-tq"   # many workers, 8 slots each, :memory:
WRITER  = "duck-writer-tq"    # one worker, ONE slot, warehouse.duckdb
```

A mutex in application code gives you exclusion. A task queue with one slot
gives you exclusion *plus*: durable queueing (work waits in Temporal, not in
your process's memory), backpressure that is visible in a dashboard, retries
with backoff, timeouts, and a history of who held it and for how long. For a
constraint you cannot remove, that is a much better shape than a lock.

The same pattern generalises: a rate-limited vendor API, a licence-limited
binary, a GPU, a legacy system that allows two sessions. One queue, N slots,
and the constraint is now infrastructure rather than folklore.

**Assignments: 2, 17, 24.**

## C2. Idempotency is not optional under at-least-once

Temporal guarantees an activity runs **at least** once. A worker that completes
a publish and dies before reporting will be asked to publish again. So:

```sql
DELETE FROM gold.x WHERE business_date = ?;     -- then
INSERT INTO gold.x BY NAME SELECT * FROM read_parquet(?);
```

"Make the world look like this", never "add this". Every metadata write is an
upsert on a primary key for the same reason.

**And the bookkeeping has to know it was retried.** `publish_log` records which
run owned a date before this one, so compensation can restore it. The naive
lookup finds *any* previous row — so a retried publish records **itself** as its
own predecessor, and compensation then restores the state it was supposed to
undo. The fix is one clause:

```sql
WHERE table_name = ? AND business_date = ? AND run_id <> ? AND NOT compensated
```

(`warehouse.py:160`, guarded by `test_publish_is_idempotent`.) This is the
subtlest bug in the repo and it is entirely a product of at-least-once. Assume
every write activity you ever create has a version of it.

**Assignments: 18, 19.**

## C3. Reversibility without time travel

Iceberg or Delta would give you `ROLLBACK TO SNAPSHOT`. Parquet gives you
nothing. The minimum substitute, built here:

1. **Immutable, run-scoped outputs.** Every step writes `…/dt=<date>/run=<run_id>/data.parquet` and never mutates it. Two runs of the same date do not collide.
2. **A publish log** that records, per (table, date), the run that owned it before and *the URI that run wrote*.
3. **Compensation = re-insert that URI.** It works because the file was never deleted.

The cost is storage: every failed run's Parquet is still there. `make lake`
shows it. That is a deliberate trade — those files are also the only forensic
evidence of what went wrong, and object storage is cheap compared to an
afternoon of not knowing.

Where this is weaker than real table formats: no atomic multi-table commit
(each table is its own transaction — the docstring at `warehouse.py:317`
explains what that means for a partial failure), no schema evolution beyond
`INSERT … BY NAME`, and no reader isolation during the swap. If you need those,
you need a table format, and this exercise tells you precisely *why* rather than
leaving it as received wisdom.

**Assignments: 19, 23.**

## C4. Payload discipline

The history is the durability mechanism, so anything you put in it you carry
forever and replay forever. Temporal's default gRPC limit is 2 MB and the real
advice is to stay far under.

So no activity here returns data. They return row counts, byte counts and URIs.
`shared.py` is written entirely around that rule — read its module docstring.

The corollary is a *design* rule, not just a plumbing one: **the unit of work
between activities is a file, not a dataset in memory.** That is why each step
writes Parquet even when the next step runs on the same machine a millisecond
later. You pay a serialisation round-trip and you buy: independent retries,
independent sizing, a durable checkpoint between every stage, and a debuggable
artifact at every boundary. At this scale it is worth it. At 50 ms per step it
would not be — and knowing where that line is, for your data, is what a POC is
for.

## C5. Cancelling an embedded engine

`con.interrupt()` is the only way to stop a running DuckDB query, and **it must
be called from a different thread than the one blocked in `execute()`**.
Cancelling the Python coroutine does not stop the thread — nothing in Python
does.

Temporal hands you exactly the right shape for free:

```python
live = _Live()                                  # shared handle
beater = asyncio.create_task(beat())            # event loop: heartbeats
try:
    return await asyncio.to_thread(fn, live)    # worker thread: DuckDB blocks
except asyncio.CancelledError:
    live.interrupt()                            # from the loop -> unblocks the thread
    raise
```

The cancellation path and the heartbeat path are the same machinery
(`activities/core.py:79`). Copy this shape for any blocking engine you wrap in
an activity.

**Assignments: 15, 16.**

## C6. Where to put the SQL

`steps.py` holds every statement as data; the activity that runs it contains no
business logic at all. This is worth doing for three reasons that only become
obvious once you have it:

- `git diff steps.py` is the whole change to the data. Nobody has to re-read the Temporal code to review a pipeline change.
- The catalog can be **validated without running anything** (`validate_catalog()`, and the tests that call it). A typo fails at worker startup instead of at 02:00 in phase three.
- The same declaration drives execution, the quality gate, lineage and the CLI's `steps` command. There is no second place for them to drift apart.

The constraint that made it work: `steps.py` is imported inside the workflow
sandbox, so it *cannot* read configuration. The SQL had to become pure.

**Assignment: 8.**

## C7. The scaling model, and where it stops

```
make scale N=6            # compute: safe, linear, boring
--scale worker-writer=2   # a corruption bug wearing a throughput costume
```

Compute scales because compute activities share nothing. The writer does not
scale, ever. So the ceiling of this design is: **how much work can one process
do on the write path?** Here that is a `DELETE` plus an `INSERT … SELECT` from a
13 KB Parquet file — microseconds. The write path would have to grow by four or
five orders of magnitude before it became the bottleneck, and long before that
you would move the serving tables to something with a real concurrency story and
keep DuckDB for the compute.

Two known lies in this repo, both left in deliberately because spotting them is
the skill:

- `silver_cleanse` differences a counter with `lag()` but reads one day, so the first reading of each meter each day has no predecessor and is quarantined as `no_prior_reading` — a 1.04 % floor on the reject rate (**Assignment 12** fixes it).
- `kwh_7d_avg` is written as a seven-day `RANGE` frame over a single partition, so it equals today's value (**Assignment 13** fixes it).

Both are the same bug: *a window function is only as wide as the data you read*.
It is the single most common correctness bug in partitioned pipelines, in every
engine, and the fix always costs you something — a wider read, a state store, or
an incremental design. Working out which one you can afford is the final
exercise.

**Assignments: 12, 13, 24, capstone.**

---

## Where next

- Practise: [ASSIGNMENTS.md](ASSIGNMENTS.md), 24 exercises plus a capstone.
- Sequence: [ROADMAP.md](ROADMAP.md).
- Evaluate: [README.md §5](README.md#5-known-friction-the-actual-output-of-this-poc).
