# Learning DuckDB and Temporal with this repo

Two technologies that have nothing to do with each other in theory, and are
slightly awkward together in practice. Part A is DuckDB, Part B is Temporal, and
**Part C is the part you cannot get from either project's documentation** —
what happens when you put them in the same system.

Every section ends with two things: links to the actual upstream documentation
for what was just described, and the assignments that drill it. Read the
upstream pages. This guide is opinionated about *which* features matter and
*why*, which is exactly what reference documentation cannot be — but it is not a
substitute for the reference.

> **Versions.** Written against DuckDB 1.5.5 and the Temporal Python SDK 1.32.0,
> links checked September 2026. DuckDB in particular moves fast and has broken
> things between minor versions before; if a link 404s, the feature almost
> certainly still exists under a new URL.

Work through it with the stack running (`make up`) and the Temporal UI open at
<http://localhost:8234>. For the order to do all of this in, see
[ROADMAP.md](ROADMAP.md). Exercises are in [ASSIGNMENTS.md](ASSIGNMENTS.md).

Rough time: 3–4 hours to read and poke; 15–25 hours to do the assignments.

---

# Part A — DuckDB as a pipeline engine

## A0. The one idea

**DuckDB is a library, not a server.** There is no daemon, no cluster, no
connection pool, no wire protocol. `import duckdb` and you have an analytical
database inside your process, using your process's memory and your process's
cores.

Everything good and everything painful follows from that:

| Because it is in-process… | …you get | …and you give up |
|---|---|---|
| no network between plan and data | microsecond query start, no serialisation tax | nothing to scale horizontally |
| the database is a file | copy it, ship it, back it up with `cp` | one read-write process at a time |
| the database can be nothing at all | `:memory:` is free, so every task can have one | no shared state between tasks |
| the engine is a dependency | you version it in `requirements.txt` | you own the upgrade, not an ops team |

That last row is the one people underrate. A DuckDB "cluster upgrade" is a pip
pin and a redeploy. If you have ever spent a quarter moving a team from Spark
3.3 to 3.5, that is not a talking point, it is the whole business case.

The honest framing for when this is the right tool is
[Big Data is Dead](https://motherduck.com/blog/big-data-is-dead/) — the argument
that most organisations' "big data" is tens of gigabytes, and that a decade of
tooling was designed for a problem they do not have. The counter-argument you
should also hold: single-node means single point of failure, single writer, and
a hard ceiling you will hit without warning. This repo is an attempt to find out
where that ceiling actually is rather than to take either side on faith.

→ **Docs:** [Why DuckDB](https://duckdb.org/why_duckdb) ·
[Friendly SQL](https://duckdb.org/docs/stable/sql/dialect/friendly_sql) ·
[TPC-H on a Raspberry Pi](https://duckdb.org/2025/01/17/raspberryi-pi-tpch.html)
(worth five minutes purely for calibration)

**Assignment: 4.**

## A1. Connections, and the order you configure them

Read `duck/session.py:42` (`_apply`) and `duck/session.py:95` (`connect`). The
order is not cosmetic:

```python
SET extension_directory = '…'      # before LOAD, or LOAD looks in the wrong place
SET threads = 4                     # before the first query builds a pipeline
SET memory_limit = '512MB'          # before the first query allocates
SET preserve_insertion_order = false
SET temp_directory = '/data/duck-tmp'
LOAD httpfs                         # before any s3:// path is parsed
CREATE OR REPLACE SECRET lake (…)   # before any s3:// path is *read*
```

Get the order wrong and the errors point somewhere else entirely: a missing
secret surfaces as an HTTP 403, a missing `LOAD` as "unknown protocol", and a
`memory_limit` set after the first query simply does not apply to it and you
spend an afternoon wondering why the limit "doesn't work".

Two choices in this file worth arguing about:

**One connection per activity, not per worker.** A `:memory:` connection costs
single-digit milliseconds. Owning one per unit of work means an activity can be
cancelled by interrupting *its* connection without touching anything else the
worker is doing — and for the warehouse file it means the file is unlocked
between activities, so `make report` works while the pipeline runs (§A7).

**`connect()` is a context manager.** DuckDB will not release a file lock held
by a connection nobody closed, and "the process is about to exit anyway" stops
being true the moment the process is a long-lived worker.

→ **Docs:** [Configuration](https://duckdb.org/docs/stable/configuration/overview)
· [Python API](https://duckdb.org/docs/stable/clients/python/overview)

**Assignments: 4, 9.**

## A2. Reading: paths are relations

`duck/session.py:148` (`register_source`) turns a URI into a plain name:

```python
CREATE OR REPLACE VIEW bronze_readings AS
SELECT * FROM read_parquet('s3://lake/bronze/…/run=abc/data.parquet',
                            union_by_name = true)
```

Which is why every statement in `steps.py` reads `FROM bronze_readings` and not
a URI. The SQL stays about the data; the path — which encodes the run id, and is
therefore the interesting thing to log — stays in the activity.

Things worth knowing about the readers, in rough order of how much pain they
save:

- **`union_by_name = true`** matches columns across files *by name* instead of by position. Without it, one file written with reordered columns silently corrupts the whole read. This is the single highest-value flag on this page.
- **Globs and Hive partitioning.** `read_parquet('s3://…/dt=*/**.parquet', hive_partitioning = true)` turns `dt=2026-08-01` in the path into a column. Assignments 12 and 13 both need this.
- **`read_csv` sniffs types from a sample.** `sample_size = -1` reads the whole file to decide, which is right for a fixture and wrong for a 40 GB file. In production, pass `types = {…}` and stop guessing — a sniffer that reads 20 000 rows and infers `BIGINT` will meet a `NULL` on row 20 001 and fail the load at 3 a.m.
- **Projection and predicate pushdown are real.** Reading Parquet costs you the columns you select and the row groups whose statistics survive your `WHERE`. That is why the quality checks in this repo re-read the file they just wrote and it barely registers.

→ **Docs:** [Reading Parquet](https://duckdb.org/docs/stable/data/parquet/overview)
· [Hive partitioning](https://duckdb.org/docs/stable/data/partitioning/hive_partitioning)
· [CSV import](https://duckdb.org/docs/stable/data/csv/overview)
· [S3 API support](https://duckdb.org/docs/stable/extensions/httpfs/s3api)

**Assignments: 5, 8, 10.**

## A3. Writing: COPY, and the flag that decides whether LEAN survives

`duck/session.py:182` (`copy_to_parquet`):

```sql
COPY (<select>) TO 's3://…/data.parquet'
     (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880)
```

- **ZSTD** because these files are read far more often than written. Roughly 2× smaller than snappy, a little slower to write, and faster to read whenever you are I/O-bound — which over S3 you always are.
- **`ROW_GROUP_SIZE` pinned** because a reader's parallelism is bounded by the row-group count. Leave it to default and downstream parallelism depends on how much memory the *writer* happened to have that day. That is a genuinely miserable bug to chase.
- **`preserve_insertion_order = false`** because DuckDB otherwise buffers a complete result to keep rows in order while writing. Turning it off lets the write stream. On the 512 MB LEAN profile it is the difference between spilling politely and not finishing. It is safe here because every consumer of these files sorts or aggregates; it is **not** safe if anything downstream depends on file row order, and "nothing does" is a claim worth checking rather than assuming.

Note what `copy_to_parquet` does *not* do: append. Every step output is one file
at one immutable `run=<run_id>` path. That is what makes rollback possible (§C3),
and it is a deliberate design decision rather than a simplification.

→ **Docs:** [COPY statement](https://duckdb.org/docs/stable/sql/statements/copy)
· [Tuning workloads](https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads)

**Assignments: 8, 9, 10.**

## A4. The SQL that earns its keep

This is the part to steal. Each of these replaces a pattern you have written by
hand, and the replacement is shorter *and* harder to get wrong. DuckDB collects
them under the name
[friendly SQL](https://duckdb.org/docs/stable/sql/dialect/friendly_sql), which
undersells them — several are not sugar, they are different algorithms.

### `QUALIFY` — filter on a window function without a subquery
`steps.py:288` (`silver_cleanse`):

```sql
SELECT * FROM bronze_readings
QUALIFY row_number() OVER (
    PARTITION BY meter_id, reading_ts ORDER BY src_updated_at DESC
) = 1
```

ANSI SQL needs a subquery or CTE purely to give the window function a name to
filter on. `QUALIFY` is to window functions what `HAVING` is to aggregates. For
"keep the latest version of each key" — the most common operation in any silver
layer anywhere — it is the difference between five lines and one.

→ [QUALIFY](https://duckdb.org/docs/stable/sql/query_syntax/qualify)

### Named `WINDOW` — say the frame once
```sql
SELECT *,
       lag(cumulative_kwh) OVER w AS prev_kwh,
       lag(reading_ts)     OVER w AS prev_ts,
       cumulative_kwh - lag(cumulative_kwh) OVER w AS interval_kwh
FROM deduped
WINDOW w AS (PARTITION BY meter_id ORDER BY reading_ts)
```

Three `lag()` calls that *must* share a frame. Written out three times they can
drift apart in a later edit, and the result is not an error — it is quietly
wrong numbers.

→ [Window functions](https://duckdb.org/docs/stable/sql/functions/window_functions)

### `ASOF JOIN` — "the row in force at that moment"
`steps.py:373` (`silver_enrich`) — the single best reason to write this pipeline
in DuckDB rather than in pandas:

```sql
FROM with_meter w
ASOF JOIN bronze_tariffs t
      ON w.tariff_plan = t.tariff_plan
     AND w.reading_ts >= t.valid_from
```

Every reading gets the most recent tariff at or before its timestamp. Written
conventionally that is a correlated `max(valid_from)` subquery, or a self-join
plus a dedupe, and both are O(bad). DuckDB compiles it into one sorted merge.

Any time you join a fact to a slowly-changing dimension, a price curve, an FX
rate, a feature-flag history or a configuration table, this is the join you
actually wanted and probably hand-rolled. Pandas users know it as
`merge_asof`; kdb+ users have had it for thirty years and are entitled to be
smug about it.

**`ASOF LEFT JOIN` is not a detail.** The weather join uses `LEFT` because a
missing observation must not delete a consumption row. An inner ASOF JOIN there
is a textbook silent-data-loss bug — the pipeline succeeds, the dashboard is
wrong, and nobody notices for a quarter. The `enrich_preserves_volume`
expectation exists to catch precisely that class of mistake, and Assignment 7
makes you cause it.

→ [ASOF joins: fuzzy temporal lookups](https://duckdb.org/2023/09/15/asof-joins-fuzzy-temporal-lookups.html)
(the blog post is better than the reference here) ·
[FROM / JOIN clause](https://duckdb.org/docs/stable/sql/query_syntax/from)

### `GROUP BY ALL` — group by everything you did not aggregate
```sql
SELECT business_date, region, count(DISTINCT meter_id) AS meters, sum(…) AS kwh
FROM silver_enriched
GROUP BY ALL
```

Removes the most common refactoring bug in analytics SQL: adding a dimension to
the SELECT and forgetting to add it to the GROUP BY. `ORDER BY ALL` exists too.

→ [GROUP BY ALL](https://duckdb.org/docs/stable/sql/query_syntax/groupby)

### `* EXCLUDE` / `* REPLACE` — subtract from a star
```sql
SELECT * EXCLUDE (price_valid_from, weather_observed_at, src_updated_at),
       round(interval_kwh * price_per_kwh, 5) AS cost_eur
FROM with_weather
```

Dropping three columns out of fourteen without naming the other eleven.
`REPLACE` does the same for overwriting one in place. Both keep working when
somebody adds a column upstream, which a hand-written list does not.

→ [Star expression](https://duckdb.org/docs/stable/sql/expressions/star)

### `FILTER (WHERE …)` — a conditional aggregate that reads like one
```sql
sum(interval_kwh) FILTER (WHERE is_peak) AS peak_kwh
```
versus `sum(CASE WHEN is_peak THEN interval_kwh END)`. Same plan; the first one
cannot be misread, and it is standard SQL that most engines still do not have.

### `RANGE … INTERVAL` frames — a rolling window that is actually rolling
```sql
avg(kwh) OVER (PARTITION BY region ORDER BY business_date
               RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW)
```

`ROWS` counts rows; `RANGE` counts *values of the ordering column*. If a region
is missing a day, `ROWS BETWEEN 6 PRECEDING` quietly reaches back seven calendar
days and your "7-day average" is an 8-day average on exactly the days you most
want to trust it. `RANGE … INTERVAL` does not. (In this repo the frame only ever
sees one partition — see §C7 and Assignment 13.)

### `ANTI JOIN` / `SEMI JOIN` — set membership that survives NULLs
```sql
SELECT count(*) FROM child c
ANTI JOIN parent p ON c.meter_id = p.meter_id
```

`NOT IN (SELECT …)` returns *nothing at all* if the subquery yields a single
NULL. That trap has broken more referential-integrity checks than any other
single cause in SQL, and it fails in the safe-looking direction: your check
passes. `ANTI JOIN` says what you meant.

### Also used in the fixture, and worth knowing
`range()` and `generate_series()` as table functions; list literals with
**1-based** indexing (`['a','b','c'][2]` is `'b'`); `to_minutes()` / `to_hours()`
/ `to_days()` for interval arithmetic on an *expression* (plain `INTERVAL 5 DAY`
only takes a literal); `any_value()` for a functionally-dependent column you do
not want as a grouping key; `hash()` for deterministic pseudo-randomness (§A5 of
`generator.py`, and the reason this fixture is reproducible at all).

And run `SUMMARIZE silver_enriched;` right now. It gives you min, max,
approximate distinct count, null percentage and quartiles for every column in
one word, and it is the fastest way to understand a dataset you did not create.

**Assignments: 5, 6, 7, 8, 11, 12, 13.**

## A5. Memory, spilling, and "larger than memory"

DuckDB can process more data than it has RAM, but only if you let it:

```python
SET memory_limit = '512MB'
SET temp_directory = '/data/duck-tmp'
SET max_temp_directory_size = '8GB'
```

Without `temp_directory`, a query that exceeds `memory_limit` fails outright.
With it, hash joins and sorts spill to disk and the query completes, more
slowly. Without `max_temp_directory_size`, a runaway spill fills the volume and
takes the *writer* down with it; with it, the query fails and Temporal retries
it — a much better failure, because it is localised.

`make run-lean` versus `make run-roomy` is exactly this knob, and
`_memory_note()` (`activities/core.py:237`) reports what actually happened:

```sql
SELECT sum(memory_usage_bytes), sum(temporary_storage_bytes) FROM duckdb_memory()
```

That second number is the only honest answer to "did this step spill?". Everyone
guesses; the guess is usually wrong.

The mental shift worth making: with a cluster you scale *out*, and the framework
decides memory per executor. With DuckDB you scale *the process*, and it is a
per-step decision — which is why `_size_step` (`workflows/pipeline.py:366`)
exists at all, and why §5 item 11 of the README is about the arithmetic nobody
does.

→ **Docs:** [Tuning workloads](https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads)
· [Environment / memory](https://duckdb.org/docs/stable/guides/performance/environment)
· [Out-of-core aggregation](https://duckdb.org/2024/03/29/external-aggregation.html)
(how the spilling actually works — genuinely good reading)

**Assignments: 4, 9.**

## A6. Metadata without scanning

```sql
SELECT sum(num_rows) FROM parquet_file_metadata('s3://…/*.parquet');  -- footers
SELECT sum(total_compressed_size) FROM parquet_metadata('s3://…');    -- row groups
SELECT * FROM glob('s3://lake/**');
```

`probe_inputs` (`activities/core.py:126`) uses the first two to size a step in
AUTO mode. Sizing 400 MB of input costs a handful of range requests rather than
a scan, which is what makes runtime sizing a *measurement* rather than a guess.

→ **Docs:** [Parquet metadata functions](https://duckdb.org/docs/stable/data/parquet/metadata)

**Assignment: 10.**

## A7. The single-writer rule, precisely

This is the constraint the entire architecture is arranged around, and the
one-line version of it that everyone repeats is wrong in a way that matters.

Run `make demo-contention`. Two different things happen.

**Across processes, the file lock refuses the open.** Not only a second writer —
a *read-only* open is refused too, while a writer holds the file. Hence:

- the writer opens the database per activity and closes it (`warehouse.py:_writer`), so the file is free between activities;
- every reader goes through `session.open_read_only()` (`session.py:123`), which backs off and retries rather than assuming.

**Within one process, connections share an instance and are allowed.** They go
through DuckDB's optimistic transaction manager instead, so the failure moves to
commit time:

```
TransactionContext Error: Conflict on tuple deletion!
```

That is the one that would actually bite a writer worker with
`max_concurrent_activities` turned up. Not a lock error — a *conflict* error, at
commit, on the rows two publishes of the same date both tried to replace. And
retrying it would paper over a second problem: `publish()` reads the previous
owner of a date *before* deleting it, which is a read-modify-write that
serialisation removes and retries do not.

If you take one page of DuckDB documentation away from this repo, make it the
concurrency page. It is short, and it is the difference between "DuckDB is
single-writer" as folklore and as a specification.

→ **Docs:** [Concurrency](https://duckdb.org/docs/stable/connect/concurrency)
· [Operational limits](https://duckdb.org/docs/stable/operations_manual/limits)

**Assignment: 18.**

## A8. Transactions, constraints and `INSERT … BY NAME`

`duck/warehouse.py` uses more of DuckDB-as-a-database than most pipelines
bother with:

- **`PRIMARY KEY`** on `meta.step_metrics (run_id, step_key)` and friends. Not decoration: it is what makes `INSERT OR REPLACE` an upsert, which is what makes the metadata writes safe to replay (§C2).
- **`INSERT … ON CONFLICT … DO UPDATE`** for `meta.runs`, where only some columns should move.
- **`INSERT INTO … BY NAME SELECT * FROM read_parquet(…)`** — columns matched by name, so reordering a gold SELECT does not shift data into the wrong columns. A positional insert would not error; it would just be wrong.
- **Explicit `BEGIN`/`COMMIT`/`ROLLBACK`** (`warehouse.py:317`). DuckDB has no nested transactions, so each publish target gets its own — which means a failure on the second table leaves the first committed *and* leaves `publish_log` agreeing with that, so compensation undoes exactly what landed. Assignment 25 makes you decide whether that is good enough.
- **`CREATE TABLE … AS SELECT * FROM read_parquet(…) LIMIT 0`** — schema-on-first-publish, read from the Parquet footer. The alternative is hand-written DDL that drifts the first time someone adds a column.

→ **Docs:** [INSERT](https://duckdb.org/docs/stable/sql/statements/insert)
· [Concurrency and transactions](https://duckdb.org/docs/stable/connect/concurrency)

**Assignments: 19, 25.**

## A9. Extensions and secrets

```sql
CREATE OR REPLACE SECRET lake (
    TYPE s3, KEY_ID '…', SECRET '…', REGION '…',
    ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false
);
```

A secret rather than the legacy `SET s3_access_key_id`: secrets are scoped, are
not readable back out of the connection, and are the only form that supports
per-prefix credentials when the lake grows a second bucket. Two gotchas that
cost everyone an hour once: `ENDPOINT` takes `host:port` with **no scheme**, and
`URL_STYLE 'path'` is required for MinIO and most non-AWS S3 implementations.

Extensions are baked into the image at build time
(`docker/worker.Dockerfile`), because an activity that runs `INSTALL httpfs` on
first use fails in an air-gapped network, times out in a slow one, and does it
again in every fresh container.

→ **Docs:** [CREATE SECRET](https://duckdb.org/docs/stable/sql/statements/create_secret)
· [S3 API support](https://duckdb.org/docs/stable/extensions/httpfs/s3api)

## A10. What DuckDB is not

Say these out loud before proposing it anywhere:

- **Not a concurrent-write store.** One writer process. If your workload is many writers, this is the wrong tool and no orchestrator fixes it.
- **Not distributed.** No shuffle across machines. A join whose build side exceeds one machine's disk is out of scope, and there is no graceful degradation — it fails.
- **Not a catalog.** No schema registry, no snapshots, no time travel. §C3 is what it costs to build the minimum substitute, and the honest conclusion of that section is that at some point you should just use Iceberg.
- **Not a server.** No connection pool, no query queue, no admission control. Those become *your* problem — and in this repo, Temporal is the answer to all three, which is most of the reason the pairing is interesting.

---

# Part B — Temporal as a data orchestrator

## B0. The one idea

Temporal is **durable execution**. You write ordinary async Python; Temporal
guarantees the function survives process death, machine death and multi-day
waits, resuming exactly where it was with its local variables intact.

The mechanism is worth internalising on day one, because every rule in Part B
follows from it and none of them make sense without it:

> Temporal does not snapshot your workflow's memory. It records an **event
> history** of everything that happened to it — activity scheduled, activity
> completed, timer fired, signal received — and when it needs the workflow's
> state back it **re-executes your workflow function from line 1**, feeding it
> the recorded results instead of doing the work again. When it runs out of
> history it hands control back to your code and the function continues live.

That re-execution is **replay**. It happens on worker restart, on a different
machine, on a query, on any resumption after the workflow was evicted from
cache. It is invisible and it is constant, and the first week with Temporal is
mostly the process of believing it.

Two consequences you will meet in every section:

1. **Workflow code must be deterministic.** Replay must reproduce the same sequence of decisions. `datetime.now()`, `random`, reading a file, reading an environment variable, iterating an unordered set — all forbidden inside a workflow, because on replay they would take a different branch and diverge from the recorded history.
2. **Everything non-deterministic is an activity.** An activity is a plain function that runs *outside* replay. Its result is recorded once; on replay the recorded result is handed back without re-running it.

So: **the workflow decides, activities do.** Here, `workflows/pipeline.py`
decides (what runs, in what order, with what memory limit, what to do when it
breaks) and `activities/core.py` does (opens DuckDB, touches S3, writes the
warehouse).

A caution about scale of ambition: Temporal is a general workflow engine, not a
data orchestrator. It has no notion of a dataset, a partition, a backfill, or a
schedule-aware dependency graph — all of which Airflow and Dagster give you for
free. What it gives you instead is that the pipeline *cannot lose its place*.
Whether that trade is worth it depends entirely on whether your failures are
"the query was wrong" (use Dagster) or "the thing died halfway through a
four-hour multi-system transaction and nobody knows what landed" (use Temporal).

→ **Docs:** [Workflows](https://docs.temporal.io/workflows)
· [Event History](https://docs.temporal.io/encyclopedia/event-history)
· [Temporal Python SDK samples](https://github.com/temporalio/samples-python)
(the best available Python documentation, honestly)

**Assignment: 1.**

## B1. The four objects

| Object | What it is | Here |
|---|---|---|
| **Workflow** | Durable, deterministic, replayable orchestration function | `workflows/pipeline.py`, `workflows/backfill.py` |
| **Activity** | One unit of side-effecting work; retried, timed out, heartbeated | `activities/core.py` — 8 of them |
| **Worker** | A process polling a task queue, executing workflow and/or activity tasks | `worker.py`, run twice: `compute` and `writer` |
| **Task queue** | Just a name. Workers poll it; callers target it | `queues.py` — `duck-compute-tq`, `duck-writer-tq` |

The Temporal **server** holds histories, timers and queues. It never runs your
code. Your workers do, and they connect *out* to the server — the server never
calls in. That is why a worker can live in a private network with no inbound
firewall rule, and why "scaling Temporal" almost always means scaling workers.

A **workflow execution** is identified by `workflow_id` + `run_id`. The
`workflow_id` is yours to choose and must be unique among *running* workflows in
a namespace — this repo uses `meter-<date>-<hhmmss>`, while the backfill uses a
stable `backfill-<start>-to-<end>` so that re-issuing it collides deliberately
(Assignment 23 makes you observe that). The `run_id` changes on retry and on
continue-as-new.

→ **Docs:** [Task Queues](https://docs.temporal.io/task-queue)
· [Workers](https://docs.temporal.io/workers)
· [Activities](https://docs.temporal.io/activities)

**Assignment: 2.**

## B2. Reading this workflow

Open `workflows/pipeline.py:218` and read `run()`. It is short on purpose:

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
(`_quality_gate:424`), the saga (`_compensate:510`), or bookkeeping. Read them in
that order and the file stops being intimidating.

Note what the workflow never does: open a connection, read an environment
variable, or look at a clock other than `workflow.now()`.

→ **Docs:** [Workflow definition](https://docs.temporal.io/workflow-definition)
· [Python: core application](https://docs.temporal.io/develop/python/core-application)

## B3. Determinism, concretely

The sandbox catches some violations and not others, and the ones it misses are
the interesting ones.

| Forbidden in workflow code | Use instead |
|---|---|
| `datetime.now()`, `time.time()` | `workflow.now()` |
| `random`, `uuid4()` | `workflow.random()`, `workflow.uuid4()` |
| `os.environ`, reading a file | pass it in on the workflow input |
| `threading`, sockets, any real IO | an activity |
| iterating a `set` | sort it, or use a list |
| `asyncio.sleep` | allowed — the SDK patches it into a durable timer |

`steps.py` is imported *inside* the workflow, which is why it is stdlib-only and
why `render_context()` takes its values as arguments instead of reading
settings. That constraint turned out to improve the design rather than
constrain it: the SQL became pure data with no configuration baked in.

`config.py` carries a docstring saying it must never be imported by workflow
code. Believe it.

→ **Docs:** [Deterministic constraints](https://docs.temporal.io/workflow-definition#deterministic-constraints)

**Assignment: 22.**

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

All the judgement is in that last line. A transient S3 error, or a spilled query
that hit a full temp directory, deserves a retry. **A failed expectation does
not**: the input Parquet is immutable, so attempt two reads identical bytes and
reaches an identical verdict. Retrying turns a five-second failure into a
ninety-second one and tells nobody anything.

Note the default nobody changes: `maximum_attempts=0` means *retry forever*.
That is the right default for a workflow whose dependency will eventually come
back, and a very expensive one for a step that is deterministically broken.

The activity opts in by *typing* its error (`activities/core.py:258`):

```python
raise ApplicationError(
    f"quality gate failed for {req.step_key}: {detail}",
    report,                      # failure details travel with the exception
    type="DataQualityError",     # matched against non_retryable_error_types
    non_retryable=True,
)
```

That second positional argument is worth knowing about: failure **details**. The
DQ report reaches the workflow through the exception, which is how the failing
checks still land in `meta.dq_results` even though the activity did not return
normally. Most people discover this parameter a year in.

`WRITER_RETRY` is deliberately different — more attempts, shorter intervals. The
writer's work is small, idempotent and on the critical path.

→ **Docs:** [Retry policies](https://docs.temporal.io/encyclopedia/retry-policies)
· [Python: failure detection](https://docs.temporal.io/develop/python/failure-detection)

**Assignment: 14.**

## B5. Timeouts and heartbeats

Four timeouts exist. Two matter here, and one of them is the one everybody gets
wrong.

- **`start_to_close_timeout`** — how long one *attempt* may take. Set it generously (20 minutes for a step). Too tight and a legitimately slow step gets killed and retried, which is strictly worse than waiting: you now have two slow steps.
- **`heartbeat_timeout`** — 30 s here. The activity must call `activity.heartbeat()` more often than this or Temporal declares it dead and retries it. This is how a crashed worker is detected in 30 seconds rather than 20 minutes. Without a heartbeat, `start_to_close_timeout` is your only detector, and it is a bad one.

`_with_heartbeat` (`activities/core.py:79`) is the machinery:

```python
beater = asyncio.create_task(beat())          # event loop: heartbeats every 5s
return await asyncio.to_thread(fn, live)      # worker thread: DuckDB blocks
```

The heartbeat payload is the current stage (`"write silver_enriched"`), so the
Temporal UI shows which statement a slow step is on without anyone opening a
log. Start a big run and look at the activity's heartbeat details — it is the
cheapest observability in the whole repo.

→ **Docs:** [Detecting activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures)
· [Python: failure detection](https://docs.temporal.io/develop/python/failure-detection)

**Assignment: 16.**

## B6. Timers and SLAs

`_with_sla` (`workflows/pipeline.py:400`) races a timer against the step:

```python
task  = asyncio.ensure_future(coro)
timer = asyncio.ensure_future(asyncio.sleep(budget))
done, _ = await workflow.wait([task, timer], return_when=asyncio.FIRST_COMPLETED)
```

`asyncio.sleep` inside a workflow is not a sleep — it is a **durable timer**,
recorded in the history and held by the server. Restart the worker mid-step and
the deadline is still exactly where it was. A sidecar cron watchdog cannot say
that, and neither can `threading.Timer`. Nor is there any cost to a long one:
`await asyncio.sleep(86400)` occupies no worker memory for the day it is
waiting.

This is a *soft* SLA: it records a breach and keeps waiting. Making it hard is
one call to `task.cancel()`, which is Assignment 15 — and the reason the
activity has to be interruptible at all (§A7, §C5).

→ **Docs:** [Python: timers](https://docs.temporal.io/develop/python/timers)

**Assignment: 15.**

## B7. Signals, queries, updates

Three ways to talk to a running workflow. They are not interchangeable and
choosing wrong is a common source of quiet bugs.

| | direction | blocks? | can mutate? | here |
|---|---|---|---|---|
| **Signal** | in | no, fire-and-forget | yes | `approve_publish`, `pause`, `resume`, `abort` |
| **Query** | out | yes, returns a value | **no** | `progress`, `plan` |
| **Update** | both | yes, returns a value | yes | `extend_sla` |

- **Signals are durable and asynchronous.** Signal a workflow whose worker is down and it parks in the history, delivered when a worker returns. You get no acknowledgement that anything *acted* on it, only that it was recorded.
- **Queries must not mutate.** A query runs against a replayed workflow; mutating during one corrupts state in ways that are genuinely hard to debug. `progress()` only reads.
- **Updates can validate before accepting.** `extend_sla.validator` rejects an unknown step key at call time, so the caller gets an error. The same thing as a signal would have been silently accepted, written to the history, and done nothing — which is Assignment 21's punchline.

Try all three:

```bash
make run-approval                      # parks at the gate
make watch ID=approval-demo            # query
make extend-sla ID=approval-demo STEP=silver_enrich SECS=120   # update
make approve ID=approval-demo          # signal
```

The approval wait is the clearest demonstration of durable execution in the
repo. While parked, the workflow occupies no worker memory at all. `make down`,
go to lunch, `make up`, `make approve` — it resumes. `make demo-durability` does
exactly this, deterministically.

→ **Docs:** [Workflow message passing](https://docs.temporal.io/encyclopedia/workflow-message-passing)
· [Python: message passing](https://docs.temporal.io/develop/python/message-passing)
· [message_passing samples](https://github.com/temporalio/samples-python/tree/main/message_passing)

**Assignments: 3, 21.**

## B8. Saga and compensation

Temporal has no transactions across activities. What it has is: if you can
express the undo, it will durably run it. That is the
[saga pattern](https://microservices.io/patterns/data/saga.html), and Temporal is
about the most comfortable place to implement one.

```python
except Exception as exc:
    self._status = RunStatus.FAILED.value
    await self._compensate(inp, _describe(exc))
    await self._finalise(inp, started, error=...)
    raise
```

Two details that are easy to get wrong and are right here:

- **The compensation is itself an activity**, so it retries, times out and appears in the history. A compensation implemented as a `finally:` block in workflow code would be re-executed on replay and could not retry.
- **A failed compensation must not mask the original failure** (`pipeline.py:510`). It logs loudly and the original exception still propagates, because the run was going to fail anyway and swallowing this would hide the far more important fact that the warehouse is now in an unknown state.

What compensation actually *does* in this repo is §C3, and it is more
interesting than the mechanism.

**Assignment: 20.**

## B9. Child workflows and continue-as-new

`workflows/backfill.py`. Two mechanics that only show up at scale.

**Child workflows, not activities.** Each date is its own `MeterPipeline`
execution with its own history, retries, gate and compensation. A 90-day
backfill becomes 90 independently inspectable runs plus one coordinator, rather
than one history with 30 000 events in it that the Web UI struggles to render.

**Continue-as-new.** A history is capped: Temporal warns at 10 MB or 10 240
events and **terminates the workflow** at 50 MB or 51 200 events. A long
backfill sails past that. `workflow.continue_as_new(...)` ends the current
execution and starts a fresh one with the same workflow id, carrying only the
small state that matters — here, which dates are done. From the outside it is
still one workflow. `completed` is what makes it resumable: a crash between
checkpoints loses at most `checkpoint_every` dates and never re-publishes a date
that already landed.

The concurrency limit is the other half of the story. Child workflows are cheap,
but every one of them publishes through the *single* writer queue — so launching
90 at once just makes 90 things queue behind one slot, with longer timers and a
worse failure mode than launching four at a time.

→ **Docs:** [Child workflows](https://docs.temporal.io/encyclopedia/child-workflows)
· [Python: continue-as-new](https://docs.temporal.io/develop/python/continue-as-new)
· [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults)
(where those limits come from)

**Assignment: 23.**

## B10. Schedules

`make schedule` creates a Temporal Schedule — durable cron with backfill,
overlap policy, pause/unpause, and a jitter window, all held by the server
rather than by a crontab on a box somebody will eventually reimage.

The feature that justifies it over cron is **overlap policy**. Cron has no
opinion about what happens when yesterday's run is still going; a Schedule makes
you choose (skip, buffer one, buffer all, cancel the old one, allow both). For a
pipeline with a single writer, that choice is load-bearing rather than
cosmetic.

It is created **paused** on purpose. A schedule that starts firing the moment
you create it is how a demo becomes forty backfill runs and an angry Slack
message.

→ **Docs:** [Schedules](https://docs.temporal.io/schedule)
· [Python: schedules](https://docs.temporal.io/develop/python/schedules)

**Assignment: 24.**

## B11. Versioning, and the error you are about to cause

Change workflow code while a workflow is in flight and its replay diverges from
its history:

```
NonDeterminismError: Workflow activation completion failed
```

The first time you see this you will assume it is a bug in Temporal. It is the
system working exactly as designed, and telling you that the code you just
deployed cannot faithfully re-execute a run that is already in progress. The
standard responses:

1. **Terminate in-flight workflows** before changing workflow code. Fine in development, and what `make restart` quietly assumes.
2. **`workflow.patched("my-change")`** — branch on it, let old histories take the old path, remove the patch once nothing old is running. Three deploys, and the discipline to actually do the third.
3. **Worker Versioning** — pin build ids to workers so old code keeps serving old runs. The right answer in production and out of scope here.

Activity code is unaffected: activities are never replayed, only their recorded
results are. So changing the body of `_run_step` is safe… but changing SQL in
`steps.py` is *not*, because `steps.py` is imported into the workflow. Knowing
which of your files are workflow files is the actual skill, and it is not
obvious from the directory layout.

→ **Docs:** [Python: versioning](https://docs.temporal.io/develop/python/versioning)

**Assignment: 22.**

## B12. Reading an event history

Open any run in the UI and switch to the full JSON history. Learn to find:

- `WorkflowExecutionStarted` — your input, verbatim. Useful more often than you would expect.
- `WorkflowTaskScheduled` / `Started` / `Completed` triples — each one is a slice of your workflow code actually executing. Count them and you know how many times the function was re-entered.
- `ActivityTaskScheduled` / `Started` / `Completed` — the middle one carries the worker identity, the last one the result payload.
- `TimerStarted` / `TimerFired` / `TimerCanceled` — your SLAs.
- `WorkflowExecutionSignaled`, `WorkflowExecutionUpdateAccepted` — your approvals and updates.
- `MarkerRecorded` — side effects and patches.

The single most useful exercise in this repo is Assignment 1: read one history
end to end and work out, from the event order alone, when your workflow code was
running and when it was not. Everything else in Part B is easier afterwards.

→ **Docs:** [Event reference](https://docs.temporal.io/references/events)
· [Event History](https://docs.temporal.io/encyclopedia/event-history)

**Assignment: 1.**

---

# Part C — The intersection

Parts A and B are, in the end, summaries of documentation you could have read
yourself. This part is not written down anywhere, because it only exists once
you put an embedded engine behind a distributed orchestrator.

## C1. A task queue is a concurrency contract

The idea worth taking away from this whole repo: **when your engine has a
concurrency constraint, a task queue is a good place to put it.**

```python
COMPUTE = "duck-compute-tq"   # many workers, 4 slots each, :memory:
WRITER  = "duck-writer-tq"    # one worker, ONE slot, warehouse.duckdb
```

A mutex in application code gives you exclusion. A task queue with one slot
gives you exclusion *plus*: durable queueing (work waits in Temporal, not in
your process's memory), backpressure that shows up on a dashboard, retries with
backoff, timeouts, and a history of who held it and for how long. For a
constraint you cannot remove, that is a far better shape than a lock.

And the pattern generalises well beyond DuckDB. A rate-limited vendor API. A
licence-limited binary. A GPU. A legacy system that permits two sessions. A
migration that must not run twice. One queue, N slots, and the constraint stops
being folklore in someone's head and becomes infrastructure.

The cost, which the README's §5 is honest about: your throughput ceiling is now
the slot count, and no amount of scaling the rest of the system moves it.

→ **Docs:** [Task Queues](https://docs.temporal.io/task-queue)
· [DuckDB concurrency](https://duckdb.org/docs/stable/connect/concurrency)

**Assignments: 2, 18, 26.**

## C2. Idempotency is not optional under at-least-once

Temporal guarantees an activity runs **at least** once. A worker that completes
a publish and dies before reporting the result will be asked to publish again.
There is no configuration that turns this off, because there cannot be — it is
the same impossibility result as exactly-once delivery everywhere else.

So:

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
subtlest bug in the repo, it was found by writing the test rather than by
thinking, and it is entirely a product of at-least-once. Assume every write
activity you ever build has a version of it hiding in it.

**Assignments: 19, 20.**

## C3. Reversibility without time travel

Iceberg or Delta would hand you `ROLLBACK TO SNAPSHOT` and this section would
not exist. Plain Parquet gives you nothing, so here is the minimum substitute:

1. **Immutable, run-scoped outputs.** Every step writes `…/dt=<date>/run=<run_id>/data.parquet` and never mutates it. Two runs of the same date cannot collide.
2. **A publish log** recording, per (table, date), the run that owned it before and *the URI that run wrote*.
3. **Compensation = re-insert that URI.** It works precisely because the file was never deleted.

The cost is storage: every failed run's Parquet is still there, and `make lake`
will show you exactly how much. That is a deliberate trade — those files are
also the only forensic evidence of what went wrong, and object storage is very
cheap compared with an afternoon of not knowing.

Where this is weaker than a real table format, stated plainly so you can quote
it in a design review: no atomic multi-table commit (each table is its own
transaction — see the docstring at `warehouse.py:317` for what that means on a
partial failure); no schema evolution beyond `INSERT … BY NAME`; no reader
isolation during the swap, so a dashboard querying mid-publish sees a partially
replaced date. Assignment 25 makes you try to fix the first one and discover why
table formats exist.

The useful outcome of this section is not the mechanism. It is being able to say
*why* you would adopt Iceberg, from having built the cheap version and found its
edges, rather than from having read that you should.

**Assignments: 20, 25.**

## C4. Payload discipline

The event history is the durability mechanism, so anything you put in it you
carry forever and replay forever. The numbers, from Temporal's own defaults
page: a payload **warns at 256 KB and errors at 2 MB**; a whole history warns at
10 MB and the workflow is **terminated** at 50 MB.

So no activity here returns data. They return row counts, byte counts and URIs.
`shared.py` is written entirely around that rule — read its module docstring.

The corollary is a design rule rather than a plumbing one: **the unit of work
between activities is a file, not a dataset in memory.** That is why each step
writes Parquet even when the next step will run on the same machine a
millisecond later. You pay a serialisation round-trip, and you buy independent
retries, independent sizing, a durable checkpoint between every stage, and a
debuggable artifact at every boundary.

At this scale it is clearly worth it. At 50 ms per step it would not be — you
would be paying more in coordination than the work costs, which is exactly what
the README's §8 measurement shows starting to happen. Knowing where that line
sits *for your data* is what a POC is for.

(If you genuinely must move something large between activities, the pattern is
the claim-check: write it to object storage, pass the key. Which is what this
repo does, dressed up as a lake.)

→ **Docs:** [Self-hosted defaults / limits](https://docs.temporal.io/self-hosted-guide/defaults)
· [Data conversion](https://docs.temporal.io/dataconversion)

**Assignment: 17.**

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
an activity — a JDBC driver, a subprocess, a C extension that holds the GIL.

The honest caveat: `asyncio.to_thread` cancellation returns control immediately
but the thread keeps running until the interrupt takes effect. For DuckDB that
is fast. For a library with no interrupt at all, your only real options are a
subprocess you can signal, or accepting that the work continues invisibly —
which is worth knowing *before* you promise someone a cancel button.

→ **Docs:** [Python: cancellation](https://docs.temporal.io/develop/python/cancellation)

**Assignment: 16.**

## C6. Where to put the SQL

`steps.py` holds every statement as data; the activity that runs it contains no
business logic at all. Three reasons, which only become obvious once you have
it:

- `git diff steps.py` is the entire change to the data. Nobody reviewing a pipeline change has to re-read the Temporal code.
- The catalog can be **validated without running anything** (`validate_catalog()`, called at worker startup and by a test). A typo fails in three seconds instead of at 02:00 in phase three.
- One declaration drives execution, the quality gate, lineage, and the CLI's `steps` command. There is no second place for them to drift apart.

The constraint that forced it: `steps.py` is imported inside the workflow
sandbox, so it *cannot* read configuration. The SQL had to become pure. This is
the same instinct behind dbt models or Dagster asset definitions, arrived at
from a different direction — and if you find yourself wanting the rest of what
dbt gives you (tests, docs, a DAG UI, `ref()`), that is a real signal about
which tool you should be using for the transformation layer.

**Assignment: 8.**

## C7. The scaling model, and where it stops

```
make scale N=6            # compute: safe, linear, boring
--scale worker-writer=2   # a corruption bug wearing a throughput costume
```

Compute scales because compute activities share nothing. The writer does not
scale, ever. So the ceiling of this design is: **how much work can one process
do on the write path?** Here that is a `DELETE` plus an `INSERT … SELECT` from a
13 KB Parquet file — microseconds. The write path would have to grow four or
five orders of magnitude before it became the bottleneck, and long before that
you would move the serving tables to something with a real concurrency story and
keep DuckDB for the compute. That is Assignment 26.

Two known lies are left in this repo deliberately, because spotting them is the
skill:

- `silver_cleanse` differences a counter with `lag()` but reads one day, so the first reading of each meter each day has no predecessor and is quarantined as `no_prior_reading` — a 1.03 % floor on the reject rate (**Assignment 12**).
- `kwh_7d_avg` is written as a seven-day `RANGE` frame over a single partition, so it equals today's value (**Assignment 13**).

Both are the same bug: *a window function is only as wide as the data you read.*
It is the most common correctness bug in partitioned pipelines, in every engine,
and the fix always costs something — a wider read, a state store, or an
incremental design. Working out which one you can afford is the capstone.

**Assignments: 12, 13, 26, capstone.**

---

## Reading list

Everything cited above, in the order I would actually read it.

**Start here (about an hour)**
- [Big Data is Dead](https://motherduck.com/blog/big-data-is-dead/) — why single-node analytics stopped being a compromise.
- [DuckDB concurrency](https://duckdb.org/docs/stable/connect/concurrency) — short, and the specification behind the folklore.
- [Temporal: Workflows](https://docs.temporal.io/workflows) and [Event History](https://docs.temporal.io/encyclopedia/event-history) — replay, in the words of the people who built it.

**DuckDB, in depth**
- [Friendly SQL](https://duckdb.org/docs/stable/sql/dialect/friendly_sql) — the index to Part A §A4.
- [ASOF joins: fuzzy temporal lookups](https://duckdb.org/2023/09/15/asof-joins-fuzzy-temporal-lookups.html)
- [Out-of-core aggregation](https://duckdb.org/2024/03/29/external-aggregation.html) — what spilling actually does.
- [Tuning workloads](https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads) and [environment](https://duckdb.org/docs/stable/guides/performance/environment)
- [Parquet metadata functions](https://duckdb.org/docs/stable/data/parquet/metadata) · [S3 API](https://duckdb.org/docs/stable/extensions/httpfs/s3api) · [CREATE SECRET](https://duckdb.org/docs/stable/sql/statements/create_secret)
- [Operational limits](https://duckdb.org/docs/stable/operations_manual/limits) — read before you promise anyone anything.

**Temporal, in depth**
- [samples-python](https://github.com/temporalio/samples-python) — the most useful Python documentation that exists for this SDK. Start with `message_passing/` and `encryption/`.
- [Python SDK API reference](https://python.temporal.io/)
- [Retry policies](https://docs.temporal.io/encyclopedia/retry-policies) · [Detecting activity failures](https://docs.temporal.io/encyclopedia/detecting-activity-failures)
- [Workflow message passing](https://docs.temporal.io/encyclopedia/workflow-message-passing) — signals vs queries vs updates, properly.
- [Child workflows](https://docs.temporal.io/encyclopedia/child-workflows) · [continue-as-new](https://docs.temporal.io/develop/python/continue-as-new) · [Schedules](https://docs.temporal.io/schedule)
- [Versioning](https://docs.temporal.io/develop/python/versioning) — before your first production deploy, not after.
- [Self-hosted defaults](https://docs.temporal.io/self-hosted-guide/defaults) — every limit in one page.

**Patterns**
- [Saga pattern](https://microservices.io/patterns/data/saga.html) — Richardson's write-up, which is where §B8 comes from.

## Where next

- Practise: [ASSIGNMENTS.md](ASSIGNMENTS.md) — 26 exercises plus a capstone.
- Sequence: [ROADMAP.md](ROADMAP.md).
- Evaluate: [README.md §5](README.md#5-known-friction-the-actual-output-of-this-poc).
