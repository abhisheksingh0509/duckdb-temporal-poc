# Temporal + DuckDB: durable orchestration around a single-writer engine

A working POC for the question *"what changes when the execution engine is
embedded?"* — when there is no cluster to submit to, no server to hold a
connection open against, and exactly one process may write the database at a
time.

Short answer from building it: **DuckDB does the compute work of a small
cluster on one box, and almost every design decision that follows is about the
write side, not the read side.** The read side is easy and fast. The write side
is where an embedded engine and a distributed orchestrator disagree, and
[Known friction](#5-known-friction-the-actual-output-of-this-poc) is the most
valuable section of this repo.

```
make up      # ~90s first time (one small image, no JVM)
make demo    # guided tour, ~6 min
```

Requirements: Docker with ~6 GB available. Nothing else — no local Python.

Learning rather than evaluating? Start with **[ROADMAP.md](ROADMAP.md)** — a
six-phase path from "I have heard of both" to "I would defend this design in a
review". It sequences [LEARN.md](LEARN.md) (the concepts, taught against this
code) and [ASSIGNMENTS.md](ASSIGNMENTS.md) (24 exercises plus a capstone).

---

## 1. What it actually does

Smart-meter telemetry, one business date, five stages:

```
  landing zone (MinIO)        ┌────────────────────────────────────────────┐
  ┌──────────────────────┐    │  Temporal workflow: MeterPipeline          │
  │ readings.parquet     │    │                                            │
  │ meters.csv           │────┼─▶ phase 1  BRONZE  (4x parallel)           │
  │ tariffs.csv          │    │     type/name normalisation only           │
  │ weather.parquet      │    │              │                             │
  └──────────────────────┘    │              ▼                             │
                              │   phase 2  CLEANSE                         │
                              │     QUALIFY-dedupe on src_updated_at,      │
                              │     difference the cumulative counter,     │
                              │     quarantine with a reason column        │
                              │              │                             │
                              │              ▼                             │
                              │   phase 3  ENRICH                          │
                              │     ASOF JOIN the tariff in force,         │
                              │     ASOF LEFT JOIN regional weather        │
                              │              │                             │
                              │              ▼                             │
                              │   phase 4  GOLD  (2x parallel)             │
                              │     daily region consumption + 7d window   │
                              │     per-meter z-score vs its region        │
                              │              │                             │
                              │              ▼                             │
                              │   phase 5  PUBLISH  ── writer queue ──▶    │
                              └────────────────────────────────────────────┘
                                             │
   s3://lake  (immutable, run-scoped Parquet)│   warehouse.duckdb (one file)
   bronze/…/dt=…/run=…/data.parquet          │   gold.daily_region_consumption
   silver/…/dt=…/run=…/data.parquet          └─▶ gold.meter_anomalies
   gold/…/dt=…/run=…/data.parquet                meta.runs  meta.step_metrics
                                                 meta.dq_results  meta.lineage
                                                 meta.publish_log
```

Every step is guarded: a quality gate runs against the Parquet it just wrote, a
lineage edge is recorded, step metrics are stored, and the publish is reversible
because `meta.publish_log` remembers which run owned each date before this one.

### Three ways to size it

```bash
make run-lean     # 512MB memory limit everywhere -- the big steps spill to disk
make run-roomy    # 1GB and 4 threads everywhere
make run-auto     # Temporal probes each step's Parquet footprint and decides
```

`AUTO` is the interesting one. There is no cluster to size here, so the only
tuning surface is the DuckDB process itself — and that surface is *per step*,
decided at runtime from a measurement:

```
step                   profile                        rows_in    rows_out    rej   secs
bronze_readings        lean(auto: 8.49MB)           1,948,776   1,948,776      0    0.2
bronze_meters          lean(auto: no parquet input)    20,000      20,000      0    0.1
bronze_tariffs         lean(auto: no parquet input)        75          75      0    0.0
bronze_weather         lean(auto: 0.0MB)                  120         120      0    0.0
silver_cleanse         lean(auto: 8.49MB)           1,948,776   1,877,649 42,351    0.9
silver_enrich          lean(auto: 11.76MB)          1,897,844   1,877,649      0    1.4
gold_daily_region      roomy(auto: 21.96MB)         1,877,649           5      0    0.1
gold_meter_anomalies   roomy(auto: 21.96MB)         1,877,649         298      0    0.1
```

That is `make run-auto` on the Docker stack, copied out of `make watch`. The
megabytes are what `probe_inputs` read from the Parquet footers, and the
profiles follow from them at the default 16 MB threshold: the gold steps are the
only ones whose input crosses it.


The row counts and megabytes are real, and the profiles follow from them at the
default 16 MB threshold. The seconds are DuckDB's own, measured through the test
harness against a local filesystem — in the Docker stack each of these is
wrapped in an activity round-trip and an S3 round-trip, which dominate. See
[§8 Measured](#8-measured).

---

## 2. The one design decision

In a Spark or Flink pipeline, a Temporal task queue picks an **engine**. Here it
picks a **concurrency contract**, and that is the whole architecture:

```python
# src/duckflow/queues.py
COMPUTE = "duck-compute-tq"   # many workers, many slots, :memory: DuckDB
WRITER  = "duck-writer-tq"    # one worker, ONE slot, warehouse.duckdb
```

```python
# The workflow chooses the contract by choosing a task queue.
await workflow.execute_activity(run_step,   req, task_queue=queues.COMPUTE, ...)
await workflow.execute_activity(publish_gold, req, task_queue=queues.WRITER, ...)
```

| | compute (`duck-compute-tq`) | writer (`duck-writer-tq`) |
|---|---|---|
| DuckDB database | `:memory:` | `/data/warehouse.duckdb` |
| Reads | Parquet on S3 | Parquet on S3 + the warehouse |
| Writes | Parquet on S3 | the warehouse file |
| Replicas | as many as you like | **exactly one** |
| `max_concurrent_activities` | 4 | **1** |
| Hosts workflows | yes | no |
| Image | same | same |

Because compute activities share nothing, they cannot conflict — which is why
`make scale N=6` is safe and `docker compose up --scale worker-writer=2` is a
corruption bug rather than a throughput win.

---

## 3. What DuckDB is actually doing

The SQL is the point, not decoration. Each of these earns its place, and
[LEARN.md §A](LEARN.md#part-a--duckdb-as-a-pipeline-engine) explains why:

| Feature | Where | What it replaces |
|---|---|---|
| `QUALIFY` | `silver_cleanse` | a subquery around every `row_number()` dedupe |
| named `WINDOW` | `silver_cleanse` | three `lag()` calls repeating the same frame |
| `ASOF JOIN` | `silver_enrich` | a correlated `max(valid_from)` subquery |
| `ASOF LEFT JOIN` | `silver_enrich` | silent row loss when weather is missing |
| `GROUP BY ALL` | both gold steps | the classic "added a dimension, forgot the GROUP BY" bug |
| `* EXCLUDE (…)` | `silver_enrich` | listing 14 columns to drop 3 |
| `FILTER (WHERE …)` | `gold_daily_region` | `sum(CASE WHEN … THEN … END)` |
| `RANGE … INTERVAL` frame | `gold_daily_region` | a self-join for a rolling 7-day average |
| `ANTI JOIN` | referential checks | `NOT IN (SELECT …)` and its NULL trap |
| `read_csv` / `read_parquet` / `COPY` | everywhere | a loader |
| `parquet_file_metadata` | `probe_inputs` | scanning a file to find out how big it is |
| `duckdb_memory()` | every step | guessing whether a step spilled |
| `INSERT … BY NAME` | `publish` | a positional insert that breaks on reorder |
| `range()` + `random()` + `setseed` | the generator | numpy, pandas and faker |

The synthetic dataset is generated by DuckDB itself — the entire fixture is one
SQL script in `src/duckflow/data/generator.py`, with five classes of deliberate
defect that each have a matching branch in `silver_cleanse`.

---

## 4. Why the writer is exactly one — precisely

"DuckDB is single-writer" is true but imprecise, and the imprecision matters
because the two failure modes want different fixes. `make demo-contention`
shows both:

**Across processes, the file lock refuses the open outright.** Not only for a
second writer — a *read-only* open is refused too, while a writer holds the
file:

```
A. another process, while this one holds the file read-write
  read-write  open: REFUSED -- IO Error: Could not set lock on file …
  read-only   open: REFUSED -- IO Error: Could not set lock on file …
```

That is why the writer opens the database **per activity and closes it**, rather
than holding a connection for the life of the worker: between activities the
file is free, so `make report` works. And it is why every read path goes through
`session.open_read_only()`, which backs off and retries instead of assuming.

**Within one process, connections share an instance and are allowed.** So a
writer worker with its concurrency turned up would never hit the lock at all. It
would hit this instead, at commit time:

```
B. two connections inside one process, replacing the same date
  connection A: DELETE ok (uncommitted)
  connection B: CONFLICT -- TransactionContext Error: Conflict on tuple deletion!
```

A retry would paper over that. Serialising the queue removes it — and also
removes the read-modify-write race in `publish()`, which reads the previous
owner of a date *before* deleting it.

---

## 5. Known friction: the actual output of this POC

Everything below was found by building the thing, and each one is load-bearing
somewhere in the code.

**1. At-least-once execution forces every write to be a restatement.**
Temporal guarantees an activity runs at least once: a worker that finishes a
publish and dies before reporting will be asked to publish again. So `publish()`
is `DELETE` the business date then `INSERT` it, and every metadata write is an
upsert on a primary key. Running any of it twice changes nothing.

**2. …and the bookkeeping has to know it was retried.**
`publish_log` records which run owned a date before this one, so compensation
can restore it. The first version of that lookup found *any* previous row — so a
retried publish recorded **itself** as its own predecessor, and compensation
silently restored the state it was supposed to undo. The fix is one `WHERE
run_id <> ?`, and `test_publish_is_idempotent` exists to keep it there. This is
the subtlest bug in the repo and it is entirely a product of at-least-once.

**3. Parquet has no time travel, so reversibility has to be built.**
Iceberg or Delta would give you `ROLLBACK TO SNAPSHOT`. Plain Parquet gives you
nothing, so the substitute is: every step output is written to an immutable
`run=<run_id>` path and never mutated, and the publish log points at the
previous run's URI. Compensation is re-inserting a file that was never deleted.
The cost is storage — every failed run's Parquet is still in the lake, which
`make lake` will show you. That is a deliberate trade; the files are also the
only forensic evidence of what went wrong.

**4. Nothing large may travel through the history.**
Temporal's payload limit (2 MB) is not a suggestion — the history is the
durability mechanism, and a DataFrame in it is a DataFrame you will replay
forever. Activities here return row counts, byte counts and URIs. The data stays
in object storage; the history carries the receipt. `shared.py` is written
entirely around that rule.

**5. Cancelling DuckDB requires a second thread, and Temporal hands you one.**
`con.interrupt()` is the only way to stop a running query, and it must be called
from a thread other than the one blocked in `execute()`. Cancelling the Python
coroutine does *not* stop the thread. So every activity runs its SQL in
`asyncio.to_thread` and keeps the event loop free to heartbeat — which means the
cancellation path and the heartbeat path are the same piece of machinery
(`activities/core.py:_with_heartbeat`). This shape is worth copying.

**6. The workflow sandbox makes `steps.py` stdlib-only.**
Workflow code is re-executed from line 1 on every replay, so it cannot read the
clock, the environment or the filesystem. The step catalog is imported *inside*
the workflow, so it inherits that constraint — which turned out to be a good
thing: it forced the SQL to be pure data with no configuration baked in.

**7. Extensions must be baked into the image.**
An activity that runs `INSTALL httpfs` on first use is an activity that fails in
an air-gapped network, times out in a slow one, and does it again on every fresh
container. `docker/worker.Dockerfile` installs it at build time into
`DUCKDB_EXTENSION_DIR`; `LOAD` at runtime is then a local file read.

**8. `preserve_insertion_order = false` is what makes LEAN finish.**
DuckDB buffers a whole result to preserve row order on write. Turning that off
lets the Parquet write stream. On a 512 MB limit it is the difference between
spilling politely and not completing. It is correct here because every consumer
of these files sorts or aggregates; it would be wrong if anything depended on
file row order.

**9. A window function is only as wide as the partition you read.**
`silver_cleanse` differences a cumulative counter with `lag()`, but it only
reads one day — so the first reading of each meter each day has no predecessor
and lands in quarantine as `no_prior_reading`. At 96 intervals a day that is a
1.03 % floor on the quarantine rate, visible in the numbers above (42,351
rejects, of which exactly 20,000 -- one per meter -- are this). The same class of bug makes
`kwh_7d_avg` equal to today's value: the frame is written for seven days, and
one partition is in scope. Both are real, both are documented in the code, and
fixing them is **Assignments 12 and 13** — because noticing this in someone
else's pipeline is the actual skill.

**10. A read-only mount plus an unclean shutdown is a trap.**
`worker-compute` mounts the warehouse volume read-only, which is a nice way to
make "compute never writes" a filesystem fact rather than a convention. But if
the writer is killed mid-transaction, the leftover WAL needs replaying, and a
read-only open cannot do it — so reads fail until the writer restarts. That is
why both workers carry `restart: unless-stopped`. Durable execution resumes your
*workflow*; it does not resurrect your *container*.

**11. `memory_limit` is per connection, and concurrency multiplies it.**
This one is arithmetic rather than insight, and it is the easiest thing here to
get wrong. Each activity opens its own DuckDB connection, so a compute worker at
`max_concurrent_activities=8` with a 3 GB ROOMY profile — the defaults this
repo started with — can have 24 GB of declared cap live inside a 4 GB container. A cap is not a reservation, so it
usually does not bite — until the day several big steps peak together and the
kernel kills the worker, which Temporal then faithfully retries into the same
wall. The defaults here are now 4 concurrent activities and a 1 GB ROOMY
profile against a 4 GB limit; the point is not the numbers, it is that
`concurrency x memory_limit` versus the cgroup limit is a sum somebody has to
do, and there is nothing in either DuckDB or Temporal that will do it for you.

**12. What you do not get, and should not pretend you do.**
No cross-node shuffle, so a join whose build side exceeds one machine's disk is
out of scope. No concurrent writers, so a write-heavy serving path is out of
scope. No catalog, so schema evolution is `INSERT … BY NAME` and hope. For the
shape of workload in this repo — a few million rows a day, wide scans, heavy
aggregation — none of that costs anything, and the operational simplicity is
enormous. Knowing exactly where that stops being true is the point of running
the POC rather than reading a blog post.

---

## 6. Repo layout

```
src/duckflow/
  steps.py            THE FILE TO READ FIRST. Datasets, SQL, SLAs, expectations,
                      lineage -- the whole pipeline as data. stdlib-only.
  shared.py           every type that crosses the workflow/activity boundary
  queues.py           the two concurrency contracts
  config.py           environment -> Settings (never imported by workflow code)
  workflows/
    pipeline.py       the orchestration: fan-out, sizing, SLA timers, saga,
                      signals, queries, updates
    backfill.py       child workflows + continue-as-new
  activities/core.py  the only code allowed to touch the outside world
  duck/
    session.py        connection config order, interruptibility, COPY helpers
    warehouse.py      the single writer: publish, compensate, metadata
  quality/checks.py   nine expectation kinds, each one SQL statement
  data/generator.py   the fixture, in DuckDB SQL
  worker.py           two roles, one codebase
  cli.py              operator commands
tests/                36 tests; 29 need nothing, 7 spin up a throwaway Temporal
```

---

## 7. Command reference

```bash
make up / down / clean / ps / logs / restart / shell
make scale N=4                 # compute workers only

make run DATE=2026-08-01       # MODE=auto|lean|roomy  METERS=20000
make run-lean / run-roomy / run-auto
make run-approval              # then: make approve ID=approval-demo
make watch ID=<workflow-id>    # live progress, via a workflow query
make pause / resume ID=…
make extend-sla ID=… STEP=silver_enrich SECS=120
make backfill START=2026-09-01 END=2026-09-05
make schedule                  # a paused daily Temporal Schedule

make demo-dq-failure           # non-retryable gate -> saga rollback
make demo-warn                 # WARN check fails, run continues
make demo-chaos                # retryable failure -> 3 attempts -> compensate
make demo-contention           # what DuckDB refuses vs conflicts on
make demo-durability          # stop both workers mid-run; the workflow resumes
make demo-rollback             # good day, bad day, restored day

make report / steps / lineage / gold / warehouse / lake
make query SQL="select …"      # read-only, against the warehouse
make test                      # 29 tests, no infrastructure
make test-workflow             # 7 orchestration tests on a throwaway Temporal
                               # (first run downloads the dev server, ~100 MB)
```

UIs: Temporal <http://localhost:8234>, MinIO <http://localhost:9201>
(minioadmin/minioadmin).

---

## 8. Measured

One business date: 20 000 meters × 96 intervals plus injected duplicates,
1.95 M raw readings. `make run-auto` at its defaults on the full Docker
stack — Temporal, MinIO, two worker containers — on an M-series laptop with
8 GB given to Docker. Run it yourself and you should get these numbers, not
numbers like these:

| stage | rows in | rows out | Parquet written | seconds |
|---|---:|---:|---:|---:|
| bronze (4 steps, parallel) | 1,968,971 | 1,968,971 | 8.5 MB | 0.2 |
| silver_cleanse | 1,948,776 | 1,877,649 (+42,351 quarantined) | 11.7 MB | 0.9 |
| silver_enrich | 1,897,844 | 1,877,649 | 22.0 MB | 1.4 |
| gold (2 steps, parallel) | 3,755,298 | 303 | 10 KB | 0.2 |
| **end to end, including publish** | | **303 rows published** | | **4.1** |

Two things in that table are worth more than the numbers themselves.

**DuckDB is not the cost.** The steps sum to about 2.7 seconds of SQL; the run
takes 4.1. The rest is activity dispatch, heartbeats, quality gates and S3
round-trips. At this scale you are paying for coordination, not compute — which
is the right moment to ask how much coordination you actually need, and the
honest answer for a single daily batch is "less than this". The orchestration
earns its keep at the *failure* boundaries, not the happy path.

**The fixture is reproducible.** Three runs of the same date produce identical
row counts, byte for byte. That took more than `setseed()` — see the comment on
the `rnd` macro in `data/generator.py`, which is the most transferable twenty
lines in the repo.

## 9. Where to go next

| If you want to… | Read |
|---|---|
| learn this properly, in order | **[ROADMAP.md](ROADMAP.md)** |
| understand a concept | [LEARN.md](LEARN.md) |
| actually practise | [ASSIGNMENTS.md](ASSIGNMENTS.md) |
| change the pipeline | `src/duckflow/steps.py` — it is all there |
