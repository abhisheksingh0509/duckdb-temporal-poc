# Roadmap: from "heard of both" to "would defend this in a review"

Six phases. Each names what to read, what to do, and — the part most learning
plans leave out — how you know you are finished with it. Roughly 40–55 hours of
real work, or five to seven weeks at an hour a day.

Do them in order. Phase 3 is where the actual learning is; Phases 1 and 2 exist
so that Phase 3 is legible.

| Phase | Focus | Time | Ends when |
|---|---|---|---|
| 0 | See it work | 1–2 h | you can describe the pipeline from memory |
| 1 | DuckDB as a pipeline engine | 6–8 h | you can add a step without reading the Temporal code |
| 2 | Temporal's execution model | 6–8 h | you can predict what a history will contain |
| 3 | **The intersection** | 8–10 h | you can explain why every write here is a restatement |
| 4 | Scale and operate | 6–8 h | you know where this design stops, with numbers |
| 5 | Own it | 10–15 h | you have shipped the capstone |

A note on how to use it: the assignments are the point, and the reading exists
to make them tractable. If you find yourself three hours into LEARN.md without
having run anything, you are doing it backwards.

---

## Phase 0 — See it work
**1–2 hours. Goal: a mental model of what this thing does, before any theory.**

```bash
make up
make demo          # the guided tour, ~6 min, do not skip it
make steps         # the whole pipeline, declared
make gold
make report
```

Then click through one run in the Temporal UI at <http://localhost:8234>, and
look at the lake in the MinIO console at <http://localhost:9201>.

**Read:** [README §1–§3](README.md#1-what-it-actually-does).

**Done when** you can, without looking: name the five phases, say what
`silver_cleanse` throws away and why, and explain what the two task queues are
for.

**Common trap:** skipping to the code. The demo takes six minutes and saves an
hour of confusion about why there are two workers.

---

## Phase 1 — DuckDB as a pipeline engine
**6–8 hours. Goal: fluency in the SQL and the process knobs.**

**Read**
- [LEARN Part A](LEARN.md#part-a--duckdb-as-a-pipeline-engine), all of it.
- `src/duckflow/steps.py`, top to bottom. This is the file the repo exists to make readable.
- `src/duckflow/duck/session.py`.
- Upstream, in this order: [Friendly SQL](https://duckdb.org/docs/stable/sql/dialect/friendly_sql), then [the ASOF join post](https://duckdb.org/2023/09/15/asof-joins-fuzzy-temporal-lookups.html), then [tuning workloads](https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads).

**Do** — Assignments **4, 5, 6, 7, 9, 10, 11**, then **8** as the checkpoint.

Along the way, in a `make shell`:
```sql
SUMMARIZE silver_enriched;          -- do this one first
SELECT * FROM duckdb_settings() WHERE name LIKE '%memory%';
EXPLAIN ANALYZE <one of the gold selects>;
```

**Done when** you can write an `ASOF JOIN` from memory, say what `QUALIFY`,
`GROUP BY ALL` and `* EXCLUDE` replace, and explain out loud — to another
person, not to yourself — why `preserve_insertion_order = false` decides whether
a LEAN run finishes.

Assignment 8 is the real gate: if you had to touch anything outside `steps.py`
to add a step, work out why before moving on.

**Common trap:** treating the SQL features as trivia. They are the reason this
pipeline is 600 lines of declaration instead of 3 000 lines of DataFrame code,
and two of them (`ASOF JOIN`, `RANGE` frames) are different algorithms rather
than shorter syntax.

---

## Phase 2 — Temporal's execution model
**6–8 hours. Goal: you can predict a history before you open it.**

**Read**
- [LEARN Part B](LEARN.md#part-b--temporal-as-a-data-orchestrator), all of it.
- `src/duckflow/workflows/pipeline.py` — `run()` first, then `_run_phase`, `_with_sla`, `_quality_gate`, `_compensate`, in that order.
- `src/duckflow/worker.py`.
- Upstream: [Workflows](https://docs.temporal.io/workflows), [Event History](https://docs.temporal.io/encyclopedia/event-history), [Retry policies](https://docs.temporal.io/encyclopedia/retry-policies), and an hour in [samples-python](https://github.com/temporalio/samples-python) — which is, honestly, better than the prose docs for this SDK.

**Do** — Assignments **1, 2, 3, 14, 17, 21, 22**.

**Done when** you can answer these cold:
1. What is replay, and what triggers it?
2. Why can a workflow not call `datetime.now()`, and what is it allowed to call?
3. Signal, query, update — what is the difference, and when do you reach for each?
4. What happens if a worker dies mid-activity? Mid-workflow-task? While a workflow is parked on a timer?
5. Why does a failed quality gate use `non_retryable=True`?

Then run `make test-workflow` and read `tests/test_pipeline_workflow.py`. (The
first run downloads a Temporal dev server, ~100 MB; later runs take about thirty
seconds.) Seven tests cover the happy path, check attribution across parallel
steps, a non-retryable gate, a WARN-only gate, chaos retries, re-publishing a
date, and the approval gate — the compact version of this whole phase.

**Common trap:** reading Temporal's tutorials and assuming a data pipeline is
just a longer one. The parts that matter here — compensation, typed errors,
payload discipline — barely appear in a hello-world workflow.

---

## Phase 3 — The intersection
**8–10 hours. This is the phase worth the whole exercise.**

**Read**
- [LEARN Part C](LEARN.md#part-c--the-intersection), twice.
- [README §4](README.md#4-why-the-writer-is-exactly-one--precisely) and [§5](README.md#5-known-friction-the-actual-output-of-this-poc).
- `src/duckflow/duck/warehouse.py`, line by line. Every statement in it is shaped by something in Part C.
- `tests/test_warehouse.py` — the executable version of the same argument.
- Upstream: [DuckDB concurrency](https://duckdb.org/docs/stable/connect/concurrency) and [Temporal's limits](https://docs.temporal.io/self-hosted-guide/defaults). Both are short and both are load-bearing.

**Do** — Assignments **12, 13, 18, 19, 20**.

Those five are the core. 18 makes you break the concurrency contract; 19 and 20
make you break idempotency and reversibility and watch what it costs; 12 and 13
are the two real correctness bugs left in the repo on purpose.

**Done when** you can explain, to a sceptical colleague:
1. Why every write in `warehouse.py` is a restatement rather than an append.
2. Why `AND run_id <> ?` is in the `previous` lookup, and the failure it prevents.
3. What compensation actually does, and exactly what it cannot do.
4. Why activities return URIs instead of data, and what that costs.
5. Why `con.interrupt()` has to come from another thread, and how `_with_heartbeat` arranges that.

**Common trap:** believing you understand idempotency because you can define it.
Assignment 19 takes an hour and changes that.

---

## Phase 4 — Scale and operate
**6–8 hours. Goal: know where this design stops, in numbers.**

**Read**
- [LEARN §B9–B12](LEARN.md#b9-child-workflows-and-continue-as-new) and [§C7](LEARN.md#c7-the-scaling-model-and-where-it-stops).
- `src/duckflow/workflows/backfill.py`.
- Upstream: [child workflows](https://docs.temporal.io/encyclopedia/child-workflows), [continue-as-new](https://docs.temporal.io/develop/python/continue-as-new), [Schedules](https://docs.temporal.io/schedule).

**Do** — Assignments **15, 16, 23, 24**, plus:
```bash
make scale N=4
make backfill START=2026-09-01 END=2026-09-14
make demo-durability
```
While the backfill runs, watch the writer queue's schedule-to-start latency in
the Temporal UI. That single number is the whole scaling story.

**Done when** you can say, with numbers: how many compute workers this design
uses well, what the writer's actual throughput ceiling is, at what data volume
you would stop using a single DuckDB process, and which of those three you would
hit first.

**Common trap:** concluding either "it doesn't scale" or "it scales fine". Both
are wrong and neither is a sentence anyone can act on. The useful answer is a
number with a bottleneck attached.

---

## Phase 5 — Own it
**10–15 hours. Goal: design judgement, not recall.**

**Do** — Assignments **25, 26**, then the **capstone**.

25 makes you build an atomic two-table publish and discover why table formats
exist. 26 makes you remove the constraint the whole architecture is arranged
around, and find out which decisions were about DuckDB and which were about
distributed systems generally. The capstone makes it incremental, which is where
every earlier lesson has to hold at once.

**Done when** you can write the last capstone deliverable convincingly: *what
you would do differently with Iceberg underneath, and whether that would have
been the cheaper way to get here.*

---

## If you only have one evening

```bash
make up && make demo            # 15 min
make demo-contention            # 5 min  -- the single-writer rule, precisely
make demo-rollback              # 5 min  -- the saga, end to end
```
Then read [README §4](README.md#4-why-the-writer-is-exactly-one--precisely) and
[§5](README.md#5-known-friction-the-actual-output-of-this-poc), and
[LEARN Part C](LEARN.md#part-c--the-intersection). Ninety minutes, and it is the
argument this repo makes.

## If you are evaluating rather than learning

[README](README.md) §1, §4, §5 and §8, then `make demo-contention` and
`make demo-rollback`. §5 is what a POC is actually for; the rest is context.

---

## The self-check

You have the mastery this repo was built to teach when you can answer all of
these cold, and defend the answers.

**DuckDB**
1. When is an embedded engine the right choice, and what is the first thing that breaks when it stops being?
2. What does `ASOF JOIN` replace, and why is `ASOF LEFT JOIN` a correctness decision rather than a preference?
3. How do you make DuckDB process more data than it has RAM, and how do you find out whether it did?
4. What exactly does DuckDB refuse across processes, and what does it merely conflict on within one?

**Temporal**
5. Explain replay to someone who has not heard of it, in three sentences.
6. Why is a failed data-quality check non-retryable, and what would retrying it cost?
7. What does a durable timer give you that a cron watchdog does not?
8. When do you reach for a child workflow instead of an activity?

**Together**
9. Why is a task queue a good place to put a concurrency constraint?
10. Write the rule that makes an activity safe under at-least-once execution, in one sentence.
11. How do you make a Parquet lake reversible, and precisely what does that fail to give you?
12. A window function in a partitioned pipeline is only as wide as ______. Give two bugs in this repo that are instances of it.

If a question is uncomfortable, its phase is in the table at the top.
