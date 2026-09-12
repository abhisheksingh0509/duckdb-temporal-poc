# Assignments

Twenty-four exercises plus a capstone, worked against the running stack. They
are ordered and later ones assume earlier ones. Read the named
[LEARN.md](LEARN.md) section first — each assignment says which.

```bash
make up          # stack (~90s first time)
make test        # 29 tests, no infrastructure
```

**Difficulty:** ● observe · ●● small change · ●●● design change · ●●●● build something

### Working rules

- **Edit on the host, then `make restart`.** `src/` is bind-mounted into both workers; there is no rebuild.
- **Terminate in-flight workflows before changing workflow code.** Replaying an old history against new code raises `NonDeterminismError` — which is Assignment 21, but you do not want it by accident:
  ```bash
  docker compose exec temporal temporal workflow list --address temporal:7233
  docker compose exec temporal temporal workflow terminate \
      --address temporal:7233 --workflow-id <id> --reason "clearing"
  ```
  Remember that `steps.py` is imported *into* the workflow, so editing SQL counts as changing workflow code.
- **Use small runs while iterating.** `--meters 1000 --no-approval` turns a minute into a few seconds.
- **Write down the answer to each "Report" question.** Half of these are about noticing something, not producing code.
- **Revert experiments before moving on** unless the assignment says to keep the change. `git diff` is your undo list.

---

## Level 0 — Observe

### 1. Read one event history end to end ●
*LEARN §B0, §B12. ~30 min. No code.*

```bash
make run METERS=1000
```

Open <http://localhost:8234>, find the workflow, open **Event History**, switch
to the full/JSON view.

**Report:**
1. What are the first three events, in order?
2. Pick one activity. Which three event types does it produce, and which one carries the result payload?
3. The four bronze steps run in parallel. Are their `ActivityTaskScheduled` events adjacent, or interleaved with their completions? What does that tell you about when your workflow code was actually executing?
4. Count the `WorkflowTaskScheduled`/`Started`/`Completed` triples. What causes a new one?
5. Find a timer. Was it fired or cancelled, and which step did it belong to?

### 2. Map the two task queues ●
*LEARN §B1, §C1. ~20 min.*

```bash
make logs        # in one terminal
make run METERS=1000
```

**Report:**
1. Which activities ran on `duck-compute-tq` and which on `duck-writer-tq`? List them.
2. In the UI, open a `publish_gold` activity. Which worker identity executed it?
3. `make scale N=3`, run again. Did the bronze steps land on different workers? How can you tell from the history alone?
4. Why does `worker-writer` host zero workflows? Name two things that would go wrong if it hosted them.

### 3. Watch a run without reading a log ●
*LEARN §B7. ~20 min.*

```bash
make run-approval                    # parks at the gate
make watch ID=approval-demo --follow # in another terminal
make approve ID=approval-demo
```

**Report:**
1. `watch` uses a workflow *query*. Where does the data it prints actually live?
2. While the run is parked awaiting approval, how much worker memory does it occupy? Verify your answer: `make down`, wait, `make up`, then `make approve ID=approval-demo`.
3. Why must `progress()` not mutate anything?

### 4. Find out whether a step spilled ●●
*LEARN §A0, §A1, §A5. ~30 min.*

```bash
make run-lean METERS=40000
make run-roomy METERS=40000
make query SQL="select run_id, step_key, memory_limit, threads, round(duration_seconds,2) secs from meta.step_metrics order by run_id, duration_seconds desc"
```

**Report:**
1. Which steps got slower under LEAN, and by how much?
2. `_memory_note()` reports `temporary_storage_bytes` from `duckdb_memory()`. Add it to `meta.step_metrics` (it is already computed — it is just not stored) and re-run. Which steps actually spilled?
3. Set `DUCKDB_TEMP_DIR` to an empty string in `docker-compose.yml`, restart, and run LEAN again. What error do you get, and at which step?

---

## Level 1 — DuckDB fluency

### 5. Rewrite `QUALIFY` the long way, and back ●●
*LEARN §A2, §A4. ~30 min.*

In `steps.py`, replace the `QUALIFY` in `silver_cleanse`'s first `pre` statement
with an equivalent ANSI subquery. Run. Then put it back.

**Report:**
1. How many extra lines, and how many extra names did you have to invent?
2. Run both through `EXPLAIN` (`make shell`, then `python -m duckflow.cli query "EXPLAIN <sql>"`). Do the plans differ?
3. The dedupe orders by `src_updated_at DESC`. The generator emits stale duplicates with an *earlier* `src_updated_at` and a value 2 % low. Change `DESC` to `ASC` and run. Which quality check catches it, and which ones do not?

### 6. Break the ASOF JOIN on purpose ●●
*LEARN §A4. ~30 min.*

In `silver_enrich`, change the ASOF predicate from `>=` to `>`.

**Report:**
1. How many rows lose their price? Which check fires?
2. Now remove the `TIMESTAMP '2000-01-01'` base row from the generator's tariff output. What happens, and why is that row in the fixture at all?
3. Rewrite the ASOF JOIN as a conventional join plus a correlated `max(valid_from)` subquery. Compare wall-clock time at `--meters 40000`.

### 7. Cause silent data loss ●●
*LEARN §A4. ~20 min.*

Change `ASOF LEFT JOIN bronze_weather` to a plain `ASOF JOIN`, then make the
generator emit weather for hours 0–5 only.

**Report:**
1. How many rows disappear? Which check catches it — and would it have caught a 2 % loss as well as a 75 % one?
2. Why is `enrich_preserves_volume` a `sum_close_to` check rather than a row-count check? What does each one catch that the other misses?

### 8. Add a step ●●●
*LEARN §A2–A4. ~90 min.*

Add `gold_hourly_load_profile`: per region and hour of day, total kWh, mean
temperature, and each hour's share of the region's daily total.

You must touch: `DATASETS`, a new `Step`, `PHASES`, `PUBLISHED`, and its
expectations. You should not have to touch `activities/`, `workflows/` or
`worker.py` at all — if you do, say which and why.

**Report:**
1. Which files did you change? (The answer should be one.)
2. What did `make steps` say before you restarted the workers, and after?
3. Add a check that the hourly shares sum to 100 % per region per day. Which existing `kind` fits, or did you need a new one?

### 9. Make a query spill, then make it not ●●
*LEARN §A1, §A3, §A5. ~45 min.*

```bash
make run METERS=200000 MODE=lean
```

**Report:**
1. Which step spilled? By how much?
2. Set `preserve_insertion_order = true` in `session.py:_apply` and re-run. What changed?
3. Raise LEAN's `memory_limit` in `PROFILES` until nothing spills. What is the smallest value that works, and what does that tell you about sizing the container?

### 10. Size a step without reading it ●●
*LEARN §A2, §A3, §A6. ~30 min.*

**Report:**
1. `make run-auto METERS=40000`, then `make query SQL="select step_key, memory_limit, threads from meta.step_metrics where run_id = (select run_id from meta.runs order by started_at desc limit 1)"`. Which steps got ROOMY?
2. Drop `--threshold-mb` to 1 and re-run. Which steps changed?
3. Time `probe_inputs` in the history. How does it compare to a `select count(*)` over the same file? Why?

### 11. Add a quality check kind ●●●
*LEARN §A4. ~60 min.*

Add `monotonic_within`: asserts that a column never decreases within a
partition, ordered by another column. Use it to assert `cumulative_kwh` is
non-decreasing per `meter_id` in `silver_readings`.

**Report:**
1. Write it as *one* SQL statement. (Hint: `lag()` and a `count(*) FILTER`.)
2. Does it pass? If not, is the data wrong or the expectation wrong?
3. Make it a `WARN` and then an `ERROR`. What is the observable difference in the workflow?

### 12. Fix the day-boundary gap ●●●
*LEARN §A4, §C7. ~90 min. **Keep this change if you like it.***

`silver_cleanse` quarantines the first reading of every meter every day as
`no_prior_reading`, because `lag()` cannot see the previous partition. At 96
intervals a day that is a 1.04 % floor on the reject rate.

Fix it by having `silver_cleanse` also read the previous day's
`bronze_readings`, use it only to seed the `lag()`, and emit only the current
day.

**Report:**
1. Where did the previous day's URI have to come from? Why could the *activity* not simply compute it?
2. What happens on the very first date, when there is no previous partition?
3. What did the quarantine rate drop to? Should `quarantine_rate`'s threshold move?
4. What did you just pay for this, in bytes read per run?

### 13. Make the rolling average real ●●●
*LEARN §A4, §C7. ~90 min.*

`gold_daily_region.kwh_7d_avg` is written as a 7-day `RANGE` frame but only ever
sees one partition, so it equals today's value. Make it real.

**Report:**
1. You have at least three options: read the last seven `dt=` partitions of `silver_enriched`; read the last seven days out of the published warehouse table; or keep a rolling state table. Which did you pick, and what does each cost on a backfill that runs days out of order?
2. Run `make backfill START=2026-09-01 END=2026-09-10` and check the values. Does your fix survive out-of-order execution? Prove it.
3. This bug and Assignment 12 are the same bug. State it in one sentence.

---

## Level 2 — Temporal fluency

### 14. Change the retry policy and watch it ●●
*LEARN §B4. ~45 min.*

```bash
make demo-chaos     # fails silver_enrich, retryable
```

**Report:**
1. How many attempts, and at what intervals? Find them in the history.
2. Set `maximum_attempts=5` and `backoff_coefficient=1.0`. Predict the timings before you run it, then check.
3. Add `"ChaosError"` to `non_retryable_error_types`. How does the history differ?
4. `make demo-dq-failure` fails non-retryably. How many attempts does *that* take, and why is that the right number?

### 15. Make the SLA hard ●●●
*LEARN §B6. ~60 min.*

`_with_sla` records a breach and keeps waiting. Make it cancel the step instead,
and make the workflow fail with a clear error.

**Report:**
1. One line does the cancelling. Which?
2. What does the activity see when the workflow cancels it? Trace it through `_with_heartbeat`.
3. Set `silver_enrich`'s SLA to 1 second and run at `--meters 200000`. Does the DuckDB query actually stop, or does the activity just stop being waited on? **Prove your answer** — do not guess.
4. Why is a soft SLA the better default for a data pipeline?

### 16. Cancel a running query and prove the interrupt landed ●●●
*LEARN §B5, §C5. ~60 min.*

```bash
make run METERS=400000 &
# then, from the UI or the CLI, cancel the workflow
```

**Report:**
1. Add a log line to `_Live.interrupt()` and show it firing.
2. Remove the `live.interrupt()` call. What changes — in the worker's CPU usage, and in how long the container takes to shut down?
3. Why can `interrupt()` not be called from the thread running the query?
4. What would happen if the activity used `activity_executor` with a process pool instead of `asyncio.to_thread`?

### 17. Break the single-writer rule ●●●
*LEARN §A7, §C1. ~60 min.*

```bash
make demo-contention        # first, understand what it shows
```

Now set `MAX_CONCURRENT_ACTIVITIES: "4"` on `worker-writer`, restart, and run
two pipelines for the **same business date** concurrently.

**Report:**
1. What error do you get, and from which statement? Is it the file-lock error or the transaction-conflict error? Why that one?
2. Now run two pipelines for *different* dates concurrently with concurrency 4. Does it fail? Does that mean it is safe?
3. There is a second problem that a transaction conflict would not reveal: `publish()` reads the previous owner of a date *before* deleting it. Describe the interleaving that corrupts `publish_log`, and say whether a retry would fix it.
4. Try `--scale worker-writer=2`. What fails, and how quickly?

### 18. Break idempotency on purpose ●●●
*LEARN §A8, §C2. ~60 min.*

Change `publish()` to `INSERT` without the `DELETE`. Then make the publish
activity fail *after* it commits but before it returns:

```python
warehouse.publish(req)
raise ApplicationError("simulated post-commit crash", type="Chaos")
```

**Report:**
1. How many rows are in `gold.daily_region_consumption` after the retries?
2. Restore the `DELETE`. Run the same experiment. What changes?
3. `meta.step_metrics` has a primary key on `(run_id, step_key)`. Drop it and re-run. What breaks, and when would you have noticed in production?

### 19. Make compensation restore the wrong thing ●●●
*LEARN §B8, §C2, §C3. ~60 min.*

In `warehouse.publish`, delete the `AND run_id <> ?` clause from the `previous`
lookup. Then:

```bash
make demo-rollback
```
…but first make the publish activity fail once and retry (Assignment 18's
technique) so that the bad run publishes twice.

**Report:**
1. What does `previous_run_id` become on the retry?
2. What does compensation restore, and why is that exactly wrong?
3. Which test catches this? Run it and read it.
4. Write down the general rule this is an instance of.

### 20. Signals, queries and updates on a parked run ●●
*LEARN §B7. ~45 min.*

```bash
make run-approval
make watch ID=approval-demo --follow
make pause ID=approval-demo
make extend-sla ID=approval-demo STEP=silver_enrich SECS=300
make resume ID=approval-demo
make approve ID=approval-demo
```

**Report:**
1. `extend-sla` is an update, not a signal. Send `extend-sla` with a step name that does not exist. What does the caller see? What would a signal have done?
2. Send `approve` twice. What happens? Is `approve_publish` idempotent, and does it need to be?
3. Signal a workflow that has already finished. What error?
4. Find the `WorkflowExecutionSignaled` and `WorkflowExecutionUpdateAccepted` events in the history.

### 21. Cause a `NonDeterminismError` ●●
*LEARN §B3, §B11. ~45 min.*

```bash
make run-approval              # parks, holding a history
```
Now, without terminating it, add a step to `PHASES` in `steps.py` and
`make restart`. Then approve it.

**Report:**
1. What is the exact error, and where does it appear — worker log, UI, or both?
2. Why did editing `steps.py` cause a *workflow* non-determinism error?
3. Change the body of an activity instead (e.g. add a log line in `_run_step`) with a workflow parked. Does that break? Why not?
4. Fix it with `workflow.patched()`. What is the lifecycle of a patch?

---

## Level 3 — Design

### 22. Backfill fourteen days ●●●
*LEARN §B9. ~90 min.*

```bash
make backfill START=2026-09-01 END=2026-09-14
```

**Report:**
1. How many workflow executions exist afterwards, and what are their ids?
2. Find the `continue_as_new`. How many coordinator executions were there, and what state crossed each boundary?
3. Kill `worker-compute` halfway through. What resumes, and what restarts from the beginning?
4. Raise `--parallel` to 8. Does it get faster? Look at the writer queue's activity schedule-to-start latency in the UI before you answer.
5. Re-issue the identical backfill command while it is still running. What happens, and which property of the workflow id causes it?

### 23. Publish two tables atomically ●●●●
*LEARN §A8, §C3. ~3 h.*

Today each publish target is its own transaction, so a failure on the second
table leaves the first committed. Make the publish all-or-nothing.

**Report:**
1. DuckDB has no nested transactions. How did you structure it?
2. What happens to `publish_log` in your version if the second table fails?
3. Now make it survive the *process* dying between the two commits. Can you? What is the smallest change to the design that would let you — and is it worth it here?
4. Compare your answer to what Iceberg or Delta would give you for free. Write the two-sentence version you would say in a design review.

### 24. Take the writer out ●●●●
*LEARN §C1, §C7. ~4 h.*

Replace `warehouse.duckdb` with Postgres as the serving store, keeping DuckDB
for all compute. Keep the `publish_gold` activity's signature; change only what
is behind it. DuckDB's `postgres` extension can `COPY` straight into it, or you
can write Parquet and load it.

**Report:**
1. Can you now raise the writer queue's concurrency? To what, and what actually limits it?
2. Which of the design decisions in [LEARN §C](LEARN.md#part-c--the-intersection) became unnecessary? Which stayed necessary anyway?
3. What did you lose? Be specific: name three things that were free with a single file.
4. On what evidence would you make this change for real?

---

## Capstone — make it incremental ●●●●
*~6–10 h. No solution is provided; there are several good ones.*

The pipeline reprocesses a whole business date on every run. Make it
incremental: a run should process only readings that arrived since the last
successful run for that date, and produce gold tables that are correct for the
whole date anyway.

Constraints:

- **Late data is normal.** A reading for 06:00 can arrive at 23:00, and after the date has already been published.
- **Gold must stay correct.** `kwh_7d_avg` and the per-region z-score both depend on the full picture.
- **At-least-once still applies.** Any activity may run twice.
- **The single writer is still single.**

Deliverables:

1. A design note: where the watermark lives, what "since the last successful run" means precisely, and what happens when a run fails halfway.
2. The implementation.
3. A test that proves correctness under out-of-order arrival — publish a date, arrive late data, re-run, assert the gold numbers match a full reprocess.
4. A paragraph on what you would do differently with Iceberg underneath, and whether that would have been the cheaper way to get here.

**If you can write deliverable 4 convincingly, you have the mastery this repo
was built to teach.** The rest is practice.
