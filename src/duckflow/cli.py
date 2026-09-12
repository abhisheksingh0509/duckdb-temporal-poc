"""Operator CLI. Runs inside the compute worker container, where Temporal, the
object store and the warehouse volume are all reachable by service name.

    python -m duckflow.cli run --date 2026-08-01 --wait
    python -m duckflow.cli watch meter-2026-08-01-120000 --follow
    python -m duckflow.cli query "select * from gold.daily_region_consumption"
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timedelta

from temporalio.client import Client, ScheduleAlreadyRunningError  # noqa: F401
from temporalio.client import (
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleSpec,
    ScheduleState,
    WorkflowFailureError,
    WorkflowUpdateFailedError,
)

from duckflow import queues, steps
from duckflow.config import settings
from duckflow.duck import session, warehouse
from duckflow.observability import logs
from duckflow.shared import Mode, PipelineInput
from duckflow.workflows.backfill import BackfillInput, MeterBackfillWorkflow
from duckflow.workflows.pipeline import MeterPipelineWorkflow


# --------------------------------------------------------------------------
# table printing -- deliberately not pandas; the worker image stays small
# --------------------------------------------------------------------------


def print_table(columns: list[str], rows: list[tuple], limit: int = 200) -> None:
    if not rows:
        print("  (no rows)")
        return
    rows = rows[:limit]
    cells = [[_fmt(v) for v in row] for row in rows]
    widths = [
        max(len(str(columns[i])), max((len(r[i]) for r in cells), default=0))
        for i in range(len(columns))
    ]
    print("  " + "  ".join(str(c).ljust(widths[i]) for i, c in enumerate(columns)))
    print("  " + "  ".join("-" * widths[i] for i in range(len(columns))))
    for r in cells:
        print("  " + "  ".join(r[i].ljust(widths[i]) for i in range(len(columns))))


def _fmt(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        return f"{v:,.4f}".rstrip("0").rstrip(".") if abs(v) < 1e12 else str(v)
    if isinstance(v, int):
        return f"{v:,}"
    return str(v)


def read_query(sql: str, limit: int = 200) -> None:
    con = session.open_read_only(settings().warehouse_path)
    try:
        cur = con.execute(sql)
        columns = [d[0] for d in cur.description] if cur.description else []
        print_table(columns, cur.fetchall(), limit)
    finally:
        con.close()


# --------------------------------------------------------------------------
# temporal helpers
# --------------------------------------------------------------------------


async def client() -> Client:
    cfg = settings()
    return await Client.connect(cfg.temporal_address, namespace=cfg.temporal_namespace)


def default_id(business_date: str) -> str:
    # Wall clock, deliberately: this runs in the CLI, not in a workflow.
    return f"meter-{business_date}-{datetime.now().strftime('%H%M%S')}"


# --------------------------------------------------------------------------
# commands
# --------------------------------------------------------------------------


async def cmd_run(args: argparse.Namespace) -> int:
    c = await client()
    wid = args.id or default_id(args.date)
    inp = PipelineInput(
        business_date=args.date,
        run_id=wid,
        lake_uri=settings().lake_uri,
        mode=args.mode,
        meters=args.meters,
        require_approval=not args.no_approval,
        approval_timeout_seconds=args.approval_timeout,
        seed=not args.no_seed,
        publish=not args.no_publish,
        fail_step=args.fail_step,
        fail_after_publish=args.fail_after_publish,
        inject=args.inject,
        auto_threshold_mb=args.threshold_mb,
    )
    handle = await c.start_workflow(
        MeterPipelineWorkflow.run,
        inp,
        id=wid,
        task_queue=queues.COMPUTE,
        execution_timeout=timedelta(hours=2),
    )
    print(f"started {wid}   http://localhost:8234/namespaces/default/workflows/{wid}")
    if not args.wait:
        return 0
    try:
        out = await handle.result()
    except WorkflowFailureError as exc:
        print(f"\nFAILED: {exc.cause}")
        await _print_run_report(args.date, limit=1)
        return 1
    print(
        f"\n{out.status}  steps ok={out.steps_succeeded} failed={out.steps_failed}  "
        f"rows published={out.rows_published:,}  {out.duration_seconds}s"
    )
    if out.sla_breaches:
        print(f"SLA breaches: {', '.join(out.sla_breaches)}")
    await _print_steps_for_run(wid)
    return 0


async def cmd_backfill(args: argparse.Namespace) -> int:
    c = await client()
    wid = f"backfill-{args.start}-to-{args.end}"
    handle = await c.start_workflow(
        MeterBackfillWorkflow.run,
        BackfillInput(
            start_date=args.start,
            end_date=args.end,
            mode=args.mode,
            meters=args.meters,
            lake_uri=settings().lake_uri,
            parallelism=args.parallel,
            checkpoint_every=args.checkpoint_every,
        ),
        id=wid,
        task_queue=queues.COMPUTE,
        execution_timeout=timedelta(hours=12),
    )
    print(f"started {wid}")
    if args.wait:
        out = await handle.result()
        print(f"completed={len(out.completed)} failed={len(out.failed)} rows={out.rows_published:,}")
        if out.failed:
            print("failed dates:", ", ".join(out.failed))
    return 0


async def cmd_signal(args: argparse.Namespace) -> int:
    c = await client()
    handle = c.get_workflow_handle(args.workflow_id)
    if args.command == "approve":
        await handle.signal(MeterPipelineWorkflow.approve_publish, args.actor)
        print(f"approved {args.workflow_id} (by {args.actor or 'unknown'})")
    elif args.command == "pause":
        await handle.signal(MeterPipelineWorkflow.pause)
        print("paused")
    elif args.command == "resume":
        await handle.signal(MeterPipelineWorkflow.resume)
        print("resumed")
    elif args.command == "abort":
        await handle.signal(MeterPipelineWorkflow.abort, args.reason)
        print("abort signalled")
    return 0


async def cmd_extend_sla(args: argparse.Namespace) -> int:
    c = await client()
    handle = c.get_workflow_handle(args.workflow_id)
    try:
        total = await handle.execute_update(
            MeterPipelineWorkflow.extend_sla, args=[args.step, args.seconds]
        )
    except WorkflowUpdateFailedError as exc:
        # The update's validator rejected it, which is the whole reason this is
        # an update and not a signal: a signal with a bad step key would have
        # been accepted, recorded in the history, and silently done nothing.
        print(f"rejected: {exc.cause}")
        return 1
    print(f"{args.step} SLA extended by {args.seconds}s (total extension {total}s)")
    return 0


async def cmd_watch(args: argparse.Namespace) -> int:
    c = await client()
    handle = c.get_workflow_handle(args.workflow_id)
    while True:
        p = await handle.query(MeterPipelineWorkflow.progress)
        print(
            f"\n[{p['status']}] phase={p['phase']} paused={p['paused']} "
            f"approved={p['approved']} rows_published={p['rows_published']:,}"
        )
        rows = [
            (
                s["step_key"], s["state"], s["profile"], s["threads"], s["memory_limit"],
                s["rows_in"], s["rows_out"], s["rows_rejected"],
                f"{s['duration_seconds']:.1f}", "yes" if s["sla_breached"] else "",
                f"{s['dq_failed']}/{s['dq_run']}",
            )
            for s in p["steps"]
        ]
        print_table(
            ["step", "state", "profile", "thr", "memory", "rows_in", "rows_out",
             "rejected", "secs", "sla!", "dq_fail"],
            rows,
        )
        if not args.follow or p["status"] in {"succeeded", "failed", "compensated", "aborted"}:
            return 0
        await asyncio.sleep(args.interval)


async def cmd_steps(args: argparse.Namespace) -> int:
    print("\nphases run in order; steps inside a phase run concurrently\n")
    for phase, keys in steps.PHASES:
        print(f"  {phase}")
        for key in keys:
            s = steps.step(key)
            print(f"    {key:22} sla={s.sla_seconds:>3}s  {s.title}")
            print(f"      reads  {', '.join(s.inputs)}")
            print(f"      writes {', '.join(s.outputs)}")
            if s.expectations:
                print(f"      checks {', '.join(e.label() for e in s.expectations)}")
    print("\npublished to the warehouse:")
    for ds, table in steps.PUBLISHED.items():
        print(f"    {ds:22} -> {table}")
    problems = steps.validate_catalog()
    print(f"\ncatalog: {'valid' if not problems else problems}")
    return 0


async def cmd_report(args: argparse.Namespace) -> int:
    await _print_run_report(args.date, args.limit)
    return 0


async def _print_run_report(business_date: str | None, limit: int) -> None:
    where = f"WHERE business_date = DATE '{business_date}'" if business_date else ""
    print("\nruns")
    read_query(
        f"""
        SELECT run_id, business_date, mode, status, rows_published,
               round(date_diff('second', started_at, finished_at), 1) AS secs,
               nullif(left(error, 60), '') AS error
        FROM meta.runs {where}
        ORDER BY started_at DESC LIMIT {limit}
        """
    )
    print("\nstep metrics (most recent run)")
    read_query(
        """
        SELECT step_key, layer, state, attempts, rows_in, rows_out, rows_rejected,
               round(duration_seconds, 2) AS secs, memory_limit, threads,
               sla_breached
        FROM meta.step_metrics
        WHERE run_id = (SELECT run_id FROM meta.runs ORDER BY started_at DESC LIMIT 1)
        ORDER BY layer, step_key
        """
    )
    print("\nquality checks that did not pass (most recent run)")
    read_query(
        """
        SELECT step_key, check_name, severity, observed, expected
        FROM meta.dq_results
        WHERE run_id = (SELECT run_id FROM meta.runs ORDER BY started_at DESC LIMIT 1)
          AND NOT passed
        ORDER BY severity, step_key
        """
    )
    print("\npublish log")
    read_query(
        f"""
        SELECT run_id, table_name, business_date, rows_deleted, rows_written,
               nullif(previous_run_id, '') AS replaced_run, compensated
        FROM meta.publish_log {where}
        ORDER BY published_at DESC LIMIT {limit * 2}
        """
    )


async def _print_steps_for_run(run_id: str) -> None:
    print()
    read_query(
        f"""
        SELECT step_key, state, attempts, rows_in, rows_out, rows_rejected,
               round(duration_seconds, 2) AS secs, memory_limit AS mem, threads AS thr
        FROM meta.step_metrics WHERE run_id = '{run_id}'
        ORDER BY layer, step_key
        """
    )


async def cmd_lineage(args: argparse.Namespace) -> int:
    print("\nlineage, reconstructed from meta.lineage")
    read_query(
        """
        SELECT DISTINCT upstream, '->' AS dir, downstream, step_key
        FROM meta.lineage
        WHERE run_id = (SELECT run_id FROM meta.runs ORDER BY started_at DESC LIMIT 1)
        ORDER BY step_key, upstream
        """
    )
    return 0


async def cmd_query(args: argparse.Namespace) -> int:
    read_query(args.sql, args.limit)
    return 0


async def cmd_warehouse(args: argparse.Namespace) -> int:
    print(f"\n{settings().warehouse_path}")
    for table, rows in warehouse.summary().items():
        print(f"  {table:34} {rows:>10,} rows")
    return 0


async def cmd_gold(args: argparse.Namespace) -> int:
    print("\ndaily consumption by region")
    read_query(
        """
        SELECT business_date, region, meters, kwh, cost_eur, peak_share_pct,
               kwh_per_meter, kwh_7d_avg, rank_in_day
        FROM gold.daily_region_consumption
        ORDER BY business_date DESC, rank_in_day
        LIMIT 20
        """
    )
    print("\nmeters flagged as anomalous")
    read_query(
        """
        SELECT business_date, region, meter_id, capacity_kw, kwh, kwh_per_kw,
               z_score, anomaly_band
        FROM gold.meter_anomalies
        WHERE anomaly_band <> 'normal'
        ORDER BY abs(z_score) DESC
        LIMIT 20
        """
    )
    return 0


async def cmd_lake(args: argparse.Namespace) -> int:
    """What the object store actually holds, by layer and dataset.

    Reads the lake directly rather than the warehouse, because the two can
    disagree -- and when they do, that gap is the interesting part: every run
    that ever failed left its Parquet behind, and only the published ones are in
    the warehouse.
    """
    cfg = settings()
    with session.connect() as con:
        rows = con.execute(
            f"""
            SELECT split_part(replace(file, '{cfg.lake_uri.rstrip('/')}/', ''), '/', 1) AS layer,
                   split_part(replace(file, '{cfg.lake_uri.rstrip('/')}/', ''), '/', 2) AS dataset,
                   count(*)                                   AS files,
                   count(DISTINCT nullif(regexp_extract(file, 'run=([^/]+)', 1), ''))
                                                              AS runs
            FROM glob('{cfg.lake_uri.rstrip('/')}/**') t(file)
            GROUP BY ALL
            ORDER BY 1, 2
            """
        ).fetchall()
    print(f"\n{cfg.lake_uri}")
    print_table(["layer", "dataset", "files", "runs"], rows)
    return 0


async def cmd_schedule(args: argparse.Namespace) -> int:
    """Create a Temporal Schedule -- the durable replacement for cron.

    It is paused on creation on purpose: a schedule that starts firing the
    moment you create it is how a demo turns into 40 backfill runs.
    """
    c = await client()
    handle = await c.create_schedule(
        "meter-daily",
        Schedule(
            action=ScheduleActionStartWorkflow(
                MeterPipelineWorkflow.run,
                PipelineInput(
                    business_date=args.date,
                    run_id="scheduled",
                    mode=args.mode,
                    require_approval=False,
                ),
                id="meter-scheduled",
                task_queue=queues.COMPUTE,
            ),
            spec=ScheduleSpec(cron_expressions=[args.cron]),
            state=ScheduleState(paused=True, note="created by duckflow CLI"),
        ),
    )
    print(f"schedule created (paused): {handle.id}  cron={args.cron}")
    print("unpause it in the UI, or: temporal schedule unpause --schedule-id meter-daily")
    return 0


async def cmd_contention(args: argparse.Namespace) -> int:
    """Show what DuckDB actually refuses, and what it merely conflicts on.

    Two different failures hide behind "single writer", and they want different
    answers:

      across processes   the file lock refuses the open outright -- and refuses
                         a *read-only* open too, while a writer holds the file.
      within one process connections share an instance and are allowed, so the
                         failure moves to commit time as a transaction conflict
                         on the rows two publishes both tried to replace.

    Only the first is what people mean by "DuckDB is single-writer". The second
    is the one that would actually bite a writer worker with its concurrency
    turned up, and it is the reason that number is 1.
    """
    import os
    import subprocess
    import tempfile

    import duckdb

    # A scratch database, not the real warehouse -- partly so a demo never
    # touches production data, and partly because this command runs in the
    # compute container, which mounts the warehouse volume read-only and
    # therefore *cannot* open it read-write. That restriction is the same
    # architecture this demo is about.
    path = os.path.join(tempfile.gettempdir(), "duckflow-contention.duckdb")
    for leftover in (path, path + ".wal"):
        if os.path.exists(leftover):
            os.remove(leftover)
    duckdb.connect(path).close()

    print(f"\nA. another process, while this one holds {path} read-write")
    held = duckdb.connect(path)
    try:
        for read_only in (False, True):
            probe = subprocess.run(
                [sys.executable, "-c",
                 f"import duckdb; duckdb.connect({path!r}, read_only={read_only})"],
                capture_output=True, text=True,
            )
            label = "read-only" if read_only else "read-write"
            if probe.returncode == 0:
                print(f"  {label:11} open: OK")
            else:
                detail = (probe.stderr.strip().splitlines() or ["?"])[-1]
                print(f"  {label:11} open: REFUSED -- {detail[:90]}")
    finally:
        held.close()

    print("\nB. two connections inside one process, replacing the same date")
    a, b = duckdb.connect(path), duckdb.connect(path)
    try:
        a.execute("CREATE TABLE IF NOT EXISTS contention (business_date DATE, v INTEGER)")
        a.execute("DELETE FROM contention")
        a.execute("INSERT INTO contention VALUES (DATE '2026-08-01', 1)")
        a.execute("BEGIN")
        b.execute("BEGIN")
        a.execute("DELETE FROM contention WHERE business_date = DATE '2026-08-01'")
        print("  connection A: DELETE ok (uncommitted)")
        try:
            b.execute("DELETE FROM contention WHERE business_date = DATE '2026-08-01'")
            b.execute("COMMIT")
            print("  connection B: committed -- no conflict")
        except duckdb.Error as exc:
            print(f"  connection B: CONFLICT -- {str(exc).splitlines()[0]}")
        a.execute("ROLLBACK")
    finally:
        a.close()
        b.close()

    print(
        "\nThis is why WRITER runs one worker with max_concurrent_activities=1,\n"
        "and why every read goes through open_read_only(), which backs off and\n"
        "retries rather than assuming the file is free. Temporal cannot make\n"
        "DuckDB accept two writers; it makes the queue in front of the one\n"
        "writer durable, observable and backpressured instead."
    )
    return 0


# --------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("duckflow", description="Temporal + DuckDB meter pipeline")
    sub = p.add_subparsers(dest="command", required=True)

    r = sub.add_parser("run", help="run the pipeline for one business date")
    r.add_argument("--date", default="2026-08-01")
    r.add_argument("--mode", default=Mode.AUTO.value, choices=[m.value for m in Mode])
    r.add_argument("--meters", type=int, default=20000)
    r.add_argument("--threshold-mb", type=int, default=16,
                   help="AUTO: inputs at or above this get the roomy profile")
    r.add_argument("--id", default="")
    r.add_argument("--no-approval", action="store_true")
    r.add_argument("--approval-timeout", type=int, default=900,
                   help="0 waits forever")
    r.add_argument("--no-seed", action="store_true", help="reuse the existing landing zone")
    r.add_argument("--no-publish", action="store_true")
    r.add_argument("--fail-step", default="", help="make this step raise (chaos)")
    r.add_argument("--fail-after-publish", action="store_true",
                   help="fail once the publish has committed, so compensation "
                        "actually has something to restore")
    r.add_argument("--inject", default="", help="bad_tariff | missing_meters | voltage_storm")
    r.add_argument("--wait", action="store_true")

    b = sub.add_parser("backfill", help="run a date range as child workflows")
    b.add_argument("--start", required=True)
    b.add_argument("--end", required=True)
    b.add_argument("--mode", default=Mode.AUTO.value, choices=[m.value for m in Mode])
    b.add_argument("--meters", type=int, default=5000)
    b.add_argument("--parallel", type=int, default=2)
    b.add_argument("--checkpoint-every", type=int, default=10)
    b.add_argument("--wait", action="store_true")

    for name, helptext in (
        ("approve", "release the publish gate"),
        ("pause", "pause at the next phase boundary"),
        ("resume", "resume a paused run"),
        ("abort", "abort at the next checkpoint"),
    ):
        s = sub.add_parser(name, help=helptext)
        s.add_argument("workflow_id")
        s.add_argument("--actor", default="")
        s.add_argument("--reason", default="operator abort")

    e = sub.add_parser("extend-sla", help="give a running step more time (workflow update)")
    e.add_argument("workflow_id")
    e.add_argument("step")
    e.add_argument("seconds", type=int)

    w = sub.add_parser("watch", help="live progress via workflow query")
    w.add_argument("workflow_id")
    w.add_argument("--follow", action="store_true")
    w.add_argument("--interval", type=float, default=3.0)

    sub.add_parser("steps", help="print the declared DAG, SLAs and checks")

    rep = sub.add_parser("report", help="observability report from meta.*")
    rep.add_argument("--date", default=None)
    rep.add_argument("--limit", type=int, default=10)

    sub.add_parser("lineage", help="lineage graph of the most recent run")
    sub.add_parser("lake", help="what the object store holds, by layer and dataset")
    sub.add_parser("warehouse", help="row counts of every warehouse table")
    sub.add_parser("gold", help="peek at the gold tables")

    q = sub.add_parser("query", help="read-only SQL against the warehouse")
    q.add_argument("sql")
    q.add_argument("--limit", type=int, default=200)

    sc = sub.add_parser("schedule", help="create a paused daily Temporal Schedule")
    sc.add_argument("--cron", default="0 2 * * *")
    sc.add_argument("--date", default="2026-08-01")
    sc.add_argument("--mode", default=Mode.AUTO.value)

    ct = sub.add_parser("contention", help="show DuckDB refusing a second writer")
    ct.add_argument("--n", type=int, default=3)

    return p


HANDLERS = {
    "run": cmd_run,
    "backfill": cmd_backfill,
    "approve": cmd_signal,
    "pause": cmd_signal,
    "resume": cmd_signal,
    "abort": cmd_signal,
    "extend-sla": cmd_extend_sla,
    "watch": cmd_watch,
    "steps": cmd_steps,
    "report": cmd_report,
    "lineage": cmd_lineage,
    "lake": cmd_lake,
    "warehouse": cmd_warehouse,
    "gold": cmd_gold,
    "query": cmd_query,
    "schedule": cmd_schedule,
    "contention": cmd_contention,
}


def main() -> int:
    logs.configure("cli")
    args = build_parser().parse_args()
    return asyncio.run(HANDLERS[args.command](args))


if __name__ == "__main__":
    sys.exit(main())
