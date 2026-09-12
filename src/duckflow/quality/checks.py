"""Data quality gate.

Every check is one SQL statement against the Parquet the step just wrote. That
choice is deliberate: checking the *artifact* rather than an in-memory relation
also catches a partial write, a wrong output path, and a schema that changed
under a `SELECT *`. It costs a re-read, which for columnar files touching two
columns is close to free.

Severity is the whole contract with the workflow:

    ERROR  the run stops and compensates. Raised as a non-retryable failure,
           because retrying a computation on the same bad input produces the
           same bad output and burns three attempts finding that out.
    WARN   recorded, surfaced in the report, run continues.
"""

from __future__ import annotations

import logging

import duckdb

from duckflow.duck import session
from duckflow.shared import DQCheck, DQReport, DuckSettings, Severity
from duckflow.steps import DATASETS, STEPS, Expectation

log = logging.getLogger("duckflow.quality")

DQ_DUCK = DuckSettings(threads=2, memory_limit="1GB")


def _rel(uri: str, dataset_name: str) -> str:
    fmt = DATASETS[dataset_name].fmt if dataset_name in DATASETS else "parquet"
    if fmt == "csv":
        return f"read_csv('{uri}', header = true)"
    return f"read_parquet('{uri}')"


def _scalar(con: duckdb.DuckDBPyConnection, sql: str):
    row = con.execute(sql).fetchone()
    return row[0] if row else None


def _evaluate_one(
    con: duckdb.DuckDBPyConnection, exp: Expectation, uris: dict[str, str]
) -> DQCheck:
    check = DQCheck(
        name=exp.label(),
        kind=exp.kind,
        dataset=exp.dataset,
        column=exp.column,
        severity=exp.severity,
    )
    uri = uris.get(exp.dataset, "")
    if not uri:
        check.passed = False
        check.detail = f"no URI known for dataset {exp.dataset!r}"
        return check

    rel = _rel(uri, exp.dataset)
    p = exp.params

    try:
        if exp.kind == "row_count_min":
            n = _scalar(con, f"SELECT count(*) FROM {rel}")
            check.observed, check.expected = str(n), f">= {p['min']}"
            check.passed = n >= p["min"]

        elif exp.kind == "row_count_max":
            n = _scalar(con, f"SELECT count(*) FROM {rel}")
            check.observed, check.expected = str(n), f"<= {p['max']}"
            check.passed = n <= p["max"]

        elif exp.kind == "not_null":
            n = _scalar(con, f"SELECT count(*) FROM {rel} WHERE {exp.column} IS NULL")
            check.observed, check.expected = f"{n} nulls", "0 nulls"
            check.passed = n == 0

        elif exp.kind == "unique":
            cols = p.get("columns") or [exp.column]
            key = ", ".join(cols)
            n = _scalar(
                con,
                f"SELECT count(*) FROM (SELECT {key} FROM {rel} "
                f"GROUP BY {key} HAVING count(*) > 1)",
            )
            check.observed, check.expected = f"{n} duplicate keys", f"0 on ({key})"
            check.passed = n == 0

        elif exp.kind == "accepted_values":
            vals = ", ".join(f"'{v}'" for v in p["values"])
            n = _scalar(
                con,
                f"SELECT count(*) FROM {rel} "
                f"WHERE {exp.column} IS NOT NULL AND {exp.column} NOT IN ({vals})",
            )
            check.observed, check.expected = f"{n} outside set", f"in ({vals})"
            check.passed = n == 0

        elif exp.kind == "between":
            n = _scalar(
                con,
                f"SELECT count(*) FROM {rel} WHERE {exp.column} IS NOT NULL "
                f"AND {exp.column} NOT BETWEEN {p['min']} AND {p['max']}",
            )
            check.observed = f"{n} out of range"
            check.expected = f"{exp.column} in [{p['min']}, {p['max']}]"
            check.passed = n == 0

        elif exp.kind == "reject_rate_max":
            parent = p["against"]
            prel = _rel(uris[parent], parent)
            rejected = _scalar(con, f"SELECT count(*) FROM {rel}") or 0
            total = _scalar(con, f"SELECT count(*) FROM {prel}") or 0
            rate = (rejected / total) if total else 0.0
            check.observed = f"{rate:.4f} ({rejected}/{total})"
            check.expected = f"<= {p['max']}"
            check.passed = rate <= p["max"]

        elif exp.kind == "referential":
            parent = p["parent"]
            prel = _rel(uris[parent], parent)
            n = _scalar(
                con,
                f"""
                SELECT count(*) FROM {rel} c
                ANTI JOIN {prel} pa ON c.{exp.column} = pa.{p['parent_column']}
                """,
            )
            check.observed = f"{n} orphans"
            check.expected = f"every {exp.column} in {parent}"
            check.passed = n == 0

        elif exp.kind == "sum_close_to":
            parent = p["against"]
            prel = _rel(uris[parent], parent)
            a = _scalar(con, f"SELECT coalesce(sum({p['column']}), 0) FROM {rel}") or 0
            b = _scalar(
                con, f"SELECT coalesce(sum({p['against_column']}), 0) FROM {prel}"
            ) or 0
            denom = abs(b) if b else 1.0
            drift = abs(a - b) / denom
            check.observed = f"{a:.4f} vs {b:.4f} (drift {drift:.6f})"
            check.expected = f"drift <= {p['tolerance']}"
            check.passed = drift <= p["tolerance"]

        else:
            check.passed = False
            check.detail = f"unimplemented check kind {exp.kind!r}"

    except (duckdb.Error, KeyError) as exc:
        # A check that cannot run is a failed check. Treating it as a pass is
        # how a gate quietly stops gating.
        check.passed = False
        check.detail = f"{type(exc).__name__}: {exc}"

    return check


def evaluate(step_key: str, uris: dict[str, str]) -> DQReport:
    report = DQReport(step_key=step_key)
    expectations = STEPS[step_key].expectations
    if not expectations:
        return report
    with session.connect(DQ_DUCK) as con:
        for exp in expectations:
            check = _evaluate_one(con, exp, uris)
            report.checks.append(check)
            if not check.passed:
                log.warning(
                    "DQ %s %s: observed=%s expected=%s %s",
                    check.severity, check.name, check.observed, check.expected, check.detail,
                )
    return report


def summarise(report: DQReport) -> str:
    failed = [c for c in report.checks if not c.passed]
    errors = [c for c in failed if c.severity == Severity.ERROR.value]
    return (
        f"{len(report.checks)} checks, {len(failed)} failed "
        f"({len(errors)} error, {len(failed) - len(errors)} warn)"
    )
