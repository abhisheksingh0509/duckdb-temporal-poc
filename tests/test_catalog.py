"""The catalog is data, so it can be checked without running anything."""

from __future__ import annotations

import re

import pytest

from duckflow import steps as S


def test_catalog_is_structurally_valid():
    assert S.validate_catalog() == []


def test_every_step_appears_in_exactly_one_phase():
    seen = [k for _, keys in S.PHASES for k in keys]
    assert sorted(seen) == sorted(S.STEPS)
    assert len(seen) == len(set(seen))


def test_dependencies_are_satisfied_by_an_earlier_phase():
    produced: set[str] = set()
    for _, keys in S.PHASES:
        for key in keys:
            for dep in S.STEPS[key].depends_on:
                assert dep in produced or dep in S.STEPS, dep
        produced.update(keys)


def test_run_scoped_uris_are_unique_per_run():
    ds = S.dataset(S.GOLD_DAILY_REGION)
    a = ds.uri("s3://lake", "2026-08-01", "run-a")
    b = ds.uri("s3://lake", "2026-08-01", "run-b")
    assert a != b
    assert a.endswith("/dt=2026-08-01/run=run-a/data.parquet")


def test_landing_uris_do_not_carry_a_run_id():
    # Landing is regenerated in place; only derived layers are run-scoped, which
    # is what lets compensation point back at a previous run's files.
    uri = S.dataset(S.LANDING_READINGS).uri("s3://lake", "2026-08-01", "run-a")
    assert "run=" not in uri


@pytest.mark.parametrize("key", sorted(S.STEPS))
def test_rendered_sql_has_no_unresolved_placeholders(key):
    pre, selects = S.STEPS[key].render(S.render_context("2026-08-01", "run-1"))
    for sql in [*pre, *selects.values()]:
        assert not re.search(r"\$\{\w+\}", sql), sql


@pytest.mark.parametrize("key", sorted(S.STEPS))
def test_pre_statements_never_mutate(key):
    """`pre` builds views. A DML statement there would write outside the COPY,
    which is how a step stops being re-runnable."""
    for sql in S.STEPS[key].pre:
        head = sql.strip().split(None, 1)[0].upper()
        assert head in {"CREATE", "SET", "PRAGMA"}, f"{key}: {head}"


def test_published_datasets_are_gold():
    for name in S.PUBLISHED:
        assert S.dataset(name).layer == "gold"
