"""Test fixtures.

Every test runs against a *local filesystem* lake and a temporary warehouse
file, so the suite needs no MinIO, no Temporal server and no Docker. That is
deliberate: a test suite you can only run with `make up` is a test suite nobody
runs while editing SQL.
"""

from __future__ import annotations

import os
import pathlib

import pytest


@pytest.fixture()
def lake(tmp_path: pathlib.Path) -> str:
    root = tmp_path / "lake"
    root.mkdir()
    os.environ["LAKE_URI"] = str(root)
    return str(root)


@pytest.fixture()
def warehouse_path(tmp_path: pathlib.Path) -> str:
    path = str(tmp_path / "warehouse.duckdb")
    os.environ["WAREHOUSE_PATH"] = path
    os.environ["DUCKDB_TEMP_DIR"] = str(tmp_path / "duck-tmp")
    return path


@pytest.fixture(autouse=True)
def _fresh_settings(tmp_path: pathlib.Path):
    """`settings()` is lru_cached, which is right in a worker and wrong in a
    test that just changed the environment."""
    os.environ.setdefault("DUCKDB_EXTENSION_DIR", str(pathlib.Path.home() / ".duckdb" / "extensions"))
    from duckflow.config import settings

    settings.cache_clear()
    yield
    settings.cache_clear()
