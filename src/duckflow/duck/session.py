"""DuckDB connection management.

Three things live here and nothing else does:

1. **Configuration order.** `extension_directory` must be set before `LOAD`,
   the S3 secret must exist before any `s3://` read, and `memory_limit` must be
   set before the first query allocates. Getting this order wrong produces
   errors that look like network problems, so it is written down once.

2. **One connection per activity.** Not one per worker. A DuckDB connection is
   cheap (single-digit milliseconds for `:memory:`), and an activity that owns
   its connection can be cancelled by interrupting it without touching anything
   else the worker is doing. For the warehouse file it matters even more: while
   no activity is running, no process holds the file, so `make sql` works.

3. **Interruptibility.** `con.interrupt()` is the only way to stop a running
   DuckDB query, and it must be called from a different thread than the one
   blocked in `execute()`. That is exactly the shape of a Temporal activity that
   heartbeats from the event loop while the query runs in a worker thread.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Iterator

import duckdb

from duckflow.config import settings
from duckflow.shared import DuckSettings

log = logging.getLogger("duckflow.duck")

#: Extensions the pipeline needs. Baked into the image at build time; `LOAD`
#: here is a local file read, never a download.
EXTENSIONS = ("httpfs",)


def _apply(con: duckdb.DuckDBPyConnection, duck: DuckSettings) -> None:
    cfg = settings()
    con.execute(f"SET extension_directory = '{cfg.duckdb_extension_dir}'")
    con.execute(f"SET threads = {max(1, duck.threads)}")
    con.execute(f"SET memory_limit = '{duck.memory_limit}'")
    con.execute("SET enable_progress_bar = false")
    # Streaming the Parquet write instead of buffering the whole result to keep
    # row order. The pipeline never depends on file row order; every consumer
    # sorts or aggregates. Turning this off is what lets a LEAN run with a
    # 512 MB limit finish at all.
    con.execute(
        f"SET preserve_insertion_order = {'true' if duck.preserve_insertion_order else 'false'}"
    )
    if duck.temp_directory and cfg.duckdb_temp_dir:
        try:
            os.makedirs(cfg.duckdb_temp_dir, exist_ok=True)
        except OSError as exc:
            # A read-only mount, typically. Better to run without spill and let
            # a large query fail loudly on memory_limit than to fail every
            # connection -- including the read-only ones that just want to
            # answer `make report`.
            log.warning("temp_directory %s unusable (%s); running without spill",
                        cfg.duckdb_temp_dir, exc)
            return
        con.execute(f"SET temp_directory = '{cfg.duckdb_temp_dir}'")
        # Without a max, a runaway spill fills the volume and takes the writer
        # down with it. With one, the query fails and Temporal retries it.
        con.execute("SET max_temp_directory_size = '8GB'")


def _load_extensions(con: duckdb.DuckDBPyConnection) -> None:
    for ext in EXTENSIONS:
        try:
            con.execute(f"LOAD {ext}")
        except duckdb.Error:
            # Only reachable on a dev machine where the image was not used.
            log.warning("LOAD %s failed from the extension dir, installing", ext)
            con.execute(f"INSTALL {ext}")
            con.execute(f"LOAD {ext}")


def _create_secret(con: duckdb.DuckDBPyConnection) -> None:
    cfg = settings()
    # A SECRET rather than the legacy `SET s3_access_key_id`: secrets are scoped,
    # are not readable back out of the connection, and are the only form that
    # supports per-prefix credentials when the lake grows a second bucket.
    con.execute(
        f"""
        CREATE OR REPLACE SECRET lake (
            TYPE s3,
            KEY_ID '{cfg.s3_access_key}',
            SECRET '{cfg.s3_secret_key}',
            REGION '{cfg.s3_region}',
            ENDPOINT '{cfg.s3_endpoint}',
            URL_STYLE 'path',
            USE_SSL {'true' if cfg.s3_use_ssl else 'false'}
        )
        """
    )


@contextmanager
def connect(
    duck: DuckSettings | None = None,
    *,
    database: str = ":memory:",
    read_only: bool = False,
    s3: bool = True,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open a configured connection and always close it.

    `database=":memory:"` is the compute path: nothing is persisted, so any
    number of these can run concurrently on any number of workers. A file path
    is the writer path, and only the writer task queue may use it read-write.
    """
    duck = duck or DuckSettings(
        threads=settings().default_threads,
        memory_limit=settings().default_memory_limit,
    )
    con = duckdb.connect(database=database, read_only=read_only)
    try:
        _apply(con, duck)
        _load_extensions(con)
        if s3:
            _create_secret(con)
        yield con
    finally:
        con.close()


def open_read_only(path: str, attempts: int = 10, delay: float = 0.5):
    """Open the warehouse for reading, retrying past the writer.

    DuckDB allows many read-only processes *or* one read-write process, never
    both. The writer holds the file only for the duration of one activity, so a
    reader that backs off briefly almost always gets in. This is the honest cost
    of an embedded engine, and it is why the CLI reads are all short.
    """
    last: Exception | None = None
    for i in range(attempts):
        try:
            con = duckdb.connect(database=path, read_only=True)
            _apply(con, DuckSettings(threads=2, memory_limit="512MB", temp_directory=False))
            _load_extensions(con)
            _create_secret(con)
            return con
        except duckdb.Error as exc:  # file is locked by the writer
            last = exc
            time.sleep(delay * (i + 1))
    raise RuntimeError(
        f"warehouse at {path} stayed locked for "
        f"{sum(delay * (i + 1) for i in range(attempts)):.1f}s: {last}"
    )


def register_source(con: duckdb.DuckDBPyConnection, name: str, uri: str, fmt: str) -> None:
    """Expose an object-store path as a plain relation name.

    Step SQL therefore reads `FROM bronze_readings`, not
    `FROM read_parquet('s3://lake/bronze/...')`. The SQL in steps.py stays about
    the data, and the URI -- which encodes the run id -- stays in the activity
    where it can be logged and recorded.
    """
    if fmt == "csv":
        reader = f"read_csv('{uri}', header = true, sample_size = -1)"
    else:
        reader = f"read_parquet('{uri}', union_by_name = true)"
    con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM {reader}")


def _ensure_local_parent(uri: str) -> None:
    """Object stores have no directories; local filesystems do.

    The lake is normally `s3://...`, where a PUT to a deep key just works. But
    pointing LAKE_URI at a local path is how you run this repo without Docker --
    and DuckDB will not create the intermediate directories for a plain
    `COPY ... TO 'a/b/c.parquet'`. One line here keeps both paths working.
    """
    if "://" in uri:
        return
    os.makedirs(os.path.dirname(uri) or ".", exist_ok=True)


def ensure_writable(uri: str) -> str:
    """Public form of the same courtesy, for callers that COPY their own SQL."""
    _ensure_local_parent(uri)
    return uri


def copy_to_parquet(con: duckdb.DuckDBPyConnection, select_sql: str, uri: str) -> int:
    """Write one relation to one Parquet file and return its row count.

    ZSTD because these files are read far more often than they are written, and
    the row-group size is pinned so that a downstream reader's parallelism does
    not depend on how much memory the writer happened to have.
    """
    _ensure_local_parent(uri)
    con.execute(
        f"""
        COPY ({select_sql})
        TO '{uri}'
        (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880)
        """
    )
    row = con.execute(f"SELECT count(*) FROM read_parquet('{uri}')").fetchone()
    return int(row[0]) if row else 0


def parquet_bytes(con: duckdb.DuckDBPyConnection, uri: str) -> int:
    """Compressed size, read from the Parquet footers -- no data pages touched."""
    try:
        row = con.execute(
            f"SELECT coalesce(sum(total_compressed_size), 0) FROM parquet_metadata('{uri}')"
        ).fetchone()
        return int(row[0]) if row else 0
    except duckdb.Error:
        return 0


def count_rows(con: duckdb.DuckDBPyConnection, uri: str, fmt: str = "parquet") -> int:
    if fmt == "csv":
        row = con.execute(f"SELECT count(*) FROM read_csv('{uri}', header = true)").fetchone()
    else:
        row = con.execute(f"SELECT count(*) FROM read_parquet('{uri}')").fetchone()
    return int(row[0]) if row else 0
