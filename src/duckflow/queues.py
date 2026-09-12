"""Task queue names.

In the Spark/Polars world a task queue picks an *engine*. Here it picks a
*concurrency contract*, which is the whole point of this POC:

  COMPUTE  Many workers, many concurrent activities. DuckDB runs in-memory,
           reads Parquet from object storage and writes Parquet back. Nothing
           is shared, so nothing can conflict. Scale this horizontally.

  WRITER   Exactly one worker, `max_concurrent_activities=1`. It is the sole
           process allowed to open `warehouse.duckdb` read-write, because
           DuckDB permits exactly one read-write process per database file.

The single-writer rule is a property of the storage engine. Temporal cannot
change it -- but a task queue with one slot is a clean, durable, observable way
to *honour* it, and that is the argument this repo is making.

Both the workflow (inside the sandbox, which must not read the environment) and
the workers need these strings, so they live in a module of their own.
"""

from __future__ import annotations

#: Stateless DuckDB. Workflows are hosted here too.
COMPUTE = "duck-compute-tq"

#: Serialised owner of warehouse.duckdb. One worker, one slot.
WRITER = "duck-writer-tq"


def for_role(role: str) -> str:
    if role == "compute":
        return COMPUTE
    if role == "writer":
        return WRITER
    raise ValueError(f"no task queue for role {role!r}")
