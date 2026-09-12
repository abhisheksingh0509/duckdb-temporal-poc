# One image, two roles, 411 MB -- most of which is the DuckDB wheel and the
# Temporal SDK's Rust core, not a runtime you have to operate.
#
# Contrast with the Spark equivalent, where the orchestrator either carries a
# JVM or lives in a second image built and versioned separately. Here the thing
# that runs the workflows and the thing that runs the SQL are the same image, so
# `--scale worker-compute=8` is a sentence rather than a project, and upgrading
# the engine is a line in requirements.txt.

FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/opt/duckflow/src \
    DUCKDB_EXTENSION_DIR=/opt/duckdb-extensions

RUN apt-get update \
 && apt-get install -y --no-install-recommends tini curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Bake httpfs in at build time. An activity that has to download an extension on
# first use is an activity whose first attempt fails in an air-gapped network
# and times out in a slow one -- and it would do it once per fresh container.
RUN mkdir -p /opt/duckdb-extensions \
 && python -c "import duckdb; c = duckdb.connect(); \
c.execute(\"SET extension_directory = '/opt/duckdb-extensions'\"); \
c.execute('INSTALL httpfs'); c.execute('LOAD httpfs'); print('httpfs baked in')"

WORKDIR /opt/duckflow
COPY src /opt/duckflow/src
COPY tests /opt/duckflow/tests
COPY pytest.ini /opt/duckflow/pytest.ini

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "duckflow.worker", "compute"]
