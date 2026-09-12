"""Declarative catalog of datasets and steps.

One place defines, for every step: what it reads, what it writes, the SQL it
runs, its SLA, its quality expectations and its lineage. The activity that
executes a step contains no business logic at all -- it opens a DuckDB
connection, registers each input as a view, runs the step's SQL, and copies each
output to object storage.

That split is what makes the pipeline legible: `git diff` on this file is the
whole change to the data, and the Temporal side never has to be re-read.

Stdlib-only. This module is imported inside the Temporal workflow sandbox, so it
must not touch the environment, the clock, the filesystem or the network.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from string import Template
from typing import Any

from duckflow.shared import Severity

# --------------------------------------------------------------------------
# Layout
# --------------------------------------------------------------------------
#
#   <lake>/landing/<name>/[dt=<date>/]<file>            raw drop, regenerated
#   <lake>/<layer>/<name>/dt=<date>/run=<run_id>/data.parquet
#
# Every step output is written to a *run-scoped* path and is never mutated.
# Publishing is therefore a pointer move, and rolling back is pointing at the
# previous run's files again -- which is the only form of time travel you get
# when the storage layer is plain Parquet.

LANDING_READINGS = "landing_readings"
LANDING_METERS = "landing_meters"
LANDING_TARIFFS = "landing_tariffs"
LANDING_WEATHER = "landing_weather"

BRONZE_READINGS = "bronze_readings"
BRONZE_METERS = "bronze_meters"
BRONZE_TARIFFS = "bronze_tariffs"
BRONZE_WEATHER = "bronze_weather"

SILVER_READINGS = "silver_readings"
SILVER_QUARANTINE = "silver_quarantine"
SILVER_ENRICHED = "silver_enriched"

GOLD_DAILY_REGION = "gold_daily_region"
GOLD_METER_ANOMALIES = "gold_meter_anomalies"


@dataclass(frozen=True)
class Dataset:
    name: str
    layer: str
    fmt: str = "parquet"
    partitioned_by_date: bool = True
    landing_file: str = ""
    """Set for landing datasets; run-scoped datasets derive their own filename."""

    def uri(self, lake: str, business_date: str, run_id: str = "") -> str:
        lake = lake.rstrip("/")
        if self.layer == "landing":
            if self.partitioned_by_date:
                return f"{lake}/landing/{self.name}/dt={business_date}/{self.landing_file}"
            return f"{lake}/landing/{self.name}/{self.landing_file}"
        return f"{lake}/{self.layer}/{self.name}/dt={business_date}/run={run_id}/data.parquet"

    def published_glob(self, lake: str, business_date: str) -> str:
        """Every run that has ever written this date. Useful for `make history`;
        never used by the pipeline, which always addresses one run."""
        lake = lake.rstrip("/")
        return f"{lake}/{self.layer}/{self.name}/dt={business_date}/run=*/data.parquet"


DATASETS: dict[str, Dataset] = {
    d.name: d
    for d in (
        Dataset(LANDING_READINGS, "landing", "parquet", True, "readings.parquet"),
        Dataset(LANDING_METERS, "landing", "csv", False, "meters.csv"),
        Dataset(LANDING_TARIFFS, "landing", "csv", False, "tariffs.csv"),
        Dataset(LANDING_WEATHER, "landing", "parquet", True, "weather.parquet"),
        Dataset(BRONZE_READINGS, "bronze"),
        Dataset(BRONZE_METERS, "bronze"),
        Dataset(BRONZE_TARIFFS, "bronze"),
        Dataset(BRONZE_WEATHER, "bronze"),
        Dataset(SILVER_READINGS, "silver"),
        Dataset(SILVER_QUARANTINE, "silver"),
        Dataset(SILVER_ENRICHED, "silver"),
        Dataset(GOLD_DAILY_REGION, "gold"),
        Dataset(GOLD_METER_ANOMALIES, "gold"),
    )
}

#: Gold tables that get merged into warehouse.duckdb by the single writer.
PUBLISHED = {
    GOLD_DAILY_REGION: "gold.daily_region_consumption",
    GOLD_METER_ANOMALIES: "gold.meter_anomalies",
}


# --------------------------------------------------------------------------
# Expectations
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Expectation:
    """One assertion, evaluated by the generic DQ activity against the Parquet
    that the step actually wrote -- not against an in-memory relation. Checking
    the artifact rather than the computation is the only way the gate also
    catches a bad write."""

    kind: str
    dataset: str
    column: str = ""
    params: dict[str, Any] = field(default_factory=dict)
    severity: str = Severity.ERROR.value
    name: str = ""

    def label(self) -> str:
        if self.name:
            return self.name
        col = f".{self.column}" if self.column else ""
        return f"{self.kind}:{self.dataset}{col}"


# Supported `kind` values, implemented in quality/checks.py:
#   row_count_min     params: {min: int}
#   row_count_max     params: {max: int}
#   not_null          column required
#   unique            params: {columns: [...]} or column
#   accepted_values   params: {values: [...]}
#   between           params: {min: float, max: float}
#   reject_rate_max   params: {max: float, against: "<dataset>"}
#   referential       params: {parent: "<dataset>", parent_column: "col"}
#   sum_close_to      params: {against: "<dataset>", against_column: "col",
#                              column: "col", tolerance: float}


# --------------------------------------------------------------------------
# Steps
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Step:
    key: str
    layer: str
    title: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    depends_on: tuple[str, ...] = ()
    pre: tuple[str, ...] = ()
    """Statements run before the COPYs -- views, SET, PRAGMA. Never DML."""
    selects: dict[str, str] = field(default_factory=dict)
    """output dataset name -> the SELECT whose result is copied to Parquet."""
    sla_seconds: int = 60
    expectations: tuple[Expectation, ...] = ()
    lineage: dict[str, tuple[str, ...]] = field(default_factory=dict)
    notes: str = ""

    def render(self, ctx: dict[str, str]) -> tuple[list[str], dict[str, str]]:
        """Substitute ${placeholders}. Template, not str.format, because DuckDB
        SQL uses braces for struct and map literals."""
        pre = [Template(s).safe_substitute(ctx) for s in self.pre]
        sel = {k: Template(v).safe_substitute(ctx) for k, v in self.selects.items()}
        return pre, sel


# -- bronze ----------------------------------------------------------------
# Four independent reads, fanned out by the workflow. Deliberately trivial SQL:
# bronze exists to fix types and names once, so that every later step can assume
# them. Anything cleverer here is a silver concern.

_BRONZE_READINGS = Step(
    key=BRONZE_READINGS,
    layer="bronze",
    title="Land raw meter readings",
    inputs=(LANDING_READINGS,),
    outputs=(BRONZE_READINGS,),
    selects={
        BRONZE_READINGS: """
            SELECT
                CAST(meter_id       AS VARCHAR)   AS meter_id,
                CAST(ts             AS TIMESTAMP) AS reading_ts,
                CAST(cumulative_kwh AS DOUBLE)    AS cumulative_kwh,
                CAST(voltage        AS DOUBLE)    AS voltage,
                CAST(quality_flag   AS VARCHAR)   AS quality_flag,
                CAST(src_updated_at AS TIMESTAMP) AS src_updated_at,
                DATE '${business_date}'           AS business_date
            FROM landing_readings
        """
    },
    sla_seconds=45,
    expectations=(
        Expectation("row_count_min", BRONZE_READINGS, params={"min": 1000}),
        Expectation("not_null", BRONZE_READINGS, "meter_id"),
        Expectation("not_null", BRONZE_READINGS, "reading_ts"),
    ),
    lineage={BRONZE_READINGS: (LANDING_READINGS,)},
)

_BRONZE_METERS = Step(
    key=BRONZE_METERS,
    layer="bronze",
    title="Land the meter dimension (CSV)",
    inputs=(LANDING_METERS,),
    outputs=(BRONZE_METERS,),
    selects={
        BRONZE_METERS: """
            SELECT
                CAST(meter_id AS VARCHAR)     AS meter_id,
                lower(region)                 AS region,
                lower(tariff_plan)            AS tariff_plan,
                CAST(install_date AS DATE)    AS install_date,
                CAST(capacity_kw AS DOUBLE)   AS capacity_kw,
                lower(customer_segment)       AS customer_segment
            FROM landing_meters
        """
    },
    sla_seconds=30,
    expectations=(
        Expectation("unique", BRONZE_METERS, "meter_id"),
        Expectation("accepted_values", BRONZE_METERS, "region",
                    params={"values": ["north", "south", "east", "west", "central"]}),
        Expectation("accepted_values", BRONZE_METERS, "tariff_plan",
                    params={"values": ["flat", "tou", "dynamic"]}),
    ),
    lineage={BRONZE_METERS: (LANDING_METERS,)},
)

_BRONZE_TARIFFS = Step(
    key=BRONZE_TARIFFS,
    layer="bronze",
    title="Land the tariff price intervals (CSV)",
    inputs=(LANDING_TARIFFS,),
    outputs=(BRONZE_TARIFFS,),
    selects={
        BRONZE_TARIFFS: """
            SELECT
                lower(tariff_plan)              AS tariff_plan,
                CAST(valid_from AS TIMESTAMP)   AS valid_from,
                CAST(price_per_kwh AS DOUBLE)   AS price_per_kwh,
                upper(currency)                 AS currency
            FROM landing_tariffs
        """
    },
    sla_seconds=30,
    expectations=(
        Expectation("row_count_min", BRONZE_TARIFFS, params={"min": 3}),
        # The `bad_tariff` injection makes a price negative. This check is the
        # one the chaos demo trips, and it is non-retryable on purpose.
        Expectation("between", BRONZE_TARIFFS, "price_per_kwh",
                    params={"min": 0.0, "max": 5.0}, name="tariff_price_sane"),
        Expectation("unique", BRONZE_TARIFFS,
                    params={"columns": ["tariff_plan", "valid_from"]}),
    ),
    lineage={BRONZE_TARIFFS: (LANDING_TARIFFS,)},
)

_BRONZE_WEATHER = Step(
    key=BRONZE_WEATHER,
    layer="bronze",
    title="Land hourly regional weather",
    inputs=(LANDING_WEATHER,),
    outputs=(BRONZE_WEATHER,),
    selects={
        BRONZE_WEATHER: """
            SELECT
                lower(region)                AS region,
                CAST(ts AS TIMESTAMP)        AS observed_at,
                CAST(temp_c AS DOUBLE)       AS temp_c
            FROM landing_weather
        """
    },
    sla_seconds=30,
    expectations=(
        Expectation("between", BRONZE_WEATHER, "temp_c", params={"min": -50, "max": 60}),
    ),
    lineage={BRONZE_WEATHER: (LANDING_WEATHER,)},
)

# -- silver ----------------------------------------------------------------

_SILVER_CLEANSE = Step(
    key="silver_cleanse",
    layer="silver",
    title="Dedupe, difference the meter counter, quarantine the rest",
    inputs=(BRONZE_READINGS,),
    outputs=(SILVER_READINGS, SILVER_QUARANTINE),
    depends_on=(BRONZE_READINGS,),
    pre=(
        # QUALIFY filters on a window function without a subquery. It is the
        # single most useful thing DuckDB gives you over ANSI SQL for
        # "keep the latest version of each key".
        """
        CREATE OR REPLACE TEMP VIEW deduped AS
        SELECT *
        FROM bronze_readings
        QUALIFY row_number() OVER (
            PARTITION BY meter_id, reading_ts
            ORDER BY src_updated_at DESC
        ) = 1
        """,
        # A meter reports a cumulative counter, so consumption is a difference.
        # Named windows keep the three lag() calls honest about sharing a frame.
        """
        CREATE OR REPLACE TEMP VIEW differenced AS
        SELECT *,
               lag(cumulative_kwh) OVER w                     AS prev_kwh,
               lag(reading_ts)     OVER w                     AS prev_ts,
               cumulative_kwh - lag(cumulative_kwh) OVER w    AS interval_kwh
        FROM deduped
        WINDOW w AS (PARTITION BY meter_id ORDER BY reading_ts)
        """,
        # One CASE decides every rejection, so a row is never quarantined twice
        # and the reason is a column rather than a log line.
        """
        CREATE OR REPLACE TEMP VIEW classified AS
        SELECT *,
               CASE
                   WHEN cumulative_kwh IS NULL          THEN 'null_reading'
                   WHEN prev_kwh IS NULL                THEN 'no_prior_reading'
                   WHEN interval_kwh < 0                THEN 'meter_rollback'
                   WHEN interval_kwh > ${spike_kwh}     THEN 'implausible_spike'
                   WHEN voltage NOT BETWEEN 180 AND 260 THEN 'voltage_out_of_range'
                   ELSE NULL
               END AS reject_reason
        FROM differenced
        """,
    ),
    selects={
        SILVER_READINGS: """
            SELECT meter_id, reading_ts, business_date,
                   cumulative_kwh,
                   round(interval_kwh, 4) AS interval_kwh,
                   voltage, quality_flag, src_updated_at
            FROM classified
            WHERE reject_reason IS NULL
        """,
        SILVER_QUARANTINE: """
            SELECT meter_id, reading_ts, business_date,
                   cumulative_kwh, prev_kwh, prev_ts,
                   round(interval_kwh, 4) AS interval_kwh,
                   voltage, reject_reason
            FROM classified
            WHERE reject_reason IS NOT NULL
        """,
    },
    sla_seconds=90,
    expectations=(
        Expectation("row_count_min", SILVER_READINGS, params={"min": 500}),
        Expectation("unique", SILVER_READINGS, params={"columns": ["meter_id", "reading_ts"]}),
        Expectation("not_null", SILVER_READINGS, "interval_kwh"),
        Expectation("between", SILVER_READINGS, "interval_kwh", params={"min": 0, "max": 500}),
        # Every meter loses its first reading of the day to `no_prior_reading`
        # (there is no prior partition in scope), so the floor on this rate is
        # 1/96. Assignment 12 is about removing that.
        Expectation("reject_rate_max", SILVER_QUARANTINE,
                    params={"max": 0.08, "against": BRONZE_READINGS},
                    severity=Severity.WARN.value, name="quarantine_rate"),
    ),
    lineage={
        SILVER_READINGS: (BRONZE_READINGS,),
        SILVER_QUARANTINE: (BRONZE_READINGS,),
    },
    notes="interval_kwh = cumulative_kwh - lag(cumulative_kwh) within the day",
)

_SILVER_ENRICH = Step(
    key="silver_enrich",
    layer="silver",
    title="ASOF-join the price and the weather in force at each reading",
    inputs=(SILVER_READINGS, BRONZE_METERS, BRONZE_TARIFFS, BRONZE_WEATHER),
    outputs=(SILVER_ENRICHED,),
    depends_on=("silver_cleanse", BRONZE_METERS, BRONZE_TARIFFS, BRONZE_WEATHER),
    pre=(
        """
        CREATE OR REPLACE TEMP VIEW with_meter AS
        SELECT r.*, m.region, m.tariff_plan, m.capacity_kw, m.customer_segment
        FROM silver_readings r
        JOIN bronze_meters m USING (meter_id)
        """,
        # ASOF JOIN: "the most recent tariff row at or before this reading".
        # Written as an equi-join plus a range predicate it is a correlated
        # subquery or a self-join on max(valid_from); DuckDB makes it one
        # sorted merge, and it is the reason this pipeline is worth writing in
        # DuckDB rather than in pandas.
        """
        CREATE OR REPLACE TEMP VIEW priced AS
        SELECT w.*, t.price_per_kwh, t.currency, t.valid_from AS price_valid_from
        FROM with_meter w
        ASOF JOIN bronze_tariffs t
              ON w.tariff_plan = t.tariff_plan
             AND w.reading_ts >= t.valid_from
        """,
        # LEFT, because a missing weather observation must not silently delete
        # consumption rows. An inner ASOF JOIN here is a classic silent
        # data-loss bug.
        """
        CREATE OR REPLACE TEMP VIEW with_weather AS
        SELECT p.*, x.temp_c, x.observed_at AS weather_observed_at
        FROM priced p
        ASOF LEFT JOIN bronze_weather x
              ON p.region = x.region
             AND p.reading_ts >= x.observed_at
        """,
    ),
    selects={
        SILVER_ENRICHED: """
            SELECT * EXCLUDE (price_valid_from, weather_observed_at, src_updated_at),
                   round(interval_kwh * price_per_kwh, 5) AS cost_eur,
                   date_trunc('hour', reading_ts)         AS reading_hour,
                   hour(reading_ts) BETWEEN 17 AND 20     AS is_peak
            FROM with_weather
        """
    },
    sla_seconds=120,
    expectations=(
        Expectation("not_null", SILVER_ENRICHED, "price_per_kwh"),
        Expectation("not_null", SILVER_ENRICHED, "region"),
        Expectation("not_null", SILVER_ENRICHED, "cost_eur"),
        # An ASOF JOIN cannot add rows, and the LEFT one cannot drop any. If the
        # count moved, the join keys are wrong.
        Expectation("sum_close_to", SILVER_ENRICHED,
                    params={"column": "interval_kwh", "against": SILVER_READINGS,
                            "against_column": "interval_kwh", "tolerance": 0.001},
                    name="enrich_preserves_volume"),
        Expectation("referential", SILVER_ENRICHED, "meter_id",
                    params={"parent": BRONZE_METERS, "parent_column": "meter_id"}),
    ),
    lineage={
        SILVER_ENRICHED: (SILVER_READINGS, BRONZE_METERS, BRONZE_TARIFFS, BRONZE_WEATHER),
    },
)

# -- gold ------------------------------------------------------------------

_GOLD_DAILY_REGION = Step(
    key=GOLD_DAILY_REGION,
    layer="gold",
    title="Daily consumption and cost per region",
    inputs=(SILVER_ENRICHED,),
    outputs=(GOLD_DAILY_REGION,),
    depends_on=("silver_enrich",),
    pre=(
        # GROUP BY ALL groups by every non-aggregated select item. It removes
        # the single most common refactoring bug in analytics SQL: adding a
        # dimension and forgetting to add it to GROUP BY.
        """
        CREATE OR REPLACE TEMP VIEW daily AS
        SELECT business_date,
               region,
               count(DISTINCT meter_id)                        AS meters,
               round(sum(interval_kwh), 3)                      AS kwh,
               round(sum(cost_eur), 2)                          AS cost_eur,
               round(sum(interval_kwh) FILTER (WHERE is_peak), 3) AS peak_kwh,
               round(avg(temp_c), 2)                            AS avg_temp_c
        FROM silver_enriched
        GROUP BY ALL
        """,
    ),
    selects={
        # The RANGE frame is written for real history even though a single run
        # only has one date in scope, so it degenerates to today. Assignment 13
        # makes it real by widening the read to the last seven partitions.
        GOLD_DAILY_REGION: """
            SELECT *,
                   round(kwh / nullif(meters, 0), 4)          AS kwh_per_meter,
                   round(100.0 * peak_kwh / nullif(kwh, 0), 2) AS peak_share_pct,
                   round(avg(kwh) OVER (
                       PARTITION BY region ORDER BY business_date
                       RANGE BETWEEN INTERVAL 6 DAY PRECEDING AND CURRENT ROW
                   ), 3)                                      AS kwh_7d_avg,
                   dense_rank() OVER (
                       PARTITION BY business_date ORDER BY kwh DESC
                   )                                          AS rank_in_day
            FROM daily
        """
    },
    sla_seconds=60,
    expectations=(
        Expectation("row_count_min", GOLD_DAILY_REGION, params={"min": 1}),
        Expectation("unique", GOLD_DAILY_REGION, params={"columns": ["business_date", "region"]}),
        Expectation("between", GOLD_DAILY_REGION, "peak_share_pct", params={"min": 0, "max": 100}),
        Expectation("sum_close_to", GOLD_DAILY_REGION,
                    params={"column": "kwh", "against": SILVER_ENRICHED,
                            "against_column": "interval_kwh", "tolerance": 0.01},
                    name="gold_reconciles_to_silver"),
    ),
    lineage={GOLD_DAILY_REGION: (SILVER_ENRICHED,)},
)

_GOLD_METER_ANOMALIES = Step(
    key=GOLD_METER_ANOMALIES,
    layer="gold",
    title="Per-meter consumption scored against its region",
    inputs=(SILVER_ENRICHED,),
    outputs=(GOLD_METER_ANOMALIES,),
    depends_on=("silver_enrich",),
    pre=(
        # any_value() lets capacity_kw ride along without becoming a grouping
        # key -- it is functionally dependent on meter_id, and listing it in
        # GROUP BY would be a lie about the grain.
        """
        CREATE OR REPLACE TEMP VIEW per_meter AS
        SELECT meter_id, region, customer_segment, business_date,
               any_value(capacity_kw)      AS capacity_kw,
               round(sum(interval_kwh), 3) AS kwh,
               round(sum(cost_eur), 2)     AS cost_eur,
               round(max(interval_kwh), 3) AS max_interval_kwh,
               count(*)                    AS intervals
        FROM silver_enriched
        GROUP BY ALL
        """,
        # Raw kWh is useless for outlier detection here: a 27 kW commercial
        # connection legitimately draws ten times a 3 kW flat. Normalising by
        # installed capacity first is what turns the z-score from noise into a
        # signal, and it is the single most important line in this step.
        """
        CREATE OR REPLACE TEMP VIEW normalised AS
        SELECT *, round(kwh / nullif(capacity_kw, 0), 4) AS kwh_per_kw
        FROM per_meter
        """,
        """
        CREATE OR REPLACE TEMP VIEW banded AS
        SELECT *,
               round((kwh_per_kw - region_avg) / nullif(region_sd, 0), 3) AS z_score,
               CASE
                   WHEN abs((kwh_per_kw - region_avg) / nullif(region_sd, 0)) >= 3 THEN 'severe'
                   WHEN abs((kwh_per_kw - region_avg) / nullif(region_sd, 0)) >= 2 THEN 'watch'
                   ELSE 'normal'
               END AS anomaly_band
        FROM (
            SELECT *,
                   avg(kwh_per_kw)         OVER (PARTITION BY region) AS region_avg,
                   stddev_samp(kwh_per_kw) OVER (PARTITION BY region) AS region_sd
            FROM normalised
        )
        """,
    ),
    selects={
        # Everything flagged, plus each region's top five, so the table is
        # never empty on a clean day and the dashboard has something to show.
        GOLD_METER_ANOMALIES: """
            SELECT * EXCLUDE (region_avg, region_sd),
                   round(region_avg, 4) AS region_avg_kwh_per_kw,
                   round(region_sd, 4)  AS region_sd_kwh_per_kw
            FROM banded
            QUALIFY anomaly_band <> 'normal'
                 OR rank() OVER (PARTITION BY region ORDER BY kwh_per_kw DESC) <= 5
        """
    },
    sla_seconds=60,
    expectations=(
        Expectation("unique", GOLD_METER_ANOMALIES,
                    params={"columns": ["business_date", "meter_id"]}),
        Expectation("accepted_values", GOLD_METER_ANOMALIES, "anomaly_band",
                    params={"values": ["severe", "watch", "normal"]}),
        Expectation("row_count_max", GOLD_METER_ANOMALIES, params={"max": 100000},
                    severity=Severity.WARN.value),
    ),
    lineage={GOLD_METER_ANOMALIES: (SILVER_ENRICHED,)},
)


STEPS: dict[str, Step] = {
    s.key: s
    for s in (
        _BRONZE_READINGS,
        _BRONZE_METERS,
        _BRONZE_TARIFFS,
        _BRONZE_WEATHER,
        _SILVER_CLEANSE,
        _SILVER_ENRICH,
        _GOLD_DAILY_REGION,
        _GOLD_METER_ANOMALIES,
    )
}

#: Execution phases. Steps inside a phase run concurrently; phases are ordered.
#: The workflow walks this list rather than topologically sorting `depends_on`,
#: because an explicit phase list is easier to reason about when you are staring
#: at a 400-event history trying to work out what ran when.
PHASES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("bronze", (BRONZE_READINGS, BRONZE_METERS, BRONZE_TARIFFS, BRONZE_WEATHER)),
    ("cleanse", ("silver_cleanse",)),
    ("enrich", ("silver_enrich",)),
    ("gold", (GOLD_DAILY_REGION, GOLD_METER_ANOMALIES)),
)

ALL_STEP_KEYS: tuple[str, ...] = tuple(k for _, keys in PHASES for k in keys)


def step(key: str) -> Step:
    return STEPS[key]


def dataset(name: str) -> Dataset:
    return DATASETS[name]


def validate_catalog() -> list[str]:
    """Cheap structural checks. Called by the worker at startup and by a unit
    test, so a typo in this file fails fast instead of failing halfway through
    a run at 02:00."""
    problems: list[str] = []
    seen: set[str] = set()
    for phase, keys in PHASES:
        for key in keys:
            if key not in STEPS:
                problems.append(f"phase {phase}: unknown step {key!r}")
                continue
            s = STEPS[key]
            for name in s.inputs:
                if name not in DATASETS:
                    problems.append(f"{key}: unknown input dataset {name!r}")
                elif DATASETS[name].layer != "landing" and name not in seen:
                    problems.append(f"{key}: input {name!r} is produced after it is read")
            for name in s.outputs:
                if name not in DATASETS:
                    problems.append(f"{key}: unknown output dataset {name!r}")
                if name not in s.selects:
                    problems.append(f"{key}: output {name!r} has no SELECT")
            for name in s.selects:
                if name not in s.outputs:
                    problems.append(f"{key}: SELECT {name!r} is not a declared output")
            for exp in s.expectations:
                if exp.dataset not in DATASETS:
                    problems.append(f"{key}: expectation on unknown dataset {exp.dataset!r}")
        seen.update(
            out for key in keys if key in STEPS for out in STEPS[key].outputs
        )
    for name in PUBLISHED:
        if name not in DATASETS:
            problems.append(f"published dataset {name!r} is not in the catalog")
    return problems


# --------------------------------------------------------------------------
# SQL rendering context
# --------------------------------------------------------------------------

#: A 15-minute interval on the largest meter in the fixture is under 7 kWh, so
#: anything past this is a counter glitch rather than a customer.
SPIKE_KWH = 40.0


def render_context(business_date: str, run_id: str) -> dict[str, str]:
    """Values substituted into step SQL. Deterministic and tiny on purpose --
    anything that varies between a run and its replay does not belong here."""
    return {
        "business_date": business_date,
        "run_id": run_id,
        "spike_kwh": repr(SPIKE_KWH),
    }
