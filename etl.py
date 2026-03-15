#!/usr/bin/env python3
"""
SuperStore ETL Pipeline
=======================
Extract  → SuperStoreOrders.csv
Transform → dim_customer, dim_date, dim_product_scd2 (SCD Type 2), fact_orders
Load     → PostgreSQL  superstore_warehouse  (upsert-safe, idempotent)
"""

import logging
import sys
from datetime import timedelta
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
DB_URL   = "postgresql://postgres:postgres123@localhost:5432/superstore_warehouse"
CSV_PATH = "SuperStoreOrders.csv"

# ── DDL ───────────────────────────────────────────────────────────────────────
DDL = """
-- Date dimension: one row per calendar day, keyed by YYYYMMDD integer
CREATE TABLE IF NOT EXISTS dim_date (
    date_key     INTEGER      PRIMARY KEY,
    full_date    DATE         NOT NULL UNIQUE,
    day          SMALLINT,
    month        SMALLINT,
    month_name   VARCHAR(10),
    quarter      SMALLINT,
    year         SMALLINT,
    weekday      SMALLINT,          -- 0 = Monday … 6 = Sunday
    weekday_name VARCHAR(10),
    is_weekend   BOOLEAN
);

-- Customer dimension: unique on the full attribute set (no natural customer_id)
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key  INTEGER  PRIMARY KEY,
    customer_name TEXT     NOT NULL,
    segment       TEXT,
    state         TEXT,
    country       TEXT,
    market        TEXT,
    region        TEXT,
    UNIQUE (customer_name, segment, state, country, market, region)
);

-- Product dimension — Slowly Changing Dimension Type 2
-- Each change in (product_name, category, sub_category) creates a new version.
-- Active record: end_date IS NULL AND is_current = TRUE
-- Expired record: end_date IS NOT NULL AND is_current = FALSE
CREATE TABLE IF NOT EXISTS dim_product_scd2 (
    product_key  INTEGER  PRIMARY KEY,
    product_id   TEXT     NOT NULL,
    product_name TEXT,
    category     TEXT,
    sub_category TEXT,
    start_date   DATE     NOT NULL,
    end_date     DATE,                         -- NULL while record is active
    is_current   BOOLEAN  NOT NULL DEFAULT TRUE,
    UNIQUE (product_id, start_date)
);

-- Fact table: one row per order line item
CREATE TABLE IF NOT EXISTS fact_orders (
    fact_key       INTEGER  PRIMARY KEY,
    order_id       TEXT     NOT NULL,
    date_key       INTEGER  NOT NULL REFERENCES dim_date(date_key),
    ship_date_key  INTEGER           REFERENCES dim_date(date_key),
    customer_key   INTEGER  NOT NULL REFERENCES dim_customer(customer_key),
    product_key    INTEGER  NOT NULL REFERENCES dim_product_scd2(product_key),
    sales          NUMERIC(12, 4),
    quantity       INTEGER,
    discount       NUMERIC(6,  4),
    profit         NUMERIC(12, 4),
    shipping_cost  NUMERIC(10, 4),
    ship_mode      TEXT,
    order_priority TEXT
);
"""

# ─────────────────────────────────────────────────────────────────────────────
# EXTRACT
# ─────────────────────────────────────────────────────────────────────────────

def _parse_mixed_dates(series: pd.Series) -> pd.Series:
    """
    The source CSV contains two date formats:
      - MM/DD/YYYY  (slash-separated, month-first US style)
      - DD-MM-YYYY  (dash-separated, day-first European style)
    Parse each subset with the correct format, then recombine.
    """
    result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    slash = series.str.contains("/", na=False)
    dash  = series.str.contains("-", na=False)
    result[slash] = pd.to_datetime(series[slash], format="%m/%d/%Y", errors="coerce")
    result[dash]  = pd.to_datetime(series[dash],  format="%d-%m-%Y", errors="coerce")
    unparsed = result.isna().sum()
    if unparsed:
        log.warning("  %d date values could not be parsed", unparsed)
    return result


def extract(path: str = CSV_PATH) -> pd.DataFrame:
    """Read the CSV and coerce column types."""
    log.info("EXTRACT  reading '%s'", path)
    df = pd.read_csv(path, encoding="utf-8-sig")

    df["order_date"]    = _parse_mixed_dates(df["order_date"])
    df["ship_date"]     = _parse_mixed_dates(df["ship_date"])
    df["sales"]         = pd.to_numeric(df["sales"],         errors="coerce")
    df["profit"]        = pd.to_numeric(df["profit"],        errors="coerce")
    df["shipping_cost"] = pd.to_numeric(df["shipping_cost"], errors="coerce")
    df["discount"]      = pd.to_numeric(df["discount"],      errors="coerce")
    df["quantity"]      = pd.to_numeric(df["quantity"],      errors="coerce").astype("Int64")

    log.info("         %d rows, %d columns", len(df), len(df.columns))
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORM
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate on the six customer attribute columns.
    Surrogate key is an incrementing integer assigned in sorted order.
    """
    cols = ["customer_name", "segment", "state", "country", "market", "region"]
    dim = (
        df[cols]
        .drop_duplicates()
        .sort_values(cols)
        .reset_index(drop=True)
    )
    dim.insert(0, "customer_key", range(1, len(dim) + 1))
    log.info("TRANSFORM dim_customer     : %6d rows", len(dim))
    return dim


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a full calendar spine covering every day between the earliest
    and latest dates that appear in order_date or ship_date.
    """
    all_dates = pd.concat([df["order_date"], df["ship_date"]]).dropna()
    date_range = pd.date_range(all_dates.min(), all_dates.max(), freq="D")

    dim = pd.DataFrame({"full_date": date_range})
    dim["date_key"]     = dim["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim["day"]          = dim["full_date"].dt.day.astype("int16")
    dim["month"]        = dim["full_date"].dt.month.astype("int16")
    dim["month_name"]   = dim["full_date"].dt.strftime("%B")
    dim["quarter"]      = dim["full_date"].dt.quarter.astype("int16")
    dim["year"]         = dim["full_date"].dt.year.astype("int16")
    dim["weekday"]      = dim["full_date"].dt.weekday.astype("int16")
    dim["weekday_name"] = dim["full_date"].dt.strftime("%A")
    dim["is_weekend"]   = dim["weekday"] >= 5
    dim["full_date"]    = dim["full_date"].dt.date          # store as Python date

    dim = dim[[
        "date_key", "full_date", "day", "month", "month_name",
        "quarter", "year", "weekday", "weekday_name", "is_weekend",
    ]]
    log.info("TRANSFORM dim_date         : %6d rows", len(dim))
    return dim


def build_dim_product_scd2(df: pd.DataFrame) -> pd.DataFrame:
    """
    SCD Type 2 for product attributes (product_name, category, sub_category).

    Algorithm
    ---------
    For each product_id, walk the source rows in order_date order.
    Each time the tracked attributes differ from the previous row, the
    current version is *expired* (end_date set to the day before the
    change) and a new version is *opened* with is_current=True.

    Only the most recent version has end_date IS NULL / is_current=TRUE.
    """
    attr_cols = ["product_name", "category", "sub_category"]
    # Some product_ids have multiple attribute sets on the same order_date (data
    # quality issue in source).  Resolve by keeping one deterministic set per
    # (product_id, order_date) before the chronological walk — this prevents
    # an SCD2 version where end_date < start_date.
    src = (
        df[["product_id", "order_date"] + attr_cols]
        .sort_values(["product_id", "order_date"] + attr_cols)   # deterministic
        .groupby(["product_id", "order_date"], as_index=False)
        .first()                                                   # one set per date
        .sort_values(["product_id", "order_date"])
    )

    records: list[dict] = []
    pk = 1

    for pid, grp in src.groupby("product_id", sort=True):
        current_attrs: Optional[tuple] = None
        current_start: Optional[pd.Timestamp] = None

        for row in grp.itertuples(index=False):
            attrs = (row.product_name, row.category, row.sub_category)

            if current_attrs is None:
                # First row for this product
                current_attrs = attrs
                current_start = row.order_date
            elif attrs != current_attrs:
                # Attribute change detected → expire the open version
                records.append(_scd2_record(
                    pk, pid, current_attrs, current_start,
                    end=row.order_date - timedelta(days=1),
                    is_current=False,
                ))
                pk += 1
                current_attrs = attrs
                current_start = row.order_date
            # Same attributes → continue existing version

        # Flush the final (currently active) version
        records.append(_scd2_record(
            pk, pid, current_attrs, current_start,
            end=None, is_current=True,
        ))
        pk += 1

    dim = pd.DataFrame(records)
    dim["start_date"] = pd.to_datetime(dim["start_date"]).dt.date
    dim["end_date"]   = pd.to_datetime(dim["end_date"]).dt.date   # NaT → None handled by to_sql

    versioned = len(dim) - dim["product_id"].nunique()
    log.info(
        "TRANSFORM dim_product_scd2 : %6d rows  (%d products, %d extra versions from changes)",
        len(dim), dim["product_id"].nunique(), versioned,
    )
    return dim


def _scd2_record(pk, pid, attrs, start, end, is_current) -> dict:
    return {
        "product_key":  pk,
        "product_id":   pid,
        "product_name": attrs[0],
        "category":     attrs[1],
        "sub_category": attrs[2],
        "start_date":   start,
        "end_date":     end,
        "is_current":   is_current,
    }


def build_fact_orders(
    df: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_product: pd.DataFrame,
) -> pd.DataFrame:
    """
    Join source rows to surrogate keys.

    SCD2 product key lookup
    -----------------------
    Uses pandas.merge_asof (backward) to find, for each order row, the
    product version whose start_date is the latest one that is still
    ≤ order_date.  A secondary filter confirms order_date ≤ end_date
    (or end_date is null), guarding against any gap in SCD2 coverage.
    """
    fact = df.copy()

    # ── date_key ─────────────────────────────────────────────────────────────
    # dim_date.full_date is Python date; align types for merge
    date_lkp = dim_date[["full_date", "date_key"]].copy()
    date_lkp["full_date"] = pd.to_datetime(date_lkp["full_date"])

    fact = fact.merge(
        date_lkp.rename(columns={"full_date": "order_date", "date_key": "date_key"}),
        on="order_date", how="left",
    )
    fact = fact.merge(
        date_lkp.rename(columns={"full_date": "ship_date", "date_key": "ship_date_key"}),
        on="ship_date", how="left",
    )

    # ── customer_key ──────────────────────────────────────────────────────────
    cust_cols = ["customer_name", "segment", "state", "country", "market", "region"]
    fact = fact.merge(dim_customer[cust_cols + ["customer_key"]], on=cust_cols, how="left")

    # ── product_key (SCD2 interval join) ──────────────────────────────────────
    scd2_lkp = (
        dim_product[["product_key", "product_id", "start_date", "end_date"]]
        .copy()
    )
    # Cast to datetime64[ns] to match fact.order_date — merge_asof requires
    # identical dtypes on the join keys.
    scd2_lkp["start_date"] = pd.to_datetime(scd2_lkp["start_date"]).astype("datetime64[ns]")
    scd2_lkp["end_date"]   = pd.to_datetime(scd2_lkp["end_date"]).astype("datetime64[ns]")
    scd2_lkp = scd2_lkp.sort_values("start_date")

    fact_sorted = fact.sort_values("order_date")
    fact_sorted = pd.merge_asof(
        fact_sorted,
        scd2_lkp,
        left_on="order_date",
        right_on="start_date",
        by="product_id",
        direction="backward",           # latest start_date ≤ order_date
    )

    # Validate that the matched version has not already expired
    out_of_range = (
        fact_sorted["end_date"].notna() &
        (fact_sorted["order_date"] > fact_sorted["end_date"])
    )
    if out_of_range.any():
        log.warning("  %d rows matched an already-expired SCD2 version — check source data",
                    out_of_range.sum())

    unmatched = fact_sorted["product_key"].isna().sum()
    if unmatched:
        log.warning("  %d rows have no SCD2 product match", unmatched)

    # ── Assemble final columns ────────────────────────────────────────────────
    fact_final = fact_sorted[[
        "order_id", "date_key", "ship_date_key", "customer_key", "product_key",
        "sales", "quantity", "discount", "profit", "shipping_cost",
        "ship_mode", "order_priority",
    ]].copy()

    fact_final.insert(0, "fact_key", range(1, len(fact_final) + 1))

    # Cast surrogate keys to plain int (merge may produce float if any NaN snuck in)
    for col in ("date_key", "ship_date_key", "customer_key", "product_key"):
        fact_final[col] = fact_final[col].astype(int)
    fact_final["quantity"] = fact_final["quantity"].astype(int)

    log.info("TRANSFORM fact_orders      : %6d rows", len(fact_final))
    return fact_final


# ─────────────────────────────────────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────────────────────────────────────

def create_schema(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(DDL))
    log.info("LOAD     schema verified / created")


def _truncate_all(engine) -> None:
    """
    Drop all rows in FK-safe order and reset identity sequences.
    Running this in a single statement keeps it atomic.
    """
    with engine.begin() as conn:
        conn.execute(text(
            "TRUNCATE TABLE fact_orders, dim_product_scd2, dim_customer, dim_date "
            "RESTART IDENTITY CASCADE"
        ))
    log.info("LOAD     all tables truncated")


def _bulk_insert(engine, df: pd.DataFrame, table: str) -> None:
    """
    Append-insert the DataFrame into *table* using SQLAlchemy / psycopg2.
    Tables are always empty at this point (post-truncate), so there are
    no conflicts.  method='multi' batches rows into fewer round-trips.
    """
    df.to_sql(table, engine, if_exists="append", index=False,
              chunksize=2_000, method="multi")
    log.info("LOAD     %-22s  %d rows inserted", table, len(df))


def load(engine, dim_customer, dim_date, dim_product, fact_orders) -> None:
    create_schema(engine)
    _truncate_all(engine)
    _bulk_insert(engine, dim_date,     "dim_date")
    _bulk_insert(engine, dim_customer, "dim_customer")
    _bulk_insert(engine, dim_product,  "dim_product_scd2")
    _bulk_insert(engine, fact_orders,  "fact_orders")


# ─────────────────────────────────────────────────────────────────────────────
# QUALITY CHECKS
# ─────────────────────────────────────────────────────────────────────────────

def run_quality_checks(engine) -> bool:
    """
    Execute SQL-based data quality checks against the loaded warehouse tables.

    Returns True if every check passes (zero violations), False otherwise.

    Checks
    ------
    1. No duplicate order line items  — no two fact rows may share identical
       business columns (same order_id + all metrics); a product legitimately
       appearing twice in one order with different quantities is not a duplicate.
    2. SCD2 date validity             — every dim_product_scd2 row must have a
       non-null start_date; expired rows must have end_date ≥ start_date;
       is_current flags must align with end_date nullability.
    3. No orphaned customer_key       — every customer_key in fact_orders must
       resolve to dim_customer.
    4. No orphaned product_key        — every product_key in fact_orders must
       resolve to dim_product_scd2.
    5. No orphaned date_key           — every date_key in fact_orders must
       resolve to dim_date.
    """
    checks = [
        (
            "No duplicate order line items",
            # Check for fully identical business rows (excluding the surrogate
            # fact_key).  Same product ordered twice with different quantities
            # is legitimate and will not trigger this check.
            """
            SELECT COUNT(*) AS cnt
            FROM (
                SELECT order_id, date_key, ship_date_key, customer_key, product_key,
                       sales, quantity, discount, profit, shipping_cost,
                       ship_mode, order_priority,
                       COUNT(*) AS n
                FROM   fact_orders
                GROUP  BY order_id, date_key, ship_date_key, customer_key, product_key,
                          sales, quantity, discount, profit, shipping_cost,
                          ship_mode, order_priority
                HAVING COUNT(*) > 1
            ) AS dups
            """,
        ),
        (
            "SCD2 date validity (start/end/is_current coherence)",
            """
            SELECT COUNT(*) AS cnt
            FROM dim_product_scd2
            WHERE start_date IS NULL
               OR (end_date IS NOT NULL AND end_date < start_date)
               OR (is_current = TRUE  AND end_date  IS NOT NULL)
               OR (is_current = FALSE AND end_date  IS NULL)
            """,
        ),
        (
            "No orphaned customer_key in fact_orders",
            """
            SELECT COUNT(*) AS cnt
            FROM       fact_orders   f
            LEFT JOIN  dim_customer  c ON f.customer_key = c.customer_key
            WHERE c.customer_key IS NULL
            """,
        ),
        (
            "No orphaned product_key in fact_orders",
            """
            SELECT COUNT(*) AS cnt
            FROM       fact_orders      f
            LEFT JOIN  dim_product_scd2 p ON f.product_key = p.product_key
            WHERE p.product_key IS NULL
            """,
        ),
        (
            "No orphaned date_key in fact_orders",
            """
            SELECT COUNT(*) AS cnt
            FROM       fact_orders  f
            LEFT JOIN  dim_date     d ON f.date_key = d.date_key
            WHERE d.date_key IS NULL
            """,
        ),
    ]

    all_passed = True
    log.info("QUALITY  running %d checks …", len(checks))
    with engine.connect() as conn:
        for name, sql in checks:
            violations = conn.execute(text(sql)).scalar()
            status = "PASS" if violations == 0 else "FAIL"
            if violations != 0:
                all_passed = False
            log.info("  [%s]  %-52s  violations=%d", status, name, violations)

    return all_passed


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    engine = create_engine(DB_URL, echo=False)

    # ── Extract ───────────────────────────────────────────────────────────────
    df = extract()

    # ── Transform ─────────────────────────────────────────────────────────────
    dim_customer = build_dim_customer(df)
    dim_date     = build_dim_date(df)
    dim_product  = build_dim_product_scd2(df)
    fact_orders  = build_fact_orders(df, dim_customer, dim_date, dim_product)

    # ── Load ──────────────────────────────────────────────────────────────────
    load(engine, dim_customer, dim_date, dim_product, fact_orders)

    # ── Quality checks ────────────────────────────────────────────────────────
    passed = run_quality_checks(engine)
    if not passed:
        log.error("One or more quality checks FAILED — review output above")
        sys.exit(1)

    log.info("Pipeline complete — all checks passed")


if __name__ == "__main__":
    main()
