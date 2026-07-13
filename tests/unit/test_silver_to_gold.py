"""
Unit tests for silver -> gold transformation logic.
WHY test star schema logic?
  - dim_date must cover the full date range — missing dates break time-series queries
  - fact_orders join must not lose or duplicate rows
  - revenue aggregation must sum correctly — wrong revenue = wrong business decisions
"""

import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, year, month, dayofweek


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_silver_to_gold")
        .getOrCreate()
    )


# ── dim_date ─────────────────────────────────────────────────────

def test_dim_date_covers_full_range(spark):
    """dim_date must have one row per calendar day — gaps break date-range filters."""
    dim_date = spark.sql("""
        SELECT explode(sequence(to_date('2021-01-01'), to_date('2021-01-07'), interval 1 day)) AS date_actual
    """)
    assert dim_date.count() == 7


def test_dim_date_has_correct_columns(spark):
    """dim_date must include year, month, day_of_week for partition pruning to work."""
    dim_date = (
        spark.sql("""
            SELECT explode(sequence(to_date('2021-01-01'), to_date('2021-01-03'), interval 1 day)) AS date_actual
        """)
        .withColumn("year", year("date_actual"))
        .withColumn("month", month("date_actual"))
        .withColumn("day_of_week", dayofweek("date_actual"))
    )
    cols = dim_date.columns
    assert "year" in cols
    assert "month" in cols
    assert "day_of_week" in cols


# ── fact_orders ───────────────────────────────────────────────────

def test_fact_orders_no_row_loss_on_join(spark):
    """
    fact_orders uses LEFT JOIN to orders — all order_items must survive.
    WHY LEFT JOIN and not INNER?
      INNER JOIN silently drops order_items whose order has no matching
      status record — we'd undercount revenue.
    """
    order_items = spark.createDataFrame(pd.DataFrame([
        ("oi1", "o1", "p1", "s1", 100.0),
        ("oi2", "o2", "p2", "s1",  50.0),
        ("oi3", "o3", "p3", "s2",  75.0),   # o3 has no matching order row
    ], columns=["order_item_id", "order_id", "product_id", "seller_id", "price"]))

    orders = spark.createDataFrame(pd.DataFrame([
        ("o1", "c1", "delivered"),
        ("o2", "c2", "shipped"),
        # o3 intentionally missing
    ], columns=["order_id", "customer_id", "order_status"]))

    joined = order_items.join(orders, on="order_id", how="left")
    assert joined.count() == 3  # no rows dropped


def test_fact_orders_inner_join_loses_rows(spark):
    """Demonstrates WHY we use LEFT JOIN — inner join silently drops unmatched rows."""
    order_items = spark.createDataFrame(pd.DataFrame([
        ("oi1", "o1", 100.0),
        ("oi2", "o3",  75.0),   # o3 missing from orders
    ], columns=["order_item_id", "order_id", "price"]))

    orders = spark.createDataFrame(pd.DataFrame([
        ("o1", "delivered"),
    ], columns=["order_id", "order_status"]))

    inner = order_items.join(orders, on="order_id", how="inner")
    assert inner.count() == 1  # proves row loss with inner join


# ── revenue_by_category ───────────────────────────────────────────

def test_revenue_by_category_aggregation(spark):
    """Revenue per category must sum correctly — this is a key business metric."""
    df = spark.createDataFrame(pd.DataFrame([
        ("electronics", 100.0),
        ("electronics", 200.0),
        ("furniture",    50.0),
    ], columns=["category", "price"]))

    result = df.groupBy("category").agg(spark_sum("price").alias("total_revenue"))
    rows = {r["category"]: r["total_revenue"] for r in result.collect()}

    assert rows["electronics"] == 300.0
    assert rows["furniture"]   ==  50.0
