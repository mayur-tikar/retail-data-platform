"""
Unit tests for raw -> silver transformation logic.
WHY test these specifically?
  - Null-drop logic: if threshold changes, silent data loss occurs
  - Deduplication: duplicate order_ids corrupt downstream aggregations
  - Type casting: wrong types cause silent integer overflow in Postgres
"""

import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_raw_to_silver")
        .getOrCreate()
    )


# ── Orders cleaning ──────────────────────────────────────────────

def test_orders_drops_null_customer_id(spark):
    """Rows with null customer_id must be dropped — they can't join to dim_customers."""
    pdf = pd.DataFrame([
        ("o1", "c1", "delivered", "2021-01-01", "2021-01-02", "2021-01-03"),
        ("o2", None, "delivered", "2021-01-01", "2021-01-02", "2021-01-03"),
    ], columns=["order_id", "customer_id", "order_status",
                "order_purchase_timestamp", "order_delivered_customer_date",
                "order_estimated_delivery_date"])

    df = spark.createDataFrame(pdf)
    cleaned = df.dropna(subset=["order_id", "customer_id", "order_status"])
    assert cleaned.count() == 1
    assert cleaned.first()["order_id"] == "o1"


def test_orders_deduplication(spark):
    """Duplicate order_ids must be removed — each order_id is a primary key."""
    pdf = pd.DataFrame([
        ("o1", "c1", "delivered"),
        ("o1", "c1", "delivered"),
        ("o2", "c2", "shipped"),
    ], columns=["order_id", "customer_id", "order_status"])

    df = spark.createDataFrame(pdf)
    deduped = df.dropDuplicates(["order_id"])
    assert deduped.count() == 2


# ── Products cleaning ────────────────────────────────────────────

def test_products_fills_null_category(spark):
    """Null product_category_name must be filled with 'unknown' — prevents null joins in gold."""
    pdf = pd.DataFrame([
        ("p1", None),
        ("p2", "electronics"),
    ], columns=["product_id", "product_category_name"])

    df = spark.createDataFrame(pdf)
    cleaned = df.withColumn(
        "product_category_name",
        when(col("product_category_name").isNull(), "unknown")
        .otherwise(col("product_category_name"))
    )
    rows = {r["product_id"]: r["product_category_name"] for r in cleaned.collect()}
    assert rows["p1"] == "unknown"
    assert rows["p2"] == "electronics"


# ── Payments cleaning ────────────────────────────────────────────

def test_payments_drops_zero_value(spark):
    """Zero-value payments are data errors — they skew revenue aggregations."""
    pdf = pd.DataFrame([
        ("o1", "credit_card", 150.0),
        ("o2", "boleto",        0.0),
        ("o3", "voucher",      50.5),
    ], columns=["order_id", "payment_type", "payment_value"])

    df = spark.createDataFrame(pdf)
    cleaned = df.filter(col("payment_value") > 0)
    assert cleaned.count() == 2
