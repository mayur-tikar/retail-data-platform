import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

SILVER_DIR = os.getenv("SILVER_DIR", "/opt/airflow/data/silver")
GOLD_DIR = os.getenv("GOLD_DIR", "/opt/airflow/data/gold")
GOLD_BUCKET = "retail-gold"
LOCALSTACK_URL = "http://localstack:4566"

def ensure_buckets():
    """Create buckets if they don't exist - idempotent, safe to run every time."""
    s3 = boto3.client(
        "s3",
        endpoint_url = LOCALSTACK_URL,
        aws_access_key_id = "test",
        aws_secret_access_key = "test",
        region_name = "us-east-1",
    )

    for bucket in ["retail-raw", "retail-silver", "retail-gold", "retail-scripts"]:
        try:
            s3.head_bucket(Bucket=bucket)
        except Exception:
            s3.create_bucket(Bucket=bucket)
            print(f"    Create bucket:  {bucket}")

def get_spark():
    return (
        SparkSession.builder
        .appName("OlistSilverToGold")
        .master("local[*]")
        .config("spark.sql.shuffle.partittions", "8")
        .getOrCreate()
    )

def read_silver(spark, table_name):
    path = os.path.join(SILVER_DIR, table_name)
    return spark.read.parquet(path)

def write_gold(df, table_name, partition_col=None):
    local_path = os.path.join(GOLD_DIR, table_name)
    writer = df.write.mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(*partition_col)
    writer.parquet(local_path)
    print(f"    Written locally ->  {local_path}")
    upload_to_s3(local_path, table_name)

def upload_to_s3(local_path, table_name):
    s3 = boto3.client(
        "s3",
        endpoint_url = LOCALSTACK_URL,
        aws_access_key_id = "test",
        aws_secret_access_key = "test",
        region_name = "us-east-1",
    )

    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            s3_key = f"{table_name}/{os.path.relpath(local_file, local_path)}"
            s3.upload_file(local_file, GOLD_BUCKET, s3_key)
    print(f"    Uploaded    ->  s3://retail-gold/{table_name}")


# ─────────────────────────────────────────────────────────────
# DIM: dim_date
# WHY build a date dimension?
#   Analysts constantly filter by date — "this month", "last quarter",
#   "fiscal year". Storing date attributes (is_weekend, fiscal_quarter)
#   in a dimension means analysts write one JOIN instead of computing
#   these attributes in every query. Custom fiscal calendars can't
#   be computed with date functions alone — they live here.
# 

def build_dim_date(spark):
    print("\nBuilding dim_date...")

    df = spark.sql("""
                   SELECT sequence(
                   to_date('2016-01-01'),
                   to_date('2019-12-31'),
                   interval 1 day
                ) AS date_array
        """).selectExpr("explode(date_array) as full_date")
    
    dim_date = (
        df
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("week", F.weekofyear("full_date"))
        .withColumn("day_of_month", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin([1,7]))
        # Brazil fiscal year follows calendar year
        .withColumn("fiscal_year", F.year("full_date"))
        .withColumn("fiscal_quarter", F.quarter("full_date"))
        .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
    )

    write_gold(dim_date, "dim_date")
    print(f"    dim_date:   {dim_date.count()} rows")

# ─────────────────────────────────────────────────────────────
# DIM: dim_customers
# INTERVIEW POINT:
#   customer_unique_id is the real person.
#   customer_id is per-order (Olist generates a new one per order).
#   We group by customer_unique_id here to get TRUE customer count.
#   This is a real-world data quality issue — interviewers love this.
# ─────────────────────────────────────────────────────────────

def build_dim_customers(spark):
    print("\n   Building dim_customers...")

    df = read_silver(spark, "customers")

    # One row per REAL customer (customer_unique_id)
    # Keep the most recent customer_id for joining to orders
    # WHY Window function here?
    #   We want one row per unique customer but need to pick WHICH
    #   customer_id to keep (they have many). We pick the latest one
    #   using row_number() — the most recent order's customer_id.

    from pyspark.sql import Window

    window = Window.partitionBy("customer_unique_id").orderBy(F.col("customer_id"). desc())

    dim_customers = (
        df
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "customer_unique_id",
            "customer_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            F.current_timestamp().alias("dw_created_at")
        )
    )

    write_gold(dim_customers, "dim_customers")
    print(f"    dim_customers:  {dim_customers.count()} real unique customers")
    print(f"    vs 99441 customer_ids   -   {99441 - dim_customers.count()} are repeat buyers")

# ─────────────────────────────────────────────────────────────
# DIM: dim_products
# ─────────────────────────────────────────────────────────────

def build_dim_products(spark):
    print("\n Building dim_products...")

    df = read_silver(spark, "products")

    dim_products = (
        df
        .select(
            "product_id",
            "product_category_name",
            "category_english",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "product_photos_qty",
        )
        # Volume in cubic cm — useful for logistics analysis
        .withColumn("volume_cm3",
                    F.col("product_length_cm") *
                    F.col("product_height_cm") *
                    F.col("product_width_cm")
        )
        # Weight tier for shipping analysis
        .withColumn("weight_tier",
                    F.when(F.col("product_weight_g") <= 300, "Light")
                    .when(F.col("product_weight_g") <=2000, "Medium")
                    .when(F.col("product_weight_g") <= 10000, "Heavy")
                    .otherwise("Very Heavy")
        )
        .withColumn("dw_created_at", F.current_timestamp())
    )

    write_gold(dim_products, "dim_products")
    print(f"    dim_products:   {dim_products.count()} rows")

# ─────────────────────────────────────────────────────────────
# DIM: dim_sellers
# ─────────────────────────────────────────────────────────────

def build_dim_seller(spark):
    print("\n Building dim_sellers...")

    df = read_silver(spark, "sellers")

    dim_sellers = (
        df
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        )
        .withColumn("dw_created_at", F.current_timestamp())
    )

    write_gold(dim_sellers, "dim_sellers")
    print(f"    dim_sellers:    {dim_sellers.count()} rows")

# ─────────────────────────────────────────────────────────────
# FACT: fact_orders
# WHY grain at order-item level?
#   Each row = one product in one order.
#   This lets you answer:
#     - Revenue per product category
#     - Average items per order
#     - Which sellers sell which products
#   Order-level grain can't answer any of these.
#
# WHY join payments here?
#   Payment info (type, installments, value) belongs in the fact
#   because it's a measurable attribute of the transaction.
#   One order can have multiple payments (credit card + voucher).
#   We aggregate payments to order level before joining.
# ─────────────────────────────────────────────────────────────

def build_fact_orders(spark):
    print("\n   Bilding fact_orders...")

    orders = read_silver(spark, "orders")
    order_items = read_silver(spark, "order_items")
    payments = read_silver(spark, "payments")
    customers = read_silver(spark, "customers")

    # Aggregate payments to order level
    # WHY aggregate? One order can have multiple payment rows.
    # We want one payment row per order in the fact table.

    payments_agg = (
        payments
        .groupBy("order_id")
        .agg(
            F.round(F.sum("payment_value"), 2).alias("total_payment_value"),
            F.countDistinct("payment_type").alias("payment_methods_count"),
            # Most used payment type for this order
            F.first("payment_type").alias("primary_payment_type"),
            F.max("payment_installments").alias("max_installments"),
        )
    )

    # Join customer_unique_id onto orders
    # WHY? orders only has customer_id (per-order ID).
    # We need customer_unique_id (real person ID) in the fact table
    # so analysts can do customer-level analysis without extra joins.

    orders_with_unique_id = (
        orders
        .join(
            customers.select("customer_id", "customer_unique_id"),
            on = "customer_id",
            how = "left"
        )
    )

    fact = (
        order_items
        .join(orders_with_unique_id, on="order_id", how="inner")
        .join(F.broadcast(payments_agg), on="order_id", how="left")
        # date_key for joining to dim_date
        .withColumn("order_date_key", F.date_format("order_purchase_timestamp", "yyyyMMdd").cast(IntegerType()))
        # Revenue metrics
        .withColumn("gross_revenue", F.round(F.col("price") + F.col("freight_value"), 2))
        # Delivery performance flag
        .withColumn("was_delivered_on_time", F.when(
            F.col("order_delivered_customer_date") <= F.col("order_estimated_delivery_date"),
            True
        ).otherwise(False)
        )
        .select(
            # Keys
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "customer_unique_id",
            "order_date_key",
            # Measures
            "price",
            "freight_value",
            "gross_revenue",
            "total_payment_value",
            "max_installments",
            # Context
            "primary_payment_type",
            "order_status",
            "was_delivered_on_time",
            "delivery_delay_days",
            # Partition columns
            "purchase_year",
            "purchase_month",
        )
    )
    
    write_gold(fact, "fact_orders", ["purchase_year", "purchase_month"])
    print(f"    fact_orders: {fact.count()} rows")
    return fact

# ─────────────────────────────────────────────────────────────
# AGGREGATE: revenue_by_category
# WHY pre-aggregate?
#   A dashboard query "revenue by category this month" on 112K
#   fact rows runs groupBy every time. Pre-computing stores
#   ~70 rows (one per category per month). Dashboard reads 70
#   rows instead of 112K. This is Materialized Aggregation.
# ─────────────────────────────────────────────────────────────

def build_revenue_by_category(spark, fact_df):
    print("\n   Building revenue_by_category...")

    products = read_silver(spark, "products")

    revenue = (
        fact_df
        .join(
            F.broadcast(products.select("product_id", "category_english")),
            on="product_id", how='left'
        )
        .filter(F.col("order_status") != "cancelled")
        .groupBy("purchase_year", "purchase_month", "category_english")
        .agg(
            F.round(F.sum("gross_revenue"), 2).alias("total_revenue"),
            F.round(F.sum("freight_value"), 2).alias("total_freight"),
            F.sum("price").alias("total_product_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.count("order_item_id").alias("item_sold"),
            F.round(F.avg("price"), 2).alias("avg_item_price"),
        )
        .orderBy("purchase_year", "purchase_month", F.desc("total_revenue"))
    )

    write_gold(revenue, "revenue_by_category", ["purchase_year", "purchase_month"])
    print(f"    revenue_by_category:    {revenue.count()} rows")

# ─────────────────────────────────────────────────────────────
# AGGREGATE: customer_lifetime_value
# ─────────────────────────────────────────────────────────────

def build_customer_ltv(spark, fact_df):
    print("\n Building customer_lifetime_value...")

    customers = read_silver(spark, "customers")

    # Get one row per unique customer
    unique_customers = (
        customers
        .select("customer_id", "customer_unique_id", "customer_city", "customer_state")
        .dropDuplicates(["customer_unique_id"])
    )

    ltv = (
        fact_df
        .filter(F.col("order_status") != "cancelled")
        .groupBy("customer_unique_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("gross_revenue"), 2).alias("lifetime_value"),
            F.round(F.avg("gross_revenue"), 2).alias("avg_order_value"),
            F.round(F.sum("freight_value"), 2).alias("total_freight_paid"),
            F.min("order_date_key").cast("string").alias("first_order_date"),
            F.max("order_date_key").cast("string").alias("last_order_date"),
            F.sum("order_item_id").alias("total_items_purchased"),
        )
        .join(unique_customers, on="customer_unique_id", how="left")
        .withColumn("customer_tier",
                    F.when(F.col("lifetime_value") >= 1000, "Gold")
                    .when(F.col("lifetime_value") >= 500, "Silver")
                    .when(F.col("lifetime_value") >= 100, "Bronze")
                    .otherwise("New")
        )
        .withColumn("is_repeat_buyer", F.col("total_orders") > 1)
    )

    write_gold(ltv, "customer_lifetime_value")
    print(f"    customer_lifetime_value:    {ltv.count()} rows")

# ─────────────────────────────────────────────────────────────
# AGGREGATE: seller_performance
# ─────────────────────────────────────────────────────────────

def build_seller_performance(spark, fact_df):
    print("\n   Building seller_performance...")

    sellers = read_silver(spark, "sellers")

    perf = (
        fact_df
        .filter(F.col("order_status") != "cancelled")
        .groupBy("seller_id", "purchase_year", "purchase_month")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.count("order_item_id").alias("total_item_sold"),
            F.round(F.sum("price"), 2).alias("total_revenue"),
            F.round(F.avg("price"), 2).alias("avg_item_price"),
            F.round(F.avg("delivery_delay_days"), 1).alias("avg_delivery_delay_days"),
            F.sum(F.when(F.col("was_delivered_on_time"), 1).otherwise(0)).alias("on_time_deliveries"),
            F.count("order_item_id").alias("total_deliveries"),
        )
        .withColumn("on_time_delivery_rate",
                    F.round(
                        F.col("on_time_deliveries") / F.col("total_deliveries") * 100, 2
                    ))
        .join(F.broadcast(sellers), on="seller_id", how="left")
    )

    write_gold(perf, "seller_performance", ["purchase_year", "purchase_month"])
    print(f"    seller_performance: {perf.count()} rows")

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("-" * 55)
    print(" Glue Job 02: Silver ->  Gold")
    print(f"    Source: {SILVER_DIR}")
    print(f"    Target: s3://retail-gold/")
    print("-" * 55)

    ensure_buckets()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Dimensions first - fact table join depend on them
    build_dim_date(spark)
    build_dim_customers(spark)
    build_dim_products(spark)
    build_dim_seller(spark)

    # Fact table
    fact_df = build_fact_orders(spark)

    # Aggregates - built on top of fact
    build_revenue_by_category(spark, fact_df)
    build_customer_ltv(spark, fact_df)
    build_seller_performance(spark, fact_df)

    print("-" * 55 )
    print(" Silver  ->  Gold complete.")
    print(" Next:   Load gold into Postgres (Redshift substitute)")
    print("-" * 55)

    spark.stop()
