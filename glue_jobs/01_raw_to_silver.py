import sys
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

# ─────────────────────────────────────────────────────────────
# CONFIG
# WHY read from environment variables?
#   Same script runs locally (pointing at LocalStack) and in real
#   AWS Glue (pointing at real S3) — zero code changes needed.
#   You just change the environment variables.
# ─────────────────────────────────────────────────────────────

import os

ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
RAW_BUCKET = "retail-raw"
SILVER_BUCKET = "retail-silver"
RAW_PREFIX = "olist"
LOCAL_DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data/raw")
LOCAL_SILVER_DIR = os.getenv("SILVER_DIR", "/opt/airflow/data/silver")
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
    """
    WHY this specific Spark config for LocalStack?
      LocalStack exposes S3 at http://localstack:4566 inside Docker.
      We tell Spark's S3 connector (hadoop-aws) to hit that endpoint
      instead of real AWS. The 'path.style.access' setting forces
      URLs like http://localstack:4566/bucket/key instead of
      http://bucket.localstack:4566/key — LocalStack needs path style.
    """
    return (
        SparkSession.builder
        .appName("OlistRawToSilver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    """
    return (
        SparkSession.builder
        .appName("OlistRawToSilver")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
        .config("spark.jars", "/home/glue_user/spark/jars/hadoop-aws-3.3.1.jar,/home/glue_user/spark/jars/aws-java-sdk-bundle-1.11.1026.jar")
        .getOrCreate()
    )

    builder = (
        SparkSession.builder
        .appName('OlistRawToSilver')
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.s3a.access.key", "test")
        .config("spark.hadoop.fs.s3a.secret.key", "test")
        .config("spark.hadoop.fs.s3a.endponint", "http://localhost:4566")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFILESYSTEM")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    return builder.getOrCreate()
    """
    

def read_csv(spark, filename):
    """Read CSV from S3 with header"""
    path = os.path.join(LOCAL_DATA_DIR, filename)
    return spark.read.option("header", True).option("inferSchema", False).csv(path)

    #return spark.read.option("header", True).option("inferSchema", False).csv(f"s3a://{bucket}/path")

def write_silver(df, table_name, partition_cols=None):
    """
    Write cleaned DataFrame to silver layer as Parquet.
    WHY Parquet? Columnar format — 5-10x smaller than CSV,
    much faster for analytics queries that only need a few columns.
    """
    """
    path = f"s3a://{bucket}/{table_name}"
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    print(f"  Writen  ->  s3://{bucket}/{table_name}")
    """

    """
    Write locally first, then upload to LocalStack S3.
    WHY local first? The Glue container doesn't have the S3 connector
    configured for LocalStack. In real AWS Glue, this writes directly to S3.
    """

    local_path = os.path.join(LOCAL_SILVER_DIR, table_name)
    writer = df.write.mode('overwrite')
    if partition_cols:
      writer = writer.partitionBy(*partition_cols)
    writer.parquet(local_path)
    print(f"  Writen locally: ->  {local_path}")

    # Upload to LocalStack S3
    upload_to_s3(local_path, table_name)

def upload_to_s3(local_path, table_name):
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url = "http://localstack:4566",
        aws_access_key_id = "test",
        aws_secret_access_key = "test",
        region_name = "us-east-1",
    )  

    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file = os.path.join(root, file)
            s3_key = f"{table_name}/{os.path.relpath(local_file, local_path)}"
            s3.upload_file(local_file, SILVER_BUCKET, s3_key)

    print(f"Upload  ->  s3://retail-silver/{table_name}")


# ─────────────────────────────────────────────────────────────
# CLEAN: ORDERS
# ─────────────────────────────────────────────────────────────

def clean_orders(spark):
    print("\n Cleaning orders...")

    df = read_csv(spark, "olist_orders_dataset.csv")

    df_clean = (
        df
        .dropDuplicates(["order_id"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        # Cast all timestamp columns from string to proper timestamp
        # WHY? String timestamps can't be used in date arithmetic.
        # You can't do "orders placed in last 30 days" on a string column.
        .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))
        # Derive delivery delay — real business metric
        # Positive = delivered late, Negative = delivered early
        .withColumn("delivery_delay_days", F.datediff(
            F.col("order_delivered_customer_date"),
            F.col("order_estimated_delivery_date")
        ))
        # Partition columns for efficient querying by date
        .withColumn("purchase_year", F.year("order_purchase_timestamp"))
        .withColumn("purchase_month", F.month("order_purchase_timestamp"))
        .withColumn("processed_at", F.current_timestamp())
    )

    print(f"  Orders: {df_clean.count()} rows after cleaning")
    write_silver(df_clean, "orders", ["purchase_year", "purchase_month"])
    return df_clean

# ─────────────────────────────────────────────────────────────
# CLEAN: CUSTOMERS
# ─────────────────────────────────────────────────────────────

def clean_customers(spark):
    """
    CRITICAL INTERVIEW POINT — the customer_id trap:
      In Olist, customer_id is generated PER ORDER — not per person.
      The same real customer has a different customer_id for each order.
      customer_unique_id is the real person identifier.
      If you GROUP BY customer_id to count customers, you get
      the number of orders — not the number of people. Wrong.
      Always use customer_unique_id for customer-level analysis.
    """

    print("\n Cleaning customers...")

    df = read_csv(spark, "olist_customers_dataset.csv")

    df_clean = (
        df
        .dropDuplicates(["customer_id"])
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("customer_unique_id").isNotNull())
        .withColumn("customer_city", F.initcap(F.trim(F.col("customer_city"))))
        .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
        .withColumn("processed_at", F.current_timestamp())
    )

    print(f"  Customers: {df_clean.count()} rows after cleaning")
    write_silver(df_clean, "customers")
    return df_clean

# ─────────────────────────────────────────────────────────────
# CLEAN: ORDER ITEMS
# ─────────────────────────────────────────────────────────────

def clean_order_items(spark):
    """
    WHY is (order_id, order_item_id) the primary key here?
      order_item_id is a sequence number within each order (1, 2, 3...).
      It resets for every order — so item_id=1 exists in every order.
      Only the COMBINATION of order_id + order_item_id is unique.
      This is called a Composite Primary Key.
    """

    print("\n Cleaning order_items...")

    df = read_csv(spark, "olist_order_items_dataset.csv")

    df_clean = (
        df
        .dropDuplicates(["order_id", "order_item_id"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("product_id").isNotNull())
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
        .withColumn("order_item_id", F.col("order_item_id").cast(IntegerType()))
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
        # Total item value including freight
        .withColumn("item_total_value", F.round(F.col("price") + F.col("freight_value"), 2))
        .withColumn("processed_at", F.current_timestamp())
    )

    print(f"  Order items: {df_clean.count()} rows after cleaning")
    write_silver(df_clean, "order_items")
    return df_clean

# ─────────────────────────────────────────────────────────────
# CLEAN: PRODUCTS
# ─────────────────────────────────────────────────────────────

def clean_products(spark):
    """
    Products need category translation from Portuguese to English.
    We join the translation table here at the silver layer so all
    downstream gold tables have English category names automatically.
    WHY join at silver and not gold?
      Every gold table that uses products needs English names.
      Joining once at silver means we don't repeat the join in
      every gold job — DRY principle applied to data pipelines.
    """
    
    print("\n Cleaning products...")

    df_products = read_csv(spark, "olist_products_dataset.csv")

    df_translation = read_csv(spark, "product_category_name_translation.csv")
    
    df_clean = (
        df_products
        .dropDuplicates(["product_id"])
        .filter(F.col("product_id").isNotNull())
        .withColumn("product_weight_g", F.col("product_weight_g").cast(DoubleType()))
        .withColumn("product_length_cm", F.col("product_length_cm").cast(DoubleType()))
        .withColumn("product_height_cm", F.col("product_height_cm").cast(DoubleType()))
        .withColumn("product_width_cm", F.col("product_width_cm").cast(DoubleType()))
        .withColumn("product_photos_qty", F.col("product_photos_qty").cast(IntegerType()))
        # Join translation — LEFT JOIN keeps products with no category
        .join(F.broadcast(df_translation), on="product_category_name", how="left")
        # WHY broadcast? Translation table is tiny (~70 rows).
        # Broadcasting sends it to every Spark worker — avoids shuffle.
        .withColumn("category_english", F.coalesce(
            F.col("product_category_name_english"),
            F.col("product_category_name"),     #fallback to Portuguese if no translation
            F.lit("unknown")
        ))
        .withColumn("processed_at", F.current_timestamp())
        .drop("product_category_name_english")
    )

    print(f"  Products: {df_clean.count()} rows after cleaning")
    write_silver(df_clean, "products")
    return df_clean

# ─────────────────────────────────────────────────────────────
# CLEAN: PAYMENTS
# ─────────────────────────────────────────────────────────────

def clean_payments(spark):
    """
    One order can have MULTIPLE payment rows — e.g., someone pays
    partly by credit card and partly by voucher.
    payment_sequential tells you the order of payments for one order.
    For analytics we keep all rows (one per payment method per order).
    """

    print("\n Cleaning payments...")

    df = read_csv(spark, "olist_order_payments_dataset.csv")

    df_clean = (
        df
        .dropDuplicates(["order_id", "payment_sequential"])
        .filter(F.col("order_id").isNotNull())
        .withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
        .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
        .withColumn("payment_sequential", F.col("payment_sequential").cast(IntegerType()))
        .withColumn("processed_at", F.current_timestamp())
    )

    print(f"  Payments: {df_clean.count()} rows after cleaning")
    write_silver(df_clean, "payments")
    return df_clean

# ─────────────────────────────────────────────────────────────
# CLEAN: SELLERS
# ─────────────────────────────────────────────────────────────

def clean_sellers(spark):
    print("\n Cleaning sellers...")

    df = read_csv(spark, "olist_sellers_dataset.csv")

    df_clean = (
        df
        .dropDuplicates(["seller_id"])
        .filter(F.col("seller_id").isNotNull())
        .withColumn("seller_city", F.initcap(F.trim(F.col("seller_city"))))
        .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
        .withColumn("processed_at", F.current_timestamp())
    )

    print(f"  Sellers:  {df_clean.count()} rows after cleaning")
    write_silver(df_clean, "sellers")
    return df_clean

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print(" Glue Job 01:  Raw ->  Silver")
    print(" Source: s3://retail-raw/olist/")
    print(" Target: s3://retail-silver/")
    print("=" * 55)

    ensure_buckets()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    clean_orders(spark)
    clean_customers(spark)
    clean_order_items(spark)
    clean_products(spark)
    clean_payments(spark)
    clean_sellers(spark)

    print("\n" + "=" * 55)
    print(" Raw:  ->  Silver complete")
    print(" Next: run 02_silver_to_gold.py")
    print("=" * 55)

    spark.stop()

