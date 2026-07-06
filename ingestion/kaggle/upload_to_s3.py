"""
Kaggle → S3 Raw Layer Ingestion Script
----------------------------------------
WHY THIS EXISTS:
  In production, raw data arrives from source systems automatically
  (via CDC, SFTP drops, or API feeds). For this project, Olist data
  was a one-time Kaggle download. This script simulates the "landing"
  step — taking source files and placing them in the raw S3 layer
  exactly as received, with no transformation.

WHY WE DON'T TRANSFORM HERE:
  Raw layer = source of truth. We never modify what landed here.
  If our ETL has a bug, we reprocess from raw. If we transformed
  during upload, we'd lose the original data forever.

WHY BOTO3 AND NOT AWS CLI:
  AWS CLI newer versions send x-amz-trailer checksum headers that
  LocalStack free tier doesn't support. Boto3 gives us full control
  over the request — no checksum headers by default.
  In production against real AWS, both work identically.
"""

import boto3
import os
import sys
from datetime import datetime

# ── Config ────────────────────────────────────────────────────
ENDPOINT_URL = os.getenv("AWS_ENDOINT_URL", "http://localhost:4566")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
RAW_BUCKET = os.getenv("RAW_BUCKET", "retail-raw")
DATA_DIR = os.path.join(os.path.dirname(__file__), "../../data/raw")

# ── File mapping: local path → S3 key ─────────────────────────
# WHY organised into subfolders by entity?
#   S3 doesn't have real folders — the "/" in the key is just
#   part of the name. But organising by entity means:
#   1. You can set different IAM policies per prefix
#   2. Glue crawlers can be pointed at one entity at a time
#   3. It mirrors how a real data lake is partitioned

FILES = {
    "olist_orders_dataset.csv":                "olist/orders/olist_orders_dataset.csv",
    "olist_order_items_dataset.csv":           "olist/order_items/olist_order_items_dataset.csv",
    "olist_customers_dataset.csv":             "olist/customers/olist_customers_dataset.csv",
    "olist_products_dataset.csv":              "olist/products/olist_products_dataset.csv",
    "olist_order_payments_dataset.csv":        "olist/payments/olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv":         "olist/reviews/olist_order_reviews_dataset.csv",
    "olist_sellers_dataset.csv":               "olist/sellers/olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv":           "olist/geolocation/olist_geolocation_dataset.csv",
    "product_category_name_translation.csv":   "olist/category_translation/product_category_name_translation.csv",
}

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url = ENDPOINT_URL,
        aws_access_key_id = AWS_KEY,
        aws_secret_access_key = AWS_SECRET,
        region_name = AWS_REGION
    )

def upload_files(s3, dry_run=False):
    success, failed = [], []

    for filename, s3_key in FILES.items():
        local_path = os.path.join(DATA_DIR, filename)

        if not os.path.exists(local_path):
            print(f"    SKIP    {filename} - file not found locally")
            continue

        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        print(f"    Uploading {filename} ({size_mb:.1f} MB)...")

        if not dry_run:
            try:
                s3.upload_file(local_path, RAW_BUCKET, s3_key)
                print(f"    OK  -> s3://{RAW_BUCKET}/{s3_key}")
                success.append(filename)
            except Exception as e:
                print(f"    FAIL    -> {e}")
                failed.append(filename)
        else:
            print(f"    DRY RUN -> s3://{RAW_BUCKET}/{s3_key}")
            success.append(filename)
    
    return success, failed

def verify_upload(s3):
    print("\n   Verifying uploads...")
    response = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix="olist/")
    objects = response.get("Contents: ", [])

    for obj in objects:
        size_kb = obj["Size"] / 1024
        print(f"    {obj["Key"]} ({size_kb:.1f} KB)")
    
    print(f"\n Total files in s3://{RAW_BUCKET}/olist/: {len(objects)}")

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv

    print("=" * 55)
    print("Olist Raw Data -> S3 Ingestion")
    print(f"Target : s3://{RAW_BUCKET}/olist/")
    print(f"Endpoint : {ENDPOINT_URL}")
    print(f"Mode : {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    s3 = get_s3_client()
    success, failed = upload_files(s3, dry_run=dry_run)
    verify_upload(s3)

    print()
    if failed:
        print(f"FAILED: {len(failed)} files - {failed}")
        sys.exit(1)
    else:
        print(f"ALL {len(success)} file uploaded successfully.")
