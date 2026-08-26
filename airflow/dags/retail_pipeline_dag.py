"""
Airflow DAG: Retail Pipeline — Full Orchestration
===================================================
WHAT THIS DAG DOES:
  Runs the entire retail data pipeline end-to-end every day at 2 AM.
  Each task only starts if the previous one succeeded.
  If any task fails, Airflow retries it and alerts the team.
WHY AIRFLOW and not just running scripts manually?
  - Dependency management: task B won't run if task A failed
  - Automatic retries: transient failures (network, memory) are handled
  - Full history: every run is logged — you can see what ran, when, and why it failed
  - Backfill: if pipeline was down for 3 days, rerun those 3 days in order
  - SLA monitoring: alert if a task takes longer than expected
INTERVIEW POINT — WHY NOT CRON JOBS?
  Cron has no dependency management. If your 2 AM cleaning job fails,
  the 3 AM loading job still runs — on incomplete data. Silent corruption.
  Airflow stops the chain at the failed task and alerts you immediately.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# ─────────────────────────────────────────────────────────────
# DEFAULT ARGS — applied to every task unless overridden
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner" : "data-engineering",
    "depends_on_past" : False,
    # WHY depends_on_past=False?
    #   If True, today's run won't start unless yesterday's succeeded.
    #   Useful when data has temporal dependencies (today needs yesterday's output).
    #   We set False here so we can rerun any day independently.
    "start_date" : datetime(2024, 1, 1),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=3),
    "retry_exponentional_backoff" : True,
    # WHY exponential backoff?
    #   Retry 1: wait 3 min. Retry 2: wait 6 min.
    #   Avoids hammering a struggling service with immediate retries.
    "email_on_failure" : False,      # set True in production with real email
    "email_on_retry" : False,
}

# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id = 'retail_pipeline_daily',
    default_args = default_args,
    description = "Daily ETL: RAW CSV -> Silver -> Gold -> Postgres Warehouse",
    schedule_interval = "0 2 * * *",
    # WHY 2 AM?
    #   Source data finishes loading by midnight.
    #   We give 2 hours buffer. Pipeline runs at 2 AM,
    #   dashboards show fresh data when analysts arrive at 9 AM.
    catchup = False,
    # WHY catchup=False?
    #   If DAG was paused for 30 days and re-enabled,
    #   catchup=True would trigger 30 runs simultaneously — dangerous.
    #   catchup=False runs from now forward only.
    tags = ["retail", "etl", "daily"],
    max_active_runs = 1,
    # WHY max_active_runs=1?
    #   Prevents two concurrent runs writing to the same S3 paths
    #   and Postgres tables simultaneously — would cause data corruption.
) as dag:
    
    # ─────────────────────────────────────────────
    # TASK 1: Ensure S3 buckets exist
    # WHY this as a separate task?
    #   LocalStack loses state on restart. This task is idempotent —
    #   safe to run every time, creates buckets only if missing.
    #   In real AWS, buckets persist — but this pattern of
    #   "ensure infrastructure before running" is good practice.
    # ─────────────────────────────────────────────

    def ensure_buckets(**context):
        import boto3
        import os

        endpoint = os.getenv("AWS_ENDPOINt_URL", "http://localstack:4566")

        s3 = boto3.client(
            "s3",
            endpoint_url = endpoint,
            aws_access_key_id = "test",
            aws_secret_access_key = "test",
            region_name = "us-east-1",
        )

        for bucket in ["retail-raw", "retail-silver", "retail-gold", "retail-scripts"]:
            try:
                s3.head_bucket(Bucket = bucket)
                print(f"    Bucket exists:  {bucket}")
            except Exception:
                s3.create_bucket(Bucket = bucket)
                print(f"    Created bucket: {bucket}")
        
    task_ensure_buckets = PythonOperator(
        task_id = "ensure_s3_buckets",
        python_callable = ensure_buckets,
    )

    # ─────────────────────────────────────────────
    # TASK 2: Upload raw data to S3
    # In production this would be replaced by an S3 sensor
    # that waits for the upstream system to drop files.
    # Here we upload from local data directory.
    # ─────────────────────────────────────────────

    def upload_raw(**context):
        import boto3
        import os

        endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
        )

        data_dir = "/opt/airflow/data/raw"

        file_map = {
            "olist_orders_dataset.csv":              "olist/orders/olist_orders_dataset.csv",
            "olist_order_items_dataset.csv":         "olist/order_items/olist_order_items_dataset.csv",
            "olist_customers_dataset.csv":           "olist/customers/olist_customers_dataset.csv",
            "olist_products_dataset.csv":            "olist/products/olist_products_dataset.csv",
            "olist_order_payments_dataset.csv":      "olist/payments/olist_order_payments_dataset.csv",
            "olist_order_reviews_dataset.csv":       "olist/reviews/olist_order_reviews_dataset.csv",
            "olist_sellers_dataset.csv":             "olist/sellers/olist_sellers_dataset.csv",
            "olist_geolocation_dataset.csv":         "olist/geolocation/olist_geolocation_dataset.csv",
            "product_category_name_translation.csv": "olist/category_translation/product_category_name_translation.csv",
        }

        for filename, s3_key in file_map.items():
            local_path = os.path.join(data_dir, filename)
            if os.path.exists(local_path):
                s3.upload_file(local_path, "retail-raw", s3_key)
                print(f"    Uploaded:   {filename}")
            else:
                print(f"    SKIP (not found) :  {filename}")
            
    task_upload_raw = PythonOperator(
        task_id = "upload_raw_to_s3",
        python_callable = upload_raw
    )

    # ─────────────────────────────────────────────
    # TASK 3: Run Raw → Silver Glue job
    # WHY BashOperator here?
    #   In production you'd use GlueJobOperator from
    #   apache-airflow-providers-amazon to trigger a real AWS Glue job.
    #   Locally we run the PySpark script directly in the Glue container.
    #   The script is identical — only the executor changes.
    # ─────────────────────────────────────────────

    def run_raw_to_silver(**context):
        import subprocess
        result = subprocess.run(
            ["python3", "/opt/airflow/glue_jobs/01_raw_to_silver.py"],
            capture_output = True, text = True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)

    task_raw_to_silver = PythonOperator(
        task_id = "raw_to_silver",
        python_callable = run_raw_to_silver,
    )
        # WHY docker exec from inside Airflow container?
        #   Airflow and Glue are separate containers. Airflow triggers
        #   the Glue container via Docker socket. In real AWS, this would
        #   be an API call to start a Glue Job Run.

    # ─────────────────────────────────────────────
    # TASK 4: Run Silver → Gold Glue job
    # ─────────────────────────────────────────────

    def run_silver_to_gold(**context):
        import subprocess
        result = subprocess.run(
            ["python3", "/opt/airflow/glue_jobs/02_silver_to_gold.py"],
            capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)

    task_silver_to_gold = PythonOperator(
        task_id = 'silver_to_gold',
        python_callable = run_silver_to_gold,
    )

    # ─────────────────────────────────────────────
    # TASK 5: Load Gold → Postgres
    # ─────────────────────────────────────────────

    def run_load_postgres(**context):
        import subprocess
        result = subprocess.run(
            ["python3", "/opt/airflow/ingestion/02_load_gold_to_postgres.py"],
            capture_output=True, text=True
        )
        print(result.stdout)
        if result.returncode != 0:
            raise Exception(result.stderr)

    task_load_to_postgres = PythonOperator(
        task_id = "load_to_postgres",
        python_callable = run_load_postgres,
    )

    # ─────────────────────────────────────────────
    # TASK 6: Data Quality Checks
    # WHY a separate DQ task after loading?
    #   Defense in depth. Glue jobs validate during transformation.
    #   This task validates AFTER loading — checks the warehouse
    #   has the expected row counts and no nulls in key columns.
    #   If this fails, you know the issue is in the load step,
    #   not the transformation step. Easier debugging.
    # ─────────────────────────────────────────────

    def run_dq_checks(**context):
        import pg8000

        conn = pg8000.connect(
            host = "postgres",
            port = 5432,
            database = "retail_dw",
            user = "admin",
            password = "admin",
        )

        cur = conn.cursor()

        checks = [
            # (description, query, minimum expected value)
            ("fact_orders row count", "SELECT COUNT(*) FROM warehouse.fact_orders", 100000),
            ("dim_customers row count", "SELECT COUNT(*) FROM warehouse.dim_customers", 90000),
            ("fact_orders null customer check", 
             "SELECT COUNT(*) FROM warehouse.fact_orders WHERE customer_unique_id IS NULL", None), # None means expect 0
            ("fact_orders null product check", 
             "SELECT COUNT(*) FROM warehouse.fact_orders WHERE product_id IS NULL", None),
             ("dim_date competeness", "SELECT COUNT(*) FROM warehouse.dim_date", 1400),
        ]

        failed = []
        for description, query, expected_min in checks:
            cur.execute(query)
            result = cur.fetchone()[0]

            if expected_min is None:
                # Expected zero (null check)
                if result > 0:
                    failed.append(f"FAIL:   {description} _ found {result} nulls, expected 0")
                else:
                    print(f"    PASS:   {description} (0 nulls)")
            else:
                if result < expected_min:
                    failed.append(f"FAIL:   {description} - got {result}, expected >= {expected_min}")
                else:
                    print(f"    PASS:   {description} ({result} rows)")
        
        cur.close()
        conn.close()

        if failed:
            raise ValueError("Data quality checks failed:\n" + "\n".join(failed))
        
        print("\nALL DQ checks passed.")

    task_dq_checks = PythonOperator(
        task_id = "data_quality_checks",
        python_callable = run_dq_checks,
    )

    # ─────────────────────────────────────────────
    # TASK 7: Notify success
    # In production: SlackWebhookOperator or EmailOperator
    # ─────────────────────────────────────────────

    task_notify = BashOperator(
        task_id = "notify_success",
        bash_command = (
            "echo '===============================================' && " \
            "echo 'Pipeline competed:   {{ds}}' && " \
            "echo 'fact_order, dim_customers, dim_products' && " \
            "echo 'All loaded to warehouse. DQ checks passed.' && " \
            "echo '==============================================='"
        ),
        trigger_rule = TriggerRule.ALL_SUCCESS,
        # WHY ALL_SUCCESS?
        #   Only notify if EVERY upstream task succeeded.
        #   Don't send a success notification if DQ checks failed.
    )

    # ─────────────────────────────────────────────
    # DEPENDENCY CHAIN
    # Read >> as "then run"
    # ──────────────────────────────────

    (
        task_ensure_buckets
        >> task_upload_raw
        >> task_raw_to_silver
        >> task_silver_to_gold
        >> task_load_to_postgres
        >> task_dq_checks
        >> task_notify
    )


        
