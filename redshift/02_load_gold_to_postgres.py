"""
Load Gold Layer -> Postgres (Redshift substitute)
WHY load Parquet into Postgres and not query Parquet directly?
  In production, Redshift stores data in its own columnar format
  with DISTKEY/SORTKEY for fast repeated queries.
  Postgres with indexes is the closest free substitute.
  The loading pattern (stage → validate → swap) is identical
  to how you'd use Redshift COPY command in production.
"""

import os
import pandas as pd
import pg8000
import pyarrow.parquet as pq


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

GOLD_DIR ="/home/glue_user/workspace/data/gold"

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "retail_dw",
    "user": "admin",
    "password": "admin"
}

TABLES = [
    "dim_date",
    "dim_customers",
    "dim_products",
    "dim_sellers",
    "fact_orders",
    "revenue_by_category",
    "customer_lifetime_value",
    "seller_performance",
]

def get_conn():
    return pg8000.connect(
        host="postgres",
        port=5432,
        database = "retail_dw",
        user = "admin",
        password = "admin"
    )


def read_parquet(table_name):
    """
    Read all Parquet files for a table into a Pandas DataFrame.
    WHY Pandas here and not Spark?
      Loading into Postgres is a single-node operation —
      no need for distributed processing. Pandas is simpler
      and faster for this small final hop.
    """

    path = os.path.join(GOLD_DIR, table_name)
    dataset = pq.read_table(path)
    df = dataset.to_pandas()
    # Replace NaN with None so psycopg2 writes NULL not NaN
    df = df.where(pd.notnull(df), None)
    return df

def load_staging(conn, table_name, df):
    """
    Load DataFrame into staging table using fast batch insert.
    WHY execute_values and not df.to_sql()?
      execute_values sends all rows in one round trip to Postgres.
      to_sql() with psycopg2 does one INSERT per row — 100x slower
      for 100K rows.
    """

    cur = conn.cursor()
    cur.execute(f"TRUNCATE staging.{table_name}")

    cols = list(df.columns)
    col_str = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    query = f"INSERT INTO staging.{table_name} ({col_str}) VALUES ({placeholders})"
    values = [tuple(None if pd.isna(v) else v for v in row)
              for row in df.itertuples(index=False, name=None)]

    cur.executemany(query, values)
    conn.commit()
    cur.close()
    return len(values)

def validate_staging(conn, table_name, expected_min_rows):
    """
    Check staging has enough rows before swapping to warehouse.
    WHY validate before swap?
      If Parquet read fails silently and staging gets 0 rows,
      without validation we'd DELETE all warehouse data and
      INSERT nothing — wiping production. This check prevents that.
    """

    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM staging.{table_name}")
    count = cur.fetchone()[0]
    cur.close()

    if count < expected_min_rows:
        raise ValueError(
            f"VALIDATION FAILED: staging.{table_name} has {count} rows, "
            f"expected >= {expected_min_rows}. Abortinf swap"
        )
    return count

def swap_to_warehouse(conn, table_name):
    """
    Atomic swap: DELETE warehouse + INSERT from staging in one transaction.
    WHY atomic?
      Without a transaction, if DELETE succeeds but INSERT fails,
      warehouse table is empty — analysts get no data.
      A transaction is all-or-nothing: both succeed or neither happens.
    WHY DELETE + INSERT and not TRUNCATE + INSERT?
      TRUNCATE in Postgres is not fully transactional in all cases.
      DELETE is always transactional — safer for atomic swaps.
    """

    cur = conn.cursor()
    cur.execute(f"DELETE FROM warehouse.{table_name}")
    cur.execute(f"""
                INSERT INTO warehouse.{table_name}
                SELECT * FROM staging.{table_name}
                """)
    conn.commit()
    cur.close()

def get_min_rows(table_name): 
    mins = {
        "dim_date":             1400,
        "dim_customers":        90000,
        "dim_products":         30000,
        "dim_sellets":          3000,
        "fact_orders":          100000,
        "revenue_by_category":  100,
        "customer_lifetime_value":  90000,
        "seller_performance":   10000,
    }

    return mins.get(table_name, 1)


if __name__ == "__main__":
    print("-" * 55)
    print("\n   Loading Gold -> Postgres Warehouse")
    print("=" * 55)

    conn = get_conn()

    for table in TABLES:
        print(f"\n {table}")

        # STEP 1: Read gold Parquet
        print(f"    Reading Parquet...")
        df = read_parquet(table)
        print(f"    {len(df)} rows read")

        # Step 2: Load into staging
        print(f"    Loading staging...")
        rows = load_staging(conn, table, df)
        print(f"    {rows} rows loaded to staging")

        # Step 3: Validate
        print(f"    Validating...")
        count = validate_staging(conn, table, get_min_rows(table))
        print(f"    Validation passed: {count} rows")

        # Step 4: Atomic swap to warehouse
        print(f"    Swapping to warehouse")
        swap_to_warehouse(conn, table)
        print(f"    Done -> warehouse.{table}")

    conn.close()

    print("\n" + "-" * 55)
    print(" Gold -> Postgres complete")
    print(" Next: Airflow DAG to orchestrate everthing")
    print("-" * 55)

