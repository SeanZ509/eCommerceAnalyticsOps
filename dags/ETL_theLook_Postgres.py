from __future__ import annotations

import os
import glob
import csv
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException

import psycopg2


# --- Config you can leave alone ---
DATA_DIR = Path(os.environ.get("AIRFLOW_DATA_DIR", "/opt/airflow/data"))
RAW_DIR = DATA_DIR / "raw" / "thelook"
KAGGLE_DATASET = "daichiuchigashima/thelook-ecommerce"  # small, multi-table, perfect for KPIs :contentReference[oaicite:0]{index=0}

# Postgres service name is "postgres" in docker compose
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_USER = os.environ.get("PG_USER", "gpi")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "gpi")

AIRFLOW_DB = os.environ.get("AIRFLOW_PG_DB", "airflow")      # Airflow metadata DB
ANALYTICS_DB = os.environ.get("ANALYTICS_PG_DB", "ecommerce") # Your project analytics DB


def pg_connect(dbname: str):
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=dbname,
    )


def _identifier(name: str) -> str:
    # very simple "safe enough" identifier quoting
    return '"' + name.replace('"', '""') + '"'


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_csv_header(path: str) -> list[str]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    # normalize
    return [h.strip().lower().replace(" ", "_") for h in header]


def create_raw_table_from_header(cur, table: str, header: list[str]) -> None:
    cols_sql = ", ".join(f"{_identifier(c)} TEXT" for c in header)
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS raw;')
    cur.execute(f'DROP TABLE IF EXISTS raw.{_identifier(table)};')
    cur.execute(f'CREATE TABLE raw.{_identifier(table)} ({cols_sql});')


def copy_csv_into_table(cur, table: str, path: str, header: list[str]) -> None:
    # COPY expects the same column order as the file
    cols_sql = ", ".join(_identifier(c) for c in header)
    with open(path, "r", encoding="utf-8", newline="") as f:
        cur.copy_expert(
            f"COPY raw.{_identifier(table)} ({cols_sql}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE '\"', ESCAPE '\"')",
            f
        )


def table_columns(cur, schema: str, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {r[0] for r in cur.fetchall()}


with DAG(
    dag_id="etl_thelook_to_postgres",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["ecommerce", "thelook", "postgres"],
) as dag:

    @task
    def ensure_kaggle_creds():
        """
        Expect secrets/kaggle.json mounted at /opt/airflow/secrets/kaggle.json (read-only).
        Copy to ~/.kaggle/kaggle.json so the kaggle CLI works.
        """
        secrets_path = Path("/opt/airflow/secrets/kaggle.json")
        if not secrets_path.exists():
            raise AirflowFailException(
                "Missing /opt/airflow/secrets/kaggle.json. Put your Kaggle API token at repo/secrets/kaggle.json (DO NOT COMMIT)."
            )

        kaggle_dir = Path("/home/airflow/.kaggle")
        ensure_dir(kaggle_dir)

        target = kaggle_dir / "kaggle.json"
        # copy file contents (works even if source is read-only mount)
        target.write_bytes(secrets_path.read_bytes())
        os.chmod(target, 0o600)  # kaggle requires strict perms

        return str(target)

    @task
    def ensure_postgres_db_and_schemas():
        """
        Creates ecommerce DB if missing, plus raw/analytics schemas.
        Connects to AIRFLOW_DB first because it is guaranteed to exist.
        """
        with pg_connect(AIRFLOW_DB) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (ANALYTICS_DB,))
                exists = cur.fetchone() is not None
                if not exists:
                    cur.execute(f'CREATE DATABASE {_identifier(ANALYTICS_DB)};')

        with pg_connect(ANALYTICS_DB) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
                cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

        return True

    @task
    def download_and_extract():
        """
        Downloads theLook dataset via Kaggle CLI and unzips into /opt/airflow/data/raw/thelook
        """
        ensure_dir(RAW_DIR)

        # Download+unzip directly into RAW_DIR
        cmd = [
            "kaggle", "datasets", "download",
            "-d", KAGGLE_DATASET,
            "-p", str(RAW_DIR),
            "--unzip",
            "--force",
        ]
        subprocess.run(cmd, check=True)

        csvs = sorted(glob.glob(str(RAW_DIR / "*.csv")))
        if not csvs:
            raise AirflowFailException(f"No CSV files found after download in {RAW_DIR}")
        return csvs

    @task
    def load_csvs_to_postgres(csv_paths: list[str]):
        """
        Creates raw.<table> for each csv and COPY loads it (all columns TEXT for reliability).
        """
        with pg_connect(ANALYTICS_DB) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for path in csv_paths:
                    table = Path(path).stem.lower().replace(" ", "_")
                    header = read_csv_header(path)
                    create_raw_table_from_header(cur, table, header)
                    copy_csv_into_table(cur, table, path, header)

        return {"tables_loaded": [Path(p).stem for p in csv_paths]}

    @task
    def build_kpi_views():
        """
        Creates starter KPI views using whatever columns exist.
        (We keep raw as TEXT; views cast when possible.)
        """
        with pg_connect(ANALYTICS_DB) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # detect columns
                oi_cols = table_columns(cur, "raw", "order_items") if True else set()
                o_cols = table_columns(cur, "raw", "orders") if True else set()
                p_cols = table_columns(cur, "raw", "products") if True else set()

                # choose date column (prefer order_items.created_at then orders.created_at)
                if "created_at" in oi_cols:
                    date_expr = "DATE(order_items.created_at::timestamp)"
                    from_sql = "raw.order_items AS order_items"
                elif "created_at" in o_cols:
                    date_expr = "DATE(orders.created_at::timestamp)"
                    from_sql = "raw.orders AS orders"
                else:
                    # no timestamps? bail gracefully
                    cur.execute("DROP VIEW IF EXISTS analytics.kpi_daily;")
                    cur.execute("DROP VIEW IF EXISTS analytics.category_daily;")
                    return "No created_at column found; skipped KPI views."

                revenue_expr = "SUM(COALESCE(order_items.sale_price::numeric, 0))" if "sale_price" in oi_cols else "COUNT(*)::numeric"
                orders_expr = "COUNT(DISTINCT order_items.order_id)" if "order_id" in oi_cols else "COUNT(*)"
                items_expr = "COUNT(*)"

                # KPI Daily
                if "order_items" in from_sql:
                    cur.execute("""
                        DROP VIEW IF EXISTS analytics.kpi_daily;
                        CREATE VIEW analytics.kpi_daily AS
                        SELECT
                            {date_expr} AS order_date,
                            {orders_expr} AS orders,
                            {items_expr} AS items,
                            {revenue_expr} AS revenue,
                            CASE WHEN {orders_expr} > 0 THEN {revenue_expr} / {orders_expr} ELSE NULL END AS aov
                        FROM raw.order_items AS order_items
                        GROUP BY 1
                        ORDER BY 1;
                    """.format(date_expr=date_expr, orders_expr=orders_expr, items_expr=items_expr, revenue_expr=revenue_expr))
                else:
                    cur.execute("""
                        DROP VIEW IF EXISTS analytics.kpi_daily;
                        CREATE VIEW analytics.kpi_daily AS
                        SELECT
                            {date_expr} AS order_date,
                            COUNT(*) AS orders
                        FROM raw.orders AS orders
                        GROUP BY 1
                        ORDER BY 1;
                    """.format(date_expr=date_expr))

                # Category daily (if we have products + order_items.product_id)
                if "product_id" in oi_cols and "id" in p_cols and "category" in p_cols and "sale_price" in oi_cols:
                    cur.execute("""
                        DROP VIEW IF EXISTS analytics.category_daily;
                        CREATE VIEW analytics.category_daily AS
                        SELECT
                            DATE(oi.created_at::timestamp) AS order_date,
                            p.category AS category,
                            COUNT(*) AS items,
                            SUM(COALESCE(oi.sale_price::numeric, 0)) AS revenue
                        FROM raw.order_items oi
                        JOIN raw.products p ON p.id = oi.product_id
                        GROUP BY 1,2
                        ORDER BY 1,2;
                    """)
                else:
                    cur.execute("DROP VIEW IF EXISTS analytics.category_daily;")

        return "Views created: analytics.kpi_daily (and analytics.category_daily if columns exist)."

    creds = ensure_kaggle_creds()
    db_ok = ensure_postgres_db_and_schemas()
    csvs = download_and_extract()
    loaded = load_csvs_to_postgres(csvs)
    views = build_kpi_views()

    creds >> db_ok >> csvs >> loaded >> views