from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "grupo_fyhbd",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="lakehouse_stackoverflow",
    description="Pipeline Medallion StackOverflow: Bronze -> Silver -> Gold",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["FYHBD", "Lakehouse", "StackOverflow", "Medallion"],
) as dag:

    # -----------------------------------------------------------
    # 1. BRONZE
    # StackOverflow -> DLT -> Parquet -> MinIO
    # Solo users 2021 se ejecuta por Airflow.
    # -----------------------------------------------------------
    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command="""
        set -e
        echo "=== BRONZE: users 2021 ==="

        python3 /opt/airflow/scripts/bronze_ingest.py \
            --dataset users \
            --year 2021

        echo "=== BRONZE FINALIZADO ==="
        """,
    )

    # -----------------------------------------------------------
    # 2. SILVER
    # Bronze -> PySpark -> Iceberg MERGE -> Nessie
    # Construye users_hist con 2020 + 2021.
    # -----------------------------------------------------------
    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command="""
        set -e
        echo "=== SILVER: users_hist ==="

        spark-submit \
            /opt/airflow/scripts/silver_transform.py \
            --table users_hist \
            --years 2020,2021

        echo "=== SILVER FINALIZADO ==="
        """,
    )

    # -----------------------------------------------------------
    # 3. GOLD
    # Silver -> PySpark -> agregaciones -> Iceberg MERGE
    # Genera la tabla del pipeline: user_engagement.
    # -----------------------------------------------------------
    gold_agg = BashOperator(
        task_id="gold_agg",
        bash_command="""
        set -e
        echo "=== GOLD: user_engagement ==="

        spark-submit \
            /opt/airflow/scripts/gold_agg.py \
            --tables user_engagement

        echo "=== GOLD FINALIZADO ==="
        """,
    )

    # Dependencias del pipeline
    bronze_ingest >> silver_transform >> gold_agg