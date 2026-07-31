"""
DAG Airflow, squelette minimal pour verifier qu'Airflow detecte bien le DAG.
"""

import sys

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


sys.path.append("/opt/airflow")
from src.extract import run_extraction  # noqa: E402


with DAG(
    dag_id="extract_bronze_daily",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_all_tickers",
        python_callable=run_extraction,
    )