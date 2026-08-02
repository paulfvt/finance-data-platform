"""
DAG Airflow, squelette minimal pour verifier qu'Airflow detecte bien le DAG.
"""

import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.append("/opt/airflow")
from src.extract import run_extraction

default_args = {
    "owner": "paul",
    "retries": 3,
    "retry_delay": 300,  # secondes entre deux tentatives
}

with DAG(
    dag_id="extract_bronze_daily",
    description="Extraction quotidienne des cours de marche vers la couche Bronze",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["bronze", "extraction"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_all_tickers",
        python_callable=run_extraction,
    )

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transformation",
        trigger_dag_id="transform_silver_daily",
    )

    extract_task >> trigger_silver