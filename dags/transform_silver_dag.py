"""
DAG Silver : nettoyage, rendements et moyennes mobiles à partir de Bronze.
Lancé automatiquement dès qu'une extraction Bronze réussit.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline_datasets import BRONZE, SILVER
from src.transform import run_transformation

default_args = {
    "owner": "paul",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="silver_transform",
    description="Nettoyage et calcul de métriques (Silver) à partir des données Bronze",
    default_args=default_args,
    schedule=[BRONZE],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "transformation"],
) as dag:

    PythonOperator(
        task_id="transform_all_tickers",
        python_callable=run_transformation,
        outlets=[SILVER],
    )
