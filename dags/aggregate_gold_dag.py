"""
DAG Gold : corrélations glissantes entre actifs à partir de Silver.
Lancé automatiquement dès qu'une transformation Silver réussit.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline_datasets import GOLD, SILVER
from src.aggregate import run_aggregation

default_args = {
    "owner": "paul",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="gold_aggregate",
    description="Corrélations glissantes et agrégats (Gold) à partir des données Silver",
    default_args=default_args,
    schedule=[SILVER],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "aggregation"],
) as dag:

    PythonOperator(
        task_id="compute_correlations",
        python_callable=run_aggregation,
        outlets=[GOLD],
    )
