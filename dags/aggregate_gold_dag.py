"""
DAG Airflow — agrégation Gold quotidienne (corrélations glissantes)
à partir des données Silver.
"""

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")
from src.aggregate import run_aggregation  # noqa: E402

default_args = {
    "owner": "paul",
    "retries": 2,
    "retry_delay": 180,
}

with DAG(
    dag_id="aggregate_gold_daily",
    description="Corrélations glissantes et agrégats (Gold) à partir des données Silver",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gold", "aggregation"],
) as dag:

    aggregate_task = PythonOperator(
        task_id="compute_correlations",
        python_callable=run_aggregation,
    )