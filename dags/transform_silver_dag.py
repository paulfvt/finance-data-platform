"""
DAG Airflow, transformation Silver quotidienne (nettoyage, rendements,
moyennes mobiles) a partir des données Bronze.
"""

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")
from src.transform import run_transformation  # noqa: E402

default_args = {
    "owner": "paul",
    "retries": 2,
    "retry_delay": 180,
}

with DAG(
    dag_id="transform_silver_daily",
    description="Nettoyage et calcul de métriques (Silver) à partir des données Bronze",
    default_args=default_args,
    schedule=None,  # déclenché uniquement par le DAG Bronze, pas de planning propre
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["silver", "transformation"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_all_tickers",
        python_callable=run_transformation,
    )