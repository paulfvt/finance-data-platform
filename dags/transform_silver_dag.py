"""
DAG Airflow — transformation Silver quotidienne (nettoyage, rendements,
moyennes mobiles) à partir des données Bronze.
"""

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")
from src.transform import run_transformation  # noqa: E402


with DAG(
    dag_id="transform_silver_daily",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    transform_task = PythonOperator(
        task_id="transform_all_tickers",
        python_callable=run_transformation,
    )