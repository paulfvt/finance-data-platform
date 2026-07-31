"""
DAG Airflow — squelette minimal pour vérifier qu'Airflow détecte bien le DAG.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello from extract_bronze_daily")


with DAG(
    dag_id="extract_bronze_daily",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )