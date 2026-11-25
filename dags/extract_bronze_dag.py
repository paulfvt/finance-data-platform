"""
DAG Bronze : extraction des cours de marché, planifiée par Airflow.

Plus de déclencheur Windows : le DAG tourne toutes les heures. Avec
`catchup=False`, quand le PC sort de veille et que le scheduler redémarre,
Airflow lance tout de suite le dernier créneau manqué (un seul, pas tous).
L'extraction relit assez de jours en arrière pour combler le trou laissé
pendant que le PC était éteint (voir `compute_lookback_days`).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pipeline_datasets import BRONZE
from src.extract import run_extraction

default_args = {
    "owner": "paul",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_extract",
    description="Extraction des cours de marché vers la couche Bronze",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "extraction"],
) as dag:

    PythonOperator(
        task_id="extract_all_tickers",
        python_callable=run_extraction,
        outlets=[BRONZE],
    )
