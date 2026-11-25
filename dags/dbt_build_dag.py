"""
DAG dbt : construit Silver et Gold à partir de Bronze, avec leurs tests.

Lancé dès qu'une extraction Bronze réussit (Dataset). Une seule commande,
`dbt build`, enchaîne dans l'ordre : chargement du référentiel des actifs,
staging et ses contrôles d'entrée, Silver, Gold, contrôles de sortie. Si un
contrôle d'entrée échoue, Silver et Gold ne sont pas réécrits, et la tâche
échoue dans Airflow (dbt sort avec un code non nul).

Les tests unitaires dbt (logique des modèles sur des données simulées) sont
exclus ici : ils vérifient le code, pas les données, et tournent dans la CI.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from pipeline_datasets import BRONZE, GOLD, SILVER

default_args = {
    "owner": "paul",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="dbt_build",
    description="Silver et Gold par dbt (modèles + tests de qualité)",
    default_args=default_args,
    schedule=[BRONZE],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "gold", "dbt"],
) as dag:

    BashOperator(
        task_id="dbt_build",
        # Variables DBT_* et DATA_DIR : voir docker-compose.yml
        bash_command="cd /opt/airflow/dbt && dbt build --no-use-colors --exclude-resource-type unit_test",
        outlets=[SILVER, GOLD],
    )
