"""
Tests d'intégrité des DAGs : ils se chargent sans erreur et s'enchaînent par Datasets.
Ne tournent que là où Airflow est installé (dans le conteneur) :

    docker compose run --rm airflow-scheduler python -m pytest tests -v
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_dags_load_without_errors(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dag_ids) == {"bronze_extract", "dbt_build"}


def test_bronze_is_scheduled_by_airflow(dagbag):
    dag = dagbag.get_dag("bronze_extract")
    assert dag.schedule_interval == "@hourly"
    assert dag.catchup is False


def _trigger_uris(dag) -> list[str]:
    # Depuis Airflow 2.9, dataset_triggers est une condition (DatasetAll), plus une liste.
    return sorted(uri for uri, _ in dag.dataset_triggers.iter_datasets())


def _outlet_uris(dag) -> list[str]:
    return sorted(d.uri for d in dag.tasks[0].outlets)


def test_dbt_is_triggered_by_bronze(dagbag):
    bronze = dagbag.get_dag("bronze_extract")
    dbt = dagbag.get_dag("dbt_build")

    assert _trigger_uris(dbt) == _outlet_uris(bronze)
    assert _outlet_uris(dbt) == ["file:///opt/airflow/data/gold", "file:///opt/airflow/data/silver"]


def test_dbt_task_runs_build_not_run(dagbag):
    """`dbt build` (modèles ET tests), jamais `dbt run` qui écrirait sans contrôler."""
    task = dagbag.get_dag("dbt_build").tasks[0]
    assert "dbt build" in task.bash_command
