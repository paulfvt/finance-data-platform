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
    assert set(dagbag.dag_ids) == {"bronze_extract", "silver_transform", "gold_aggregate"}


def test_bronze_is_scheduled_by_airflow(dagbag):
    dag = dagbag.get_dag("bronze_extract")
    assert dag.schedule_interval == "@hourly"
    assert dag.catchup is False


def _trigger_uris(dag) -> list[str]:
    # Depuis Airflow 2.9, dataset_triggers est une condition (DatasetAll), plus une liste.
    return sorted(uri for uri, _ in dag.dataset_triggers.iter_datasets())


def _outlet_uris(dag) -> list[str]:
    return sorted(d.uri for d in dag.tasks[0].outlets)


def test_layers_are_chained_by_datasets(dagbag):
    bronze = dagbag.get_dag("bronze_extract")
    silver = dagbag.get_dag("silver_transform")
    gold = dagbag.get_dag("gold_aggregate")

    assert _trigger_uris(silver) == _outlet_uris(bronze)
    assert _trigger_uris(gold) == _outlet_uris(silver)
