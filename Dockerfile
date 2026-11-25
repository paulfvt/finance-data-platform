# Image Airflow du pipeline : les dépendances sont installées une fois au build,
# et non plus à chaque démarrage du conteneur (lent, et impossible sans réseau).
FROM apache/airflow:2.9.3-python3.11

COPY requirements-airflow.txt /tmp/requirements-airflow.txt

# On réinstalle la même version d'Airflow à côté des dépendances du pipeline,
# pour que pip ne la remplace pas par une autre en résolvant les versions.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-airflow.txt

# dbt dans son propre environnement virtuel : dbt et Airflow épinglent des
# versions différentes des mêmes bibliothèques. Airflow appelle dbt en ligne
# de commande (/opt/dbt-venv/bin/dbt), comme n'importe quel outil externe.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
USER root
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
USER airflow
ENV PATH="${PATH}:/opt/dbt-venv/bin"
