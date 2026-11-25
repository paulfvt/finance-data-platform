"""
Datasets Airflow partagés entre les DAGs (planification pilotée par les données).

Quand une tâche qui déclare un Dataset en `outlets` réussit, Airflow lance les
DAGs qui l'ont en `schedule`. Bronze n'a donc plus besoin de connaître Silver,
ni Silver de connaître Gold : chaque couche dit seulement ce qu'elle produit
et ce qu'elle consomme.
"""

from airflow.datasets import Dataset

BRONZE = Dataset("file:///opt/airflow/data/bronze")
SILVER = Dataset("file:///opt/airflow/data/silver")
GOLD = Dataset("file:///opt/airflow/data/gold")
