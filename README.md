# Finance Data Platform

Pipeline de données financières (actions, matières premières, taux, cryptomonnaies) construit autour d'une architecture en couches (Bronze / Silver / Gold), avec extraction automatisée, nettoyage, et calcul d'indicateurs de marché (rendements, volatilité, corrélations inter-actifs).

Projet personnel réalisé dans le cadre de ma spécialisation Data & IA à l'ECE Paris, pour mettre en pratique des compétences de data engineering (orchestration, conteneurisation, modélisation de données) sur un sujet concret.

---

## Objectif

Suivre l'évolution de plusieurs classes d'actifs (actions, matières premières, taux d'intérêt, cryptomonnaies) et produire des indicateurs exploitables sur leurs dynamiques et leurs corrélations, sans intervention manuelle : le pipeline s'exécute de façon autonome, du téléchargement des données brutes jusqu'aux agrégats analytiques.

## Actifs suivis

- Actions : S&P 500, EuroStoxx 50
- Matières premières : Or, Pétrole
- Taux : US 10 ans
- Cryptomonnaies : Bitcoin, Ethereum

Un actif par grande classe, pour pouvoir observer des dynamiques et des corrélations réellement différentes plutôt que des variantes d'un même marché.

---

## Architecture

Le pipeline suit une architecture médaillon, standard en data engineering pour séparer clairement donnée brute et donnée transformée :

[APIs de marché] ─▶ [BRONZE : ingestion brute, Parquet partitionné par date]
│
▼
[SILVER : nettoyage, alignement calendaire,
rendements, moyennes mobiles]
│
▼
[GOLD : corrélations glissantes 30 jours,
agrégats entre actifs]

**Bronze** conserve les données telles que renvoyées par la source, sans transformation, en agrégeant une fenêtre glissante de quelques jours à chaque extraction — ça garantit de pouvoir toujours revenir à l'origine si une étape de traitement doit être revue.

**Silver** reconstitue l'historique complet à partir de tous les fichiers Bronze accumulés, nettoie (déduplication, valeurs aberrantes), et calcule rendements logarithmiques, moyennes mobiles (20/50j) et volatilité glissante (20j).

**Gold** croise tous les actifs suivis, aligne leurs calendriers (les marchés traditionnels ferment le week-end, la crypto trade en continu — géré par forward-fill), et calcule des corrélations glissantes sur 30 jours entre chaque paire d'actifs. Cette couche ne devient réellement significative qu'après ~30 jours d'historique accumulé.

## Orchestration

Le pipeline est orchestré par Airflow, avec un enchaînement automatique des trois couches : `extract_bronze_daily` déclenche `transform_silver_daily` à sa réussite, qui déclenche à son tour `aggregate_gold_daily` (via `TriggerDagRunOperator`).

Les DAGs n'ont pas de planning cron fixe (`schedule=None`) : le poste de travail n'étant pas allumé en permanence, un cron à heure fixe n'aurait pas de sens. L'extraction est déclenchée à la demande via une tâche planifiée Windows, à chaque connexion à la session — un choix pragmatique documenté plutôt qu'un compromis caché.

## Choix techniques

- **Airflow** pour l'orchestration : gestion native des retries, logs centralisés, dépendances explicites entre les trois couches.
- **Parquet** plutôt que CSV pour le stockage : format colonnaire, compressé, qui conserve les types de données.
- **Pas de Spark** : le volume de données (7 tickers, historique quotidien) tient largement en mémoire sur une seule machine — Spark n'apporterait aucun bénéfice réel et ajouterait de la complexité inutile.
- **Docker Compose** pour la reproductibilité : l'ensemble du pipeline (Airflow inclus) se lance en une seule commande.
- **DuckDB envisagé pour une V2** de la couche Gold (requêtes analytiques plus riches sur les corrélations) — non implémenté pour l'instant, Pandas/Parquet suffisant au volume actuel.

## Stack technique

| Composant | Outil |
|---|---|
| Extraction | Python, yfinance |
| Orchestration | Apache Airflow |
| Stockage | Parquet |
| Traitement | Pandas |
| Conteneurisation | Docker / Docker Compose |
| Dashboard | Streamlit *(à venir)* |

---

## Structure du projet

```text
📁finance-data-platform/
├── config/ # configuration (tickers suivis)
├── dags/ # DAGs Airflow (extract_bronze, transform_silver, aggregate_gold)
├── src/ # logique métier (extraction, transformation, agrégation)
├── scripts/ # utilitaires de debug (inspection des sorties par couche)
├── data/ # données générées (non versionnées) : bronze/, silver/, gold/
├── docker-compose.yml
└── trigger_pipeline.ps1 # script de déclenchement (tâche planifiée Windows)
