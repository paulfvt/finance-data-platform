# Finance Data Platform

Pipeline de données financières (actions, matières premières, taux, cryptomonnaies) construit autour d'une architecture en couches (Bronze / Silver / Gold), avec extraction automatisée, nettoyage, et calcul d'indicateurs de marché (rendements, volatilité, corrélations inter-actifs).

Projet personnel réalisé dans le cadre de ma spécialisation Data & IA à l'ECE Paris, pour mettre en pratique des compétences de data engineering (orchestration, conteneurisation, modélisation de données) sur un sujet concret.

## Objectif

Suivre l'évolution de plusieurs classes d'actifs (actions, matières premières, taux d'intérêt, cryptomonnaies) et produire des indicateurs exploitables sur leurs dynamiques et leurs corrélations, sans intervention manuelle : le pipeline tourne quotidiennement, du téléchargement des données brutes jusqu'à la restitution.

## Actifs suivis

- Actions : S&P 500, EuroStoxx 50
- Matières premières : Or, Pétrole
- Taux : US 10 ans
- Cryptomonnaies : Bitcoin, Ethereum

Un actif par grande classe, pour pouvoir observer des dynamiques et des corrélations réellement différentes plutôt que des variantes d'un même marché.

## Architecture

Le pipeline suit une architecture médaillon, standard en data engineering pour séparer clairement donnée brute et donnée transformée :

[APIs de marché] ─▶ [BRONZE : ingestion brute, Parquet partitionné par date]
│
▼
[SILVER : nettoyage, alignement calendaire,
rendements, moyennes mobiles]
│
▼
[GOLD : corrélations glissantes, agrégats]
│
▼
[Dashboard]

**Bronze** conserve les données telles que renvoyées par la source, sans transformation — ça garantit de pouvoir toujours revenir à l'origine si une étape de traitement doit être revue.

**Silver** nettoie et normalise : harmonisation des dates (les marchés traditionnels ferment le week-end, la crypto trade en continu), calcul des rendements et moyennes mobiles.

**Gold** agrège : matrices de corrélation glissantes entre les actifs suivis, prêtes à être consommées par un dashboard.

## Choix techniques

- **Airflow** pour l'orchestration : gestion native des retries, logs centralisés, visibilité sur l'historique d'exécution — préférable à un simple cron pour un pipeline qui va gagner en complexité (dépendances entre étapes Bronze/Silver/Gold).
- **Parquet** plutôt que CSV pour le stockage brut : format colonnaire, compressé, qui conserve les types de données.
- **DuckDB** pour la couche analytique : base OLAP embarquée, largement suffisante pour ce volume de données sans la complexité d'un serveur de base de données dédié.
- **Pas de Spark** : le volume de données (quelques tickers, historique quotidien) tient largement en mémoire sur une seule machine — Spark n'apporterait aucun bénéfice réel et ajouterait de la complexité inutile.
- **Docker Compose** pour la reproductibilité : l'ensemble du pipeline se lance en une seule commande, sur n'importe quel environnement.

## Stack technique

| Composant | Outil |
|---|---|
| Extraction | Python, yfinance |
| Orchestration | Apache Airflow |
| Stockage brut | Parquet |
| Stockage analytique | DuckDB |
| Dashboard | Streamlit |
| Conteneurisation | Docker / Docker Compose |

## Structure du projet 

```text
📁 finance-data-platform/
├── config/       # configuration (tickers suivis)
├── dags/         # DAGs Airflow
├── src/          # logique métier (extraction, transformation)
├── data/         # données générées (non versionnées)
└── docker-compose.yml
