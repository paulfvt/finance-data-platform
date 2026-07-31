"""Utilitaire de debug : affiche le contenu du dernier fichier Bronze extrait."""

from datetime import date
import pandas as pd

TICKER = "bitcoin"
run_date = date.today().isoformat()

df = pd.read_parquet(f"data/bronze/{run_date}/{TICKER}.parquet")
print(df)
print(df.info())