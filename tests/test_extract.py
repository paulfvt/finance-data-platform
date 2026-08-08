"""
Tests d'intégration légers sur l'écriture des fichiers (src/extract.py).
Ce test couvre spécifiquement le bug rencontré en conditions réelles :
une incompatibilité de version pyarrow entre environnements avait rendu
des fichiers Parquet illisibles après écriture.
"""

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.extract import save_bronze  # noqa: E402


def test_save_bronze_writes_readable_parquet(tmp_path, monkeypatch):
    """Le fichier écrit par save_bronze doit être immédiatement relisible."""
    import src.extract as extract_module
    monkeypatch.setattr(extract_module, "DATA_DIR", tmp_path)

    df = pd.DataFrame({"date": ["2026-01-01"], "close": [100.0], "ticker": ["TEST"]})
    out_path = save_bronze(df, "test_ticker", date(2026, 1, 1))

    result = pd.read_parquet(out_path)

    assert len(result) == 1
    assert result.iloc[0]["close"] == 100.0