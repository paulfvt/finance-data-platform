"""
Tests pour la couche Gold (src/aggregate.py).
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.aggregate import align_calendar, compute_rolling_correlations  # noqa: E402


def test_align_calendar_fills_weekend_gaps():
    """
    Un actif qui ne trade pas le week-end (valeurs manquantes) doit être
    complété par forward-fill pour couvrir tout le calendrier.
    """
    returns = pd.DataFrame(
        {"stock": [0.01, 0.02, 0.03]},
        index=pd.to_datetime(["2026-01-02", "2026-01-05", "2026-01-06"]),  # vendredi, lundi, mardi
    )

    result = align_calendar(returns)

    assert pd.Timestamp("2026-01-03") in result.index
    assert pd.Timestamp("2026-01-04") in result.index
    assert result.loc["2026-01-03", "stock"] == 0.01


def test_compute_rolling_correlations_requires_full_window():
    """
    Avec moins de jours que la fenêtre de corrélation (30j), aucune
    ligne ne doit être produite.
    """
    dates = pd.date_range("2026-01-01", periods=10, freq="D")
    returns = pd.DataFrame({
        "asset_a": [0.01] * 10,
        "asset_b": [0.02] * 10,
    }, index=dates)

    result = compute_rolling_correlations(returns)

    assert result.empty


def test_compute_rolling_correlations_produces_expected_pairs():
    """Avec assez d'historique, chaque paire d'actifs (pas de doublon, pas d'auto-corrélation) doit apparaître."""
    dates = pd.date_range("2026-01-01", periods=35, freq="D")
    returns = pd.DataFrame({
        "asset_a": [0.01 * i for i in range(35)],
        "asset_b": [0.02 * i for i in range(35)],
        "asset_c": [-0.01 * i for i in range(35)],
    }, index=dates)

    result = compute_rolling_correlations(returns)

    pairs = set(zip(result["asset_1"], result["asset_2"]))
    assert ("asset_a", "asset_b") in pairs
    assert ("asset_a", "asset_c") in pairs
    assert ("asset_b", "asset_c") in pairs
    assert ("asset_a", "asset_a") not in pairs
    assert ("asset_b", "asset_a") not in pairs