"""
Tests de la fenêtre de rattrapage Bronze (src/extract.py) : après une coupure,
l'extraction doit remonter jusqu'au dernier run réussi pour ne pas laisser de trou.
"""

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))
import src.extract as extract  # noqa: E402
from src.extract import (  # noqa: E402
    MAX_LOOKBACK_DAYS,
    MIN_LOOKBACK_DAYS,
    compute_lookback_days,
    last_bronze_date,
)


def test_lookback_stays_minimal_when_pipeline_ran_yesterday():
    assert compute_lookback_days(date(2026, 11, 24), date(2026, 11, 25)) == MIN_LOOKBACK_DAYS


def test_lookback_covers_the_whole_gap_after_an_outage():
    """Cas réel : dernier run le 15/09, reprise le 25/11 (71 jours de trou)."""
    lookback = compute_lookback_days(date(2026, 9, 15), date(2026, 11, 25))
    assert lookback >= 71


def test_lookback_is_capped():
    assert compute_lookback_days(date(2020, 1, 1), date(2026, 11, 25)) == MAX_LOOKBACK_DAYS


def test_lookback_without_history_fetches_the_maximum():
    assert compute_lookback_days(None, date(2026, 11, 25)) == MAX_LOOKBACK_DAYS


def test_last_bronze_date_ignores_non_date_entries(tmp_path):
    (tmp_path / "2026-08-14").mkdir()
    (tmp_path / "2026-09-15").mkdir()
    (tmp_path / "not-a-date").mkdir()
    (tmp_path / ".gitkeep").touch()

    assert last_bronze_date(tmp_path, before=date(2026, 11, 25)) == date(2026, 9, 15)


def test_last_bronze_date_empty_dir(tmp_path):
    assert last_bronze_date(tmp_path, before=date(2026, 11, 25)) is None


def test_second_run_of_the_day_keeps_the_backfill_window(tmp_path):
    """
    Bug réel du 25/11 : le 1er run du jour rattrapait 73 jours, le 2e voyait
    « dernier run = aujourd'hui », prenait 5 jours et écrasait le rattrapage.
    Le dossier du jour ne doit pas compter.
    """
    (tmp_path / "2026-09-15").mkdir()
    (tmp_path / "2026-11-25").mkdir()  # écrit par le 1er run du jour
    today = date(2026, 11, 25)

    last = last_bronze_date(tmp_path, before=today)

    assert last == date(2026, 9, 15)
    assert compute_lookback_days(last, today) >= 71


def test_run_extraction_fails_loudly_but_keeps_successful_tickers(tmp_path, monkeypatch):
    """Un ticker en échec doit faire échouer la tâche, sans perdre ceux déjà sauvegardés."""
    monkeypatch.setattr(extract, "DATA_DIR", tmp_path)
    monkeypatch.setattr(extract, "TICKERS", {"ok": "OK", "ko": "KO"})

    def fake_fetch(symbol, lookback_days):
        if symbol == "KO":
            raise RuntimeError("API indisponible")
        return pd.DataFrame({"date": ["2026-11-25"], "close": [1.0], "ticker": [symbol]})

    monkeypatch.setattr(extract, "fetch_ticker", fake_fetch)

    with pytest.raises(RuntimeError, match="ko"):
        extract.run_extraction(run_date=date(2026, 11, 25))

    assert (tmp_path / "2026-11-25" / "ok.parquet").exists()
