"""
Génère un Bronze synthétique réaliste, pour faire tourner dbt build en CI sans
accès aux vraies données ni à yfinance.

- 8 actifs du référentiel, 120 jours jusqu'à aujourd'hui (UTC), marche aléatoire
  reproductible (graine fixe) ;
- pas de cotation le week-end, sauf pour la crypto ;
- volume nul pour un indice ou un taux (^VIX, ^TNX), comme dans les vraies données ;
- un dossier de run tous les 5 jours, chacun relisant les 7 derniers jours :
  les mêmes jours apparaissent dans plusieurs runs, comme en production ;
- colonnes au format du contrat Bronze (src/bronze_schema.py).

    python scripts/make_bronze_fixture.py /tmp/fixture    # écrit /tmp/fixture/bronze/...
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from config.tickers import TICKERS  # noqa: E402
from src.bronze_schema import to_bronze_schema  # noqa: E402

DAYS = 120
RUN_EVERY = 5
LOOKBACK = 7
CRYPTO = {"BTC-USD", "ETH-USD"}
NO_VOLUME = {"^VIX", "^TNX"}
START_PRICE = {"^GSPC": 6800, "^STOXX50E": 5500, "GC=F": 3900, "CL=F": 62, "^TNX": 4.5,
               "BTC-USD": 60000, "ETH-USD": 1700, "^VIX": 20}


def price_history(symbol: str, days: list[date], rng: np.random.Generator) -> pd.DataFrame:
    """Une série OHLCV cohérente (low <= open, close <= high) sur les jours de cotation de l'actif."""
    trading = [d for d in days if symbol in CRYPTO or d.weekday() < 5]
    vol = 0.03 if symbol in CRYPTO else 0.012
    closes = START_PRICE[symbol] * np.exp(np.cumsum(rng.normal(0, vol, len(trading))))
    opens = np.r_[closes[0], closes[:-1]] * np.exp(rng.normal(0, vol / 3, len(trading)))
    spread = np.abs(rng.normal(0, vol / 2, len(trading)))
    return pd.DataFrame({
        "date": pd.to_datetime(trading),
        "open": opens,
        "high": np.maximum(opens, closes) * (1 + spread),
        "low": np.minimum(opens, closes) * (1 - spread),
        "close": closes,
        "volume": 0.0 if symbol in NO_VOLUME else rng.uniform(1e6, 1e9, len(trading)).round(),
        "ticker": symbol,
    })


def main(out_root: Path) -> None:
    today = datetime.now(timezone.utc).date()
    days = [today - timedelta(days=DAYS - 1 - i) for i in range(DAYS)]
    # Même arborescence qu'en production (dossiers versionnés avec un .gitkeep) :
    # DuckDB ne crée pas les dossiers parents quand dbt écrit Silver et Gold.
    for layer in ("silver", "gold"):
        (out_root / layer).mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(42)
    histories = {name: price_history(symbol, days, rng) for name, symbol in TICKERS.items()}

    run_dates = days[LOOKBACK::RUN_EVERY]
    if run_dates[-1] != today:
        run_dates.append(today)            # le dernier run est toujours aujourd'hui
    files = 0
    for run in run_dates:
        run_dir = out_root / "bronze" / run.isoformat()
        run_dir.mkdir(parents=True, exist_ok=True)
        window_start = pd.Timestamp(run - timedelta(days=LOOKBACK))
        for name, history in histories.items():
            window = history[(history["date"] > window_start) & (history["date"] <= pd.Timestamp(run))]
            to_bronze_schema(window).to_parquet(run_dir / f"{name}.parquet", index=False)
            files += 1
    print(f"Bronze synthétique : {len(run_dates)} runs, {files} fichiers, {days[0]} -> {today}, dans {out_root / 'bronze'}")


if __name__ == "__main__":
    main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path("fixture"))
