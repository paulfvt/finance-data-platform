"""
Couche Bronze, extraction brute des donnees de marche via yfinance
"""

from datetime import date, timedelta
import yfinance as yf


def fetch_ticker(ticker_symbol: str, lookback_days: int = 5):
    """Recupere les cours d'un ticker sur les derniers jours"""
    end = date.today() + timedelta(days=1)
    start = end - timedelta(days=lookback_days)

    df = yf.download(ticker_symbol, start=start.isoformat(), end=end.isoformat())
    return df


if __name__ == "__main__":
    df = fetch_ticker("BTC-USD")
    print(df)