"""
Dashboard Streamlit — visualisation des données Bronze/Silver/Gold
du pipeline finance-data-platform.
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.tickers import TICKERS  # noqa: E402

SILVER_DIR = Path(__file__).resolve().parent.parent / "data" / "silver"

st.set_page_config(page_title="Finance Data Platform", layout="wide")
st.title("Finance Data Platform")


@st.cache_data
def load_silver(ticker_name: str) -> pd.DataFrame:
    return pd.read_parquet(SILVER_DIR / f"{ticker_name}.parquet")


st.sidebar.header("Sélection")
selected_ticker = st.sidebar.selectbox("Actif", options=list(TICKERS.keys()))

st.header(f"Évolution du prix — {selected_ticker}")
df = load_silver(selected_ticker)

fig = px.line(
    df,
    x="date",
    y=["close", "ma_20", "ma_50"],
    labels={"value": "Prix", "date": "Date", "variable": "Série"},
)
st.plotly_chart(fig, use_container_width=True)

st.header(f"Rendements et volatilité — {selected_ticker}")

col1, col2 = st.columns(2)

with col1:
    fig_returns = px.bar(
        df,
        x="date",
        y="daily_return",
        labels={"daily_return": "Rendement journalier", "date": "Date"},
    )
    st.plotly_chart(fig_returns, use_container_width=True)

with col2:
    fig_vol = px.line(
        df,
        x="date",
        y="volatility_20d",
        labels={"volatility_20d": "Volatilité glissante (20j)", "date": "Date"},
    )
    st.plotly_chart(fig_vol, use_container_width=True)