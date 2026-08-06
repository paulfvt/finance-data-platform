"""
Dashboard Streamlit — visualisation des données Bronze/Silver/Gold
du pipeline finance-data-platform.
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.tickers import TICKERS  # noqa: E402

BRONZE_DIR = Path(__file__).resolve().parent.parent / "data" / "bronze"
SILVER_DIR = Path(__file__).resolve().parent.parent / "data" / "silver"
GOLD_DIR = Path(__file__).resolve().parent.parent / "data" / "gold"

TICKER_LABELS = {
    "sp500": "S&P 500",
    "stoxx50": "EuroStoxx 50",
    "gold": "Or",
    "oil": "Pétrole",
    "us10y": "Taux US 10 ans",
    "bitcoin": "Bitcoin",
    "ethereum": "Ethereum",
}

st.set_page_config(page_title="Finance Data Platform", page_icon="📈", layout="wide")
st.title("📈 Finance Data Platform")


@st.cache_data
def load_silver(ticker_name: str) -> pd.DataFrame:
    return pd.read_parquet(SILVER_DIR / f"{ticker_name}.parquet")


@st.cache_data
def load_gold_correlations() -> pd.DataFrame:
    path = GOLD_DIR / "correlations_30d.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def compute_kpis(df: pd.DataFrame) -> dict:
    """Calcule les indicateurs clés à afficher pour un actif donné."""
    latest = df.iloc[-1]
    previous = df.iloc[-2] if len(df) > 1 else latest

    daily_change_pct = ((latest["close"] - previous["close"]) / previous["close"]) * 100
    trend = "Haussière" if latest["close"] > latest.get("ma_20", latest["close"]) else "Baissière"

    return {
        "last_close": latest["close"],
        "daily_change_pct": daily_change_pct,
        "volatility_20d": latest.get("volatility_20d"),
        "period_high": df["close"].max(),
        "period_low": df["close"].min(),
        "trend": trend,
    }


# --- Sidebar ---
st.sidebar.header("Sélection")
selected_ticker = st.sidebar.selectbox(
    "Actif",
    options=list(TICKERS.keys()),
    format_func=lambda t: TICKER_LABELS.get(t, t),
)
period_days = st.sidebar.slider("Période affichée (jours)", min_value=5, max_value=90, value=30)

# --- Vue d'ensemble du marché ---
st.subheader("Vue d'ensemble du marché")

overview_rows = []
for t_name in TICKERS:
    t_df = load_silver(t_name).tail(period_days)
    t_kpis = compute_kpis(t_df)
    overview_rows.append({
        "Actif": TICKER_LABELS.get(t_name, t_name),
        "Dernier cours": round(t_kpis["last_close"], 2),
        "Variation (24h)": f"{t_kpis['daily_change_pct']:+.2f}%",
        "Volatilité 20j": round(t_kpis["volatility_20d"], 4) if t_kpis["volatility_20d"] else None,
        "Tendance": t_kpis["trend"],
    })

overview_df = pd.DataFrame(overview_rows)
st.dataframe(overview_df, use_container_width=True, hide_index=True)

# --- Détail de l'actif sélectionné ---
df = load_silver(selected_ticker)
df_period = df.tail(period_days)
kpis = compute_kpis(df_period)

st.header(f"Détail — {TICKER_LABELS.get(selected_ticker, selected_ticker)}")

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric(
    "Dernier cours",
    f"{kpis['last_close']:,.2f}",
    f"{kpis['daily_change_pct']:+.2f}%",
)
kpi2.metric("Volatilité 20j", f"{kpis['volatility_20d']:.4f}" if kpis["volatility_20d"] else "N/A")
kpi3.metric("Plus haut (période)", f"{kpis['period_high']:,.2f}")
kpi4.metric("Plus bas (période)", f"{kpis['period_low']:,.2f}")
kpi5.metric("Tendance", kpis["trend"])

st.subheader("🌡️ Baromètre de la peur (VIX)")

vix_df = load_silver("vix")
vix_latest = vix_df.iloc[-1]["close"]

if vix_latest < 15:
    vix_zone, vix_color = "Calme", "#2ECC71"
elif vix_latest < 25:
    vix_zone, vix_color = "Normal", "#F1C40F"
elif vix_latest < 35:
    vix_zone, vix_color = "Tension", "#E67E22"
else:
    vix_zone, vix_color = "Panique", "#E74C3C"

fig_vix = go.Figure(go.Indicator(
    mode="gauge+number",
    value=vix_latest,
    number={"suffix": "", "font": {"size": 40}},
    gauge={
        "axis": {"range": [0, 50]},
        "bar": {"color": vix_color},
        "steps": [
            {"range": [0, 15], "color": "#1E3D2F"},
            {"range": [15, 25], "color": "#3D3A1E"},
            {"range": [25, 35], "color": "#3D2A1E"},
            {"range": [35, 50], "color": "#3D1E1E"},
        ],
    },
))
fig_vix.update_layout(height=250, margin=dict(t=30, b=10))
st.plotly_chart(fig_vix, use_container_width=True)
st.caption(
    f"**Zone actuelle : {vix_zone}**. Le VIX mesure la volatilité attendue du S&P 500 — "
    "plus il est élevé, plus les investisseurs anticipent des mouvements brutaux. "
    "< 15 : marché calme · 15-25 : normal · 25-35 : nervosité · > 35 : panique."
)

st.subheader("Évolution du prix")

fig = px.line(
    df_period,
    x="date",
    y=["close", "ma_20", "ma_50"],
    labels={"value": "Prix", "date": "Date", "variable": "Série"},
)
fig.update_xaxes(
    rangeselector=dict(
        buttons=[
            dict(count=7, label="7j", step="day", stepmode="backward"),
            dict(count=30, label="30j", step="day", stepmode="backward"),
            dict(step="all", label="Tout"),
        ]
    )
)
newnames = {"close": "Cours de clôture", "ma_20": "Moyenne mobile 20j", "ma_50": "Moyenne mobile 50j"}
fig.for_each_trace(lambda t: t.update(name=newnames.get(t.name, t.name)))
st.plotly_chart(fig, use_container_width=True)

st.subheader("Chandelier — Ouverture / Clôture / Plus Haut / Plus Bas")

fig_candle = go.Figure(
    data=[
        go.Candlestick(
            x=df_period["date"],
            open=df_period["open"],
            high=df_period["high"],
            low=df_period["low"],
            close=df_period["close"],
            increasing_line_color="#2ECC71",
            decreasing_line_color="#E74C3C",
            name="Prix",
        )
    ]
)
fig_candle.update_layout(
    xaxis_title="Date",
    yaxis_title="Prix",
    xaxis_rangeslider_visible=False,
)
st.plotly_chart(fig_candle, use_container_width=True)
st.caption(
    "🟢 Vert : le prix a clôturé plus haut qu'à l'ouverture (journée haussière). "
    "🔴 Rouge : le prix a clôturé plus bas qu'à l'ouverture (journée baissière). "
    "Les traits fins montrent le plus haut et le plus bas atteints dans la journée."
)

# --- Corrélations (Gold) ---
st.header("Corrélations entre actifs (fenêtre glissante 30 jours)")

gold_df = load_gold_correlations()

if gold_df.empty:
    st.info(
        "Pas encore assez d'historique pour calculer des corrélations sur 30 jours. "
        "Cette section se remplira automatiquement à mesure que le pipeline accumule "
        "des données quotidiennes."
    )
else:
    latest_date = gold_df["date"].max()
    latest = gold_df[gold_df["date"] == latest_date]

    tickers = list(TICKERS.keys())
    matrix = pd.DataFrame(index=tickers, columns=tickers, dtype=float)
    for t in tickers:
        matrix.loc[t, t] = 1.0
    for _, row in latest.iterrows():
        matrix.loc[row["asset_1"], row["asset_2"]] = row["correlation_30d"]
        matrix.loc[row["asset_2"], row["asset_1"]] = row["correlation_30d"]

    fig_heatmap = px.imshow(
        matrix.astype(float),
        text_auto=".2f",
        color_continuous_scale="RdBu",
        zmin=-1,
        zmax=1,
        labels={"color": "Corrélation"},
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)
    st.caption(f"Dernière mise à jour : {latest_date.date()}")

# --- Debug / transparence Bronze ---
with st.expander("Données brutes (Bronze) — debug/transparence"):
    st.caption(
        "Cette section n'est pas destinée à l'analyse : elle montre les données "
        "telles qu'extraites, avant tout nettoyage, pour vérifier que le pipeline "
        "d'ingestion fonctionne correctement."
    )
    bronze_dates = sorted([d.name for d in BRONZE_DIR.iterdir() if d.is_dir()], reverse=True)
    if bronze_dates:
        selected_date = st.selectbox("Date d'extraction", options=bronze_dates)
        bronze_path = BRONZE_DIR / selected_date / f"{selected_ticker}.parquet"
        if bronze_path.exists():
            st.dataframe(pd.read_parquet(bronze_path))
        else:
            st.warning(f"Pas de donnée Bronze pour {selected_ticker} à cette date.")
    else:
        st.warning("Aucune donnée Bronze disponible.")