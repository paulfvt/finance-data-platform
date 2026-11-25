"""
Page Live du dashboard : bougies d'une minute BTC et ETH issues de la couche
streaming (Coinbase -> Redpanda -> consommateur -> data/live/bars).

Seul le bloc live se rafraîchit (st.fragment, toutes les 5 s) : les contrôles
et le reste de la page ne sont pas réexécutés.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.stream.live_queries import MIN_OBS, VOL_WINDOW_MIN, live_metrics, live_status  # noqa: E402

LOCAL_TZ = "Europe/Paris"
SYMBOL_LABELS = {"BTC-USD": "Bitcoin", "ETH-USD": "Ethereum"}
SYMBOL_COLORS = {"BTC-USD": "#F7931A", "ETH-USD": "#8C8CFF"}   # orange Bitcoin, violet Ethereum : bien distincts
WINDOWS = {"1 h": 60, "2 h": 120, "6 h": 360}
FRESH_MIN, STALE_MIN = 2, 10   # au-delà de 2 min de retard : alerte ; au-delà de 10 : flux arrêté

st.set_page_config(page_title="Live — Finance Data Platform", page_icon="⚡", layout="wide")
st.title("⚡ Live — Bitcoin & Ethereum")
st.caption(
    "Bougies d'une minute construites en continu à partir de chaque trade Coinbase : "
    "websocket → Redpanda → consommateur → Parquet → DuckDB. Rafraîchi toutes les 5 secondes."
)

col_symbol, col_window = st.columns([2, 1])
symbol = col_symbol.radio(
    "Actif", list(SYMBOL_LABELS), format_func=SYMBOL_LABELS.get, horizontal=True, key="live_symbol"
)
window_label = col_window.radio("Fenêtre", list(WINDOWS), index=1, horizontal=True, key="live_window")


def load(window_min: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lit métriques et fraîcheur. Si un fichier est en cours de réécriture par le
    consommateur au moment de la lecture, on garde la dernière lecture réussie
    plutôt que d'afficher une erreur.
    """
    since = datetime.now(timezone.utc) - timedelta(minutes=window_min)
    try:
        metrics, status = live_metrics(since), live_status()
        st.session_state["live_last_good"] = (metrics, status)
    except Exception as exc:   # lecture concurrente d'un fichier en cours de copie
        if "live_last_good" not in st.session_state:
            raise
        st.toast(f"Lecture ignorée ({type(exc).__name__}), affichage de la précédente")
        metrics, status = st.session_state["live_last_good"]
    for df, col in ((metrics, "minute"), (status, "last_minute")):
        if not df.empty:
            df[col] = pd.to_datetime(df[col], utc=True).dt.tz_convert(LOCAL_TZ)
    return metrics, status


def freshness_badge(minutes_behind: int) -> str:
    if minutes_behind <= FRESH_MIN:
        return "🟢 en direct"
    if minutes_behind <= STALE_MIN:
        return f"🟠 en retard de {minutes_behind} min"
    return f"🔴 flux arrêté depuis {minutes_behind} min"


def candles_chart(df: pd.DataFrame) -> go.Figure:
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75, 0.25], vertical_spacing=0.03)
    fig.add_trace(go.Candlestick(
        x=df["minute"], open=df["open"], high=df["high"], low=df["low"], close=df["close"],
        increasing_line_color="#2ECC71", decreasing_line_color="#E74C3C", name="Prix",
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=df["minute"], y=df["vwap"], mode="lines", line=dict(width=1, dash="dot", color="#F1C40F"), name="VWAP",
    ), row=1, col=1)
    # Volume découpé en achats (vert) et ventes (rouge) : la pression acheteuse se lit d'un coup d'œil
    fig.add_trace(go.Bar(x=df["minute"], y=df["buy_volume"], marker_color="#2ECC71", name="Volume acheté"), row=2, col=1)
    fig.add_trace(go.Bar(x=df["minute"], y=df["volume"] - df["buy_volume"], marker_color="#E74C3C",
                         name="Volume vendu"), row=2, col=1)
    fig.update_layout(barmode="stack", xaxis_rangeslider_visible=False, height=560,
                      margin=dict(t=10, b=10), legend=dict(orientation="h", y=1.02, x=0))
    fig.update_yaxes(title_text="Prix (USD)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    return fig


def volatility_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for sym, g in df.groupby("symbol"):
        fig.add_trace(go.Scatter(x=g["minute"], y=g["vol_30m_annualized"] * 100, mode="lines",
                                 line=dict(color=SYMBOL_COLORS.get(sym), width=2),
                                 name=SYMBOL_LABELS.get(sym, sym)))
    fig.update_layout(height=300, margin=dict(t=10, b=10), yaxis_title="Volatilité annualisée (%)",
                      legend=dict(orientation="h", y=1.05, x=0))
    return fig


# ?refresh=off fige la page : pour une capture d'écran, ou examiner un graphique sans qu'il bouge.
REFRESH = None if st.query_params.get("refresh") == "off" else "5s"


@st.fragment(run_every=REFRESH)
def live_block() -> None:
    metrics, status = load(WINDOWS[st.session_state["live_window"]])
    sym = st.session_state["live_symbol"]

    if metrics.empty:
        st.info(
            "Aucune bougie live sur la fenêtre choisie. La couche streaming tourne-t-elle ? "
            "`docker compose ps` doit montrer stream-producer et stream-consumer « Up »."
        )
        return

    # Bandeau : dernier prix, variation sur la fenêtre, fraîcheur du flux
    cols = st.columns(len(status))
    for col, row in zip(cols, status.itertuples()):
        g = metrics[metrics["symbol"] == row.symbol]
        change = (g["close"].iloc[-1] / g["open"].iloc[0] - 1) * 100 if len(g) else float("nan")
        col.metric(
            f"{SYMBOL_LABELS.get(row.symbol, row.symbol)} — {freshness_badge(int(row.minutes_behind))}",
            f"{row.last_close:,.2f} $",
            f"{change:+.2f} % sur {st.session_state['live_window']}",
        )

    worst = int(status["minutes_behind"].max())
    if worst > STALE_MIN:
        st.warning(
            f"Le flux est arrêté depuis {worst} min (PC en veille, Docker arrêté ou coupure réseau). "
            "Il repartira tout seul ; les minutes manquantes restent vides, sans être inventées."
        )

    sel = metrics[metrics["symbol"] == sym]
    st.subheader(f"{SYMBOL_LABELS[sym]} — bougies d'une minute")
    st.plotly_chart(candles_chart(sel), use_container_width=True)

    last = sel.iloc[-1]
    c1, c2, c3 = st.columns(3)
    c1.metric("Trades sur la dernière minute", f"{int(last['trade_count'])}")
    c2.metric("Part achetée (dernière minute)", f"{last['buy_ratio'] * 100:.0f} %")
    vol = last["vol_30m_annualized"]
    c3.metric(f"Volatilité {VOL_WINDOW_MIN} min (annualisée)", "—" if pd.isna(vol) else f"{vol * 100:.0f} %")

    st.subheader(f"Volatilité glissante {VOL_WINDOW_MIN} min, annualisée")
    st.plotly_chart(volatility_chart(metrics), use_container_width=True)
    st.caption(
        f"Écart-type des rendements log d'une minute sur les {VOL_WINDOW_MIN} dernières minutes (fenêtre en temps), "
        f"× √525 600. Affichée à partir de {MIN_OBS} rendements ; pas de rendement calculé à travers un trou du flux. "
        f"Dernière mise à jour : {datetime.now().strftime('%H:%M:%S')}."
    )


live_block()
