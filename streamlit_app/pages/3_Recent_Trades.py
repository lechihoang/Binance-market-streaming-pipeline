import sys
import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, "/app")

from components.api import get_available_symbols, get_recent_trades
from components.trades_feed import render_trade_stats, render_trades_feed

st.set_page_config(page_title="Recent Trades", layout="wide")

COLORS = {
    "green": "#2E7D32",
    "red": "#D32F2F",
    "blue": "#1976D2",
    "bg": "#FFFFFF",
    "card": "#F5F5F5",
    "grid": "#E0E0E0",
}


def create_volume_distribution_chart(trades: list[dict]):
    """Create buy/sell volume distribution pie chart."""
    if not trades:
        return go.Figure().update_layout(title="Volume Distribution", template="plotly_white")

    df = pd.DataFrame(trades)

    buy_volume = df[df["side"] == "BUY"]["quantity"].sum()
    sell_volume = df[df["side"] == "SELL"]["quantity"].sum()

    fig = go.Figure(
        data=[
            go.Pie(
                labels=["Buy", "Sell"],
                values=[buy_volume, sell_volume],
                marker_colors=[COLORS["green"], COLORS["red"]],
                hole=0.4,
                textinfo="label+percent",
                hovertemplate="<b>%{label}</b><br>Volume: %{value:.4f}<br>%{percent}<br><extra></extra>",
            )
        ]
    )

    fig.update_layout(
        title="Buy/Sell Volume Distribution",
        template="plotly_white",
        paper_bgcolor=COLORS["bg"],
        height=400,
        margin={"l": 20, "r": 20, "t": 40, "b": 20},
    )

    return fig


symbols = get_available_symbols()
if not symbols:
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]

col1, col2 = st.columns([2, 1])
with col1:
    symbol = st.selectbox("Select Symbol", symbols, index=0)
with col2:
    limit = st.selectbox("Number of Trades", [20, 50, 100], index=0, format_func=lambda x: f"{x} trades")

placeholder = st.empty()


def render():
    with placeholder.container():
        st.markdown(f"## Recent Trades - {symbol}")
        st.caption(f"Latest {limit} trades • Auto-refresh every 5 seconds")

        trades = get_recent_trades(symbol, limit)

        if not trades:
            st.warning(f"No recent trades for {symbol}. Make sure trade consumer is running.")
            st.info(
                "To start collecting trades:\n"
                "1. Run trade consumer: `python -m ingestion.trade_consumer`\n"
                "2. Wait a few seconds for trades to populate\n"
                "3. Refresh this page"
            )
            return

        # Trade statistics
        st.markdown("### Trade Statistics")
        render_trade_stats(trades)

        st.markdown("---")

        # Chart
        st.plotly_chart(create_volume_distribution_chart(trades), use_container_width=True)

        st.markdown("---")

        # Trades feed
        st.markdown(f"### Trades Feed ({len(trades)})")
        render_trades_feed(trades, height=500, show_symbol=False)


render()

time.sleep(5)
st.rerun()
