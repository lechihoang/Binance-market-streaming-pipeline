import sys
import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, "/app")

from components.api import get_available_symbols, get_klines

st.set_page_config(page_title="Symbol Deep Dive", page_icon="🔍", layout="wide")

COLORS = {
    "green": "#2E7D32",
    "blue": "#1976D2",
    "orange": "#F57C00",
    "red": "#D32F2F",
    "bg": "#FFFFFF",
    "card": "#F5F5F5",
    "grid": "#E0E0E0",
}


def create_price_chart(data, symbol):
    if not data:
        return go.Figure().update_layout(title=f"Price - {symbol}", template="plotly_dark")
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Calculate y-axis range with padding
    price_min = df["low"].min()
    price_max = df["high"].max()
    price_range = price_max - price_min
    padding = price_range * 0.1 if price_range > 0 else price_max * 0.01

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["close"],
            mode="lines",
            name="Price",
            line={"color": COLORS["green"], "width": 2},
            hovertemplate="<b>%{x|%H:%M}</b><br>" + "Close: $%{y:,.2f}<br>" + "<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"Price - {symbol}",
        template="plotly_white",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        height=300,
        margin={"l": 50, "r": 30, "t": 40, "b": 30},
        yaxis_title="Price (USD)",
        xaxis={"showgrid": True, "gridcolor": COLORS["grid"]},
        yaxis={"showgrid": True, "gridcolor": COLORS["grid"], "range": [price_min - padding, price_max + padding]},
        hovermode="x unified",
    )
    return fig


def create_volume_chart(data, symbol):
    if not data:
        return go.Figure().update_layout(title=f"Volume - {symbol}", template="plotly_white")
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df["timestamp"],
            y=df["volume"],
            name="Volume",
            marker_color=COLORS["blue"],
            opacity=0.7,
            hovertemplate="<b>%{x|%H:%M}</b><br>" + "Volume: %{y:,.4f}<br>" + "<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"Volume - {symbol}",
        template="plotly_white",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        height=300,
        margin={"l": 50, "r": 30, "t": 40, "b": 30},
        yaxis_title="Volume",
        xaxis={"showgrid": True, "gridcolor": COLORS["grid"]},
        yaxis={"showgrid": True, "gridcolor": COLORS["grid"]},
        hovermode="x unified",
    )
    return fig


def create_trades_chart(data, symbol):
    if not data:
        return go.Figure().update_layout(title=f"Trades - {symbol}", template="plotly_white")
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=df["timestamp"],
            y=df["trade_count"],
            name="Trades",
            marker_color=COLORS["orange"],
            opacity=0.7,
            customdata=df[["buy_count", "sell_count"]].values,
            hovertemplate="<b>%{x|%H:%M}</b><br>"
            + "Total: %{y:,} trades<br>"
            + "Buy: %{customdata[0]:,}<br>"
            + "Sell: %{customdata[1]:,}<br>"
            + "<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"Trades Count - {symbol}",
        template="plotly_white",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        height=300,
        margin={"l": 50, "r": 30, "t": 40, "b": 30},
        yaxis_title="Trades",
        xaxis={"showgrid": True, "gridcolor": COLORS["grid"]},
        yaxis={"showgrid": True, "gridcolor": COLORS["grid"]},
        hovermode="x unified",
    )
    return fig


def create_buysell_chart(data, symbol):
    if not data:
        return go.Figure().update_layout(title=f"Buy/Sell - {symbol}", template="plotly_white")
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df["buy_pct"] = df["buy_count"] / df["trade_count"] * 100
    df["sell_pct"] = df["sell_count"] / df["trade_count"] * 100

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["buy_pct"],
            mode="lines",
            name="Buy %",
            line={"color": COLORS["green"], "width": 0},
            fill="tozeroy",
            fillcolor="rgba(46,125,50,0.7)",
            stackgroup="one",
            customdata=df[["buy_count", "trade_count"]].values,
            hovertemplate="<b>Buy</b>: %{y:.1f}%<br>"
            + "(%{customdata[0]:,} / %{customdata[1]:,})<br>"
            + "<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["sell_pct"],
            mode="lines",
            name="Sell %",
            line={"color": COLORS["red"], "width": 0},
            fill="tonexty",
            fillcolor="rgba(211,47,47,0.7)",
            stackgroup="one",
            customdata=df[["sell_count", "trade_count"]].values,
            hovertemplate="<b>Sell</b>: %{y:.1f}%<br>"
            + "(%{customdata[0]:,} / %{customdata[1]:,})<br>"
            + "<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"Buy/Sell Ratio - {symbol}",
        template="plotly_white",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        height=300,
        margin={"l": 50, "r": 30, "t": 40, "b": 30},
        yaxis={"range": [0, 100], "showgrid": True, "gridcolor": COLORS["grid"], "title": "Percentage"},
        xaxis={"showgrid": True, "gridcolor": COLORS["grid"]},
        hovermode="x unified",
    )
    return fig


symbols = get_available_symbols()
if not symbols:
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    symbol = st.selectbox("Symbol", symbols, index=0, label_visibility="collapsed")
with col2:
    interval = st.selectbox("Interval", ["1m", "5m", "15m"], index=0, label_visibility="collapsed")
with col3:
    minutes = st.selectbox(
        "Period", [30, 60, 120], index=1, format_func=lambda x: f"{x} min", label_visibility="collapsed"
    )

placeholder = st.empty()


def render():
    with placeholder.container():
        data = get_klines(symbol, interval, minutes)

        st.markdown(f"## Symbol Deep Dive - {symbol}")

        if not data:
            st.warning(f"No data for {symbol}")
            return

        row1_left, row1_right = st.columns(2)
        with row1_left:
            st.plotly_chart(create_price_chart(data, symbol), use_container_width=True)
        with row1_right:
            st.plotly_chart(create_volume_chart(data, symbol), use_container_width=True)

        row2_left, row2_right = st.columns(2)
        with row2_left:
            st.plotly_chart(create_buysell_chart(data, symbol), use_container_width=True)
        with row2_right:
            st.plotly_chart(create_trades_chart(data, symbol), use_container_width=True)

        st.markdown(f"### Recent Candles - {symbol}")
        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp", ascending=False)

        st.dataframe(
            df[["timestamp", "open", "high", "low", "close", "volume", "trade_count", "buy_count", "sell_count"]],
            use_container_width=True,
            hide_index=True,
            height=300,
            column_config={
                "timestamp": st.column_config.DatetimeColumn("Time", format="YYYY-MM-DD HH:mm"),
                "open": st.column_config.NumberColumn("Open", format="$%.2f"),
                "high": st.column_config.NumberColumn("High", format="$%.2f"),
                "low": st.column_config.NumberColumn("Low", format="$%.2f"),
                "close": st.column_config.NumberColumn("Close", format="$%.2f"),
                "volume": st.column_config.NumberColumn("Volume", format="%.4f"),
                "trade_count": st.column_config.NumberColumn("Trades"),
                "buy_count": st.column_config.NumberColumn("Buy"),
                "sell_count": st.column_config.NumberColumn("Sell"),
            },
        )


render()

time.sleep(5)
st.rerun()
