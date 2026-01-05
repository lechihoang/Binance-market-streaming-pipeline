import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sys
sys.path.insert(0, "/app")

from components.api import get_klines, get_available_symbols

st.set_page_config(page_title="Symbol Deep Dive", page_icon="🔍", layout="wide")

COLORS = {
    "green": "#73BF69",
    "blue": "#5794F2", 
    "orange": "#FF9830",
    "red": "#F2495C",
    "bg": "#111111",
    "card": "#1E1E1E"
}

def create_price_chart(data, symbol):
    if not data:
        return go.Figure().update_layout(title=f"Price - {symbol}", template="plotly_dark")
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["timestamp"], y=df["close"],
        mode="lines", name="Price",
        line=dict(color=COLORS["green"], width=2),
        fill="tozeroy", fillcolor="rgba(115,191,105,0.15)"
    ))
    fig.update_layout(
        title=f"Price - {symbol}",
        template="plotly_dark",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        height=300,
        margin=dict(l=50, r=30, t=40, b=30),
        yaxis_title="Price (USD)",
        xaxis=dict(showgrid=True, gridcolor="#333"),
        yaxis=dict(showgrid=True, gridcolor="#333")
    )
    return fig

def create_volume_chart(data, symbol):
    if not data:
        return go.Figure().update_layout(title=f"Volume - {symbol}", template="plotly_dark")
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["timestamp"], y=df["volume"],
        name="Volume", marker_color=COLORS["blue"], opacity=0.7
    ))
    fig.update_layout(
        title=f"Volume - {symbol}",
        template="plotly_dark",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        height=300,
        margin=dict(l=50, r=30, t=40, b=30),
        yaxis_title="Volume",
        xaxis=dict(showgrid=True, gridcolor="#333"),
        yaxis=dict(showgrid=True, gridcolor="#333")
    )
    return fig

def create_trades_chart(data, symbol):
    if not data:
        return go.Figure().update_layout(title=f"Trades - {symbol}", template="plotly_dark")
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["timestamp"], y=df["trade_count"],
        name="Trades", marker_color=COLORS["orange"], opacity=0.7
    ))
    fig.update_layout(
        title=f"Trades Count - {symbol}",
        template="plotly_dark",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        height=300,
        margin=dict(l=50, r=30, t=40, b=30),
        yaxis_title="Trades",
        xaxis=dict(showgrid=True, gridcolor="#333"),
        yaxis=dict(showgrid=True, gridcolor="#333")
    )
    return fig

def create_buysell_chart(data, symbol):
    if not data:
        return go.Figure().update_layout(title=f"Buy/Sell - {symbol}", template="plotly_dark")
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    df["buy_pct"] = df["buy_count"] / df["trade_count"] * 100
    df["sell_pct"] = df["sell_count"] / df["trade_count"] * 100
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["timestamp"], y=df["buy_pct"],
        mode="lines", name="Buy %",
        line=dict(color=COLORS["green"], width=0),
        fill="tozeroy", fillcolor="rgba(115,191,105,0.8)",
        stackgroup="one"
    ))
    fig.add_trace(go.Scatter(
        x=df["timestamp"], y=df["sell_pct"],
        mode="lines", name="Sell %",
        line=dict(color=COLORS["red"], width=0),
        fill="tonexty", fillcolor="rgba(242,73,92,0.8)",
        stackgroup="one"
    ))
    fig.update_layout(
        title=f"Buy/Sell Ratio - {symbol}",
        template="plotly_dark",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        height=300,
        margin=dict(l=50, r=30, t=40, b=30),
        yaxis=dict(range=[0, 100], showgrid=True, gridcolor="#333"),
        xaxis=dict(showgrid=True, gridcolor="#333")
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
    minutes = st.selectbox("Period", [30, 60, 120], index=1, format_func=lambda x: f"{x} min", label_visibility="collapsed")

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
            }
        )

render()

import time
time.sleep(5)
st.rerun()
