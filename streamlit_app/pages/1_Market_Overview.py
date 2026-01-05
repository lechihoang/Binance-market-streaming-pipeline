import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import sys
sys.path.insert(0, "/app")

from components.api import (
    get_realtime_tickers,
    get_market_summary,
    get_top_by_volume,
    get_top_by_trades
)

st.set_page_config(page_title="Market Overview", page_icon="📈", layout="wide")

COLORS = {
    "blue": "#5794F2",
    "green": "#73BF69", 
    "yellow": "#FADE2A",
    "orange": "#FF9830",
    "red": "#F2495C",
    "purple": "#B877D9",
    "bg": "#111111",
    "card": "#1E1E1E",
    "text": "#FAFAFA"
}

def metric_card(label, value, color):
    st.markdown(f"""
    <div style="background:{COLORS['card']}; padding:20px; border-radius:4px; border-left:4px solid {color};">
        <div style="color:#999; font-size:12px; margin-bottom:8px;">{label}</div>
        <div style="color:{color}; font-size:28px; font-weight:bold;">{value}</div>
    </div>
    """, unsafe_allow_html=True)

def horizontal_bar_chart(data, x_col, y_col, title, color):
    if not data:
        return go.Figure().update_layout(title=title, template="plotly_dark")
    
    df = pd.DataFrame(data)
    fig = go.Figure(go.Bar(
        x=df[x_col],
        y=df[y_col],
        orientation='h',
        marker_color=color,
        text=df[x_col].apply(lambda x: f"{x:,.0f}" if isinstance(x, (int, float)) else x),
        textposition='outside'
    ))
    fig.update_layout(
        title=title,
        template="plotly_dark",
        paper_bgcolor=COLORS["bg"],
        plot_bgcolor=COLORS["bg"],
        height=300,
        margin=dict(l=100, r=50, t=50, b=30),
        yaxis=dict(autorange="reversed"),
        xaxis=dict(showgrid=True, gridcolor="#333")
    )
    return fig

placeholder = st.empty()

def render():
    with placeholder.container():
        summary = get_market_summary()
        tickers = get_realtime_tickers()
        top_vol = get_top_by_volume(5)
        top_trades = get_top_by_trades(5)
        
        st.markdown("## Market Overview")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            metric_card("Total Symbols", summary.get("total_symbols", 0), COLORS["blue"])
        with col2:
            trades = summary.get("total_trades", 0)
            metric_card("Total Trades (24h)", f"{trades:,.0f}" if trades else "0", COLORS["green"])
        with col3:
            vol = summary.get("total_quote_volume", 0)
            metric_card("Total Volume (24h)", f"${vol:,.0f}" if vol else "$0", COLORS["yellow"])
        with col4:
            avg = summary.get("avg_trade_value", 0)
            metric_card("Avg Trade Value", f"${avg:,.2f}" if avg else "$0", COLORS["purple"])
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("### All Tickers - Real-time Data")
        
        if tickers:
            df = pd.DataFrame(tickers)
            
            display_df = pd.DataFrame()
            display_df["Symbol"] = df["symbol"]
            display_df["Price"] = df["last_price"].apply(lambda x: float(x) if x else 0)
            display_df["Change"] = df["price_change"].apply(lambda x: float(x) if x else 0)
            display_df["Change %"] = df["price_change_pct"].apply(lambda x: float(x) if x else 0)
            display_df["High"] = df["high"].apply(lambda x: float(x) if x else 0)
            display_df["Low"] = df["low"].apply(lambda x: float(x) if x else 0)
            display_df["Volume"] = df["volume"].apply(lambda x: float(x) if x else 0)
            display_df["Quote Vol"] = df["quote_volume"].apply(lambda x: float(x) if x else 0)
            display_df["Trades"] = df["trade_count"].apply(lambda x: int(x) if x else 0)
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                height=400,
                column_config={
                    "Symbol": st.column_config.TextColumn("Symbol", width="small"),
                    "Price": st.column_config.NumberColumn("Price", format="$%.4f"),
                    "Change": st.column_config.NumberColumn("Change", format="$%.4f"),
                    "Change %": st.column_config.NumberColumn("Change %", format="%.2f%%"),
                    "High": st.column_config.NumberColumn("High", format="$%.4f"),
                    "Low": st.column_config.NumberColumn("Low", format="$%.4f"),
                    "Volume": st.column_config.NumberColumn("Volume", format="%.2f"),
                    "Quote Vol": st.column_config.NumberColumn("Quote Vol", format="$%.0f"),
                    "Trades": st.column_config.NumberColumn("Trades", format="%d"),
                }
            )
        else:
            st.info("No ticker data")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        col_left, col_right = st.columns(2)
        
        with col_left:
            if top_trades:
                for t in top_trades:
                    t["trade_count"] = int(t.get("trade_count", 0))
                fig = horizontal_bar_chart(top_trades, "trade_count", "symbol", "Top 5 by Trade Count", COLORS["blue"])
                st.plotly_chart(fig, use_container_width=True)
        
        with col_right:
            if top_vol:
                for v in top_vol:
                    v["quote_volume"] = float(v.get("quote_volume", 0))
                fig = horizontal_bar_chart(top_vol, "quote_volume", "symbol", "Top 5 by Quote Volume", COLORS["green"])
                st.plotly_chart(fig, use_container_width=True)

render()

import time
time.sleep(5)
st.rerun()
