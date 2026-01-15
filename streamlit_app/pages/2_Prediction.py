import sys
import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, "/app")

from components.api import (
    get_available_symbols,
    get_buy_sell_imbalance,
    get_ml_prediction,
    get_ml_status,
    get_price_spikes,
    get_trade_count_spikes,
    get_volume_spikes,
)

st.set_page_config(page_title="Prediction & Alerts", layout="wide")

COLORS = {
    "green": "#2E7D32",
    "blue": "#1976D2",
    "orange": "#F57C00",
    "red": "#D32F2F",
    "purple": "#7B1FA2",
    "bg": "#FFFFFF",
    "card": "#F5F5F5",
    "grid": "#E0E0E0",
}


def get_volatility_color(level: str) -> str:
    return {"LOW": COLORS["green"], "MEDIUM": COLORS["orange"], "HIGH": COLORS["red"]}.get(level, COLORS["blue"])


def get_volatility_icon(level: str) -> str:
    return {"LOW": "", "MEDIUM": "", "HIGH": ""}.get(level, "")


symbols = get_available_symbols()
if not symbols:
    symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]

placeholder = st.empty()


def render():
    with placeholder.container():
        st.markdown("## Prediction & Alerts")

        # ML Status
        ml_status = get_ml_status()
        model_loaded = ml_status.get("model_loaded", False)

        if model_loaded:
            st.success("✅ ML Model loaded and ready")
        else:
            st.error("❌ ML Model not loaded")
            st.stop()

        st.markdown("---")

        # Volatility Prediction Section
        st.markdown("### Volatility Prediction")

        col1, col2 = st.columns([1, 3])
        with col1:
            symbol = st.selectbox("Select Symbol", symbols, index=0)

        prediction = get_ml_prediction(symbol)

        if prediction:
            level = prediction.get("volatility_level", "UNKNOWN")
            current_vol = prediction.get("current_volatility", 0) * 100
            predicted_vol = prediction.get("predicted_volatility_5m", 0) * 100
            timestamp = prediction.get("timestamp", "")

            col1, col2, col3 = st.columns(3)

            with col1:
                st.markdown(
                    f"""
                <div style="background:{COLORS['card']}; padding:20px; border-radius:8px; border-left:4px solid {COLORS['blue']};">
                    <div style="color:#666; font-size:12px; margin-bottom:8px;">Current Volatility</div>
                    <div style="color:{COLORS['blue']}; font-size:28px; font-weight:bold;">{current_vol:.2f}%</div>
                </div>
                """,
                    unsafe_allow_html=True,
                )

            with col2:
                st.markdown(
                    f"""
                <div style="background:{COLORS['card']}; padding:20px; border-radius:8px; border-left:4px solid {COLORS['purple']};">
                    <div style="color:#666; font-size:12px; margin-bottom:8px;">Predicted (5m)</div>
                    <div style="color:{COLORS['purple']}; font-size:28px; font-weight:bold;">{predicted_vol:.2f}%</div>
                </div>
                """,
                    unsafe_allow_html=True,
                )

            with col3:
                vol_color = get_volatility_color(level)
                st.markdown(
                    f"""
                <div style="background:{COLORS['card']}; padding:20px; border-radius:8px; border-left:4px solid {vol_color};">
                    <div style="color:#666; font-size:12px; margin-bottom:8px;">Risk Level</div>
                    <div style="color:{vol_color}; font-size:28px; font-weight:bold;">{level}</div>
                </div>
                """,
                    unsafe_allow_html=True,
                )

            st.markdown("<br>", unsafe_allow_html=True)
            st.caption(f"Last updated: {timestamp}")
        else:
            st.warning(f"No prediction available for {symbol}")

        st.markdown("---")

        # Alerts Section
        st.markdown("### Market Alerts")

        tab1, tab2, tab3, tab4 = st.tabs(["Price Spikes", "Volume Spikes", "Trade Spikes", "Buy/Sell Imbalance"])

        with tab1:
            st.caption("Price changes > 2% in 1 minute")
            price_spikes = get_price_spikes(20)
            if price_spikes:
                df = pd.DataFrame(price_spikes)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df = df.sort_values("timestamp", ascending=False)

                st.dataframe(
                    df[["timestamp", "symbol", "open_price", "close_price", "price_change_pct"]],
                    use_container_width=True,
                    hide_index=True,
                    height=300,
                    column_config={
                        "timestamp": st.column_config.DatetimeColumn("Time", format="MM-DD HH:mm"),
                        "symbol": st.column_config.TextColumn("Symbol"),
                        "open_price": st.column_config.NumberColumn("Open", format="$%.2f"),
                        "close_price": st.column_config.NumberColumn("Close", format="$%.2f"),
                        "price_change_pct": st.column_config.NumberColumn("Change %", format="%.2f%%"),
                    },
                )
            else:
                st.info("No price spikes detected")

        with tab2:
            st.caption("Quote volume > $1M in 1 minute")
            volume_spikes = get_volume_spikes(20)
            if volume_spikes:
                df = pd.DataFrame(volume_spikes)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df = df.sort_values("timestamp", ascending=False)

                st.dataframe(
                    df[["timestamp", "symbol", "volume", "quote_volume", "trade_count"]],
                    use_container_width=True,
                    hide_index=True,
                    height=300,
                    column_config={
                        "timestamp": st.column_config.DatetimeColumn("Time", format="MM-DD HH:mm"),
                        "symbol": st.column_config.TextColumn("Symbol"),
                        "volume": st.column_config.NumberColumn("Volume", format="%.4f"),
                        "quote_volume": st.column_config.NumberColumn("Quote Vol", format="$%.0f"),
                        "trade_count": st.column_config.NumberColumn("Trades"),
                    },
                )
            else:
                st.info("No volume spikes detected")

        with tab3:
            st.caption("Abnormal trade count spike")
            trade_spikes = get_trade_count_spikes(20)
            if trade_spikes:
                df = pd.DataFrame(trade_spikes)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df = df.sort_values("timestamp", ascending=False)

                st.dataframe(
                    df[["timestamp", "symbol", "trade_count", "buy_count", "sell_count"]],
                    use_container_width=True,
                    hide_index=True,
                    height=300,
                    column_config={
                        "timestamp": st.column_config.DatetimeColumn("Time", format="MM-DD HH:mm"),
                        "symbol": st.column_config.TextColumn("Symbol"),
                        "trade_count": st.column_config.NumberColumn("Total Trades"),
                        "buy_count": st.column_config.NumberColumn("Buy"),
                        "sell_count": st.column_config.NumberColumn("Sell"),
                    },
                )
            else:
                st.info("No trade count spikes detected")

        with tab4:
            st.caption("Significant buy/sell ratio imbalance")
            imbalances = get_buy_sell_imbalance(20)
            if imbalances:
                df = pd.DataFrame(imbalances)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df = df.sort_values("timestamp", ascending=False)

                st.dataframe(
                    df[["timestamp", "symbol", "buy_count", "sell_count", "buy_sell_ratio", "imbalance_direction"]],
                    use_container_width=True,
                    hide_index=True,
                    height=300,
                    column_config={
                        "timestamp": st.column_config.DatetimeColumn("Time", format="MM-DD HH:mm"),
                        "symbol": st.column_config.TextColumn("Symbol"),
                        "buy_count": st.column_config.NumberColumn("Buy"),
                        "sell_count": st.column_config.NumberColumn("Sell"),
                        "buy_sell_ratio": st.column_config.NumberColumn("Ratio", format="%.2f"),
                        "imbalance_direction": st.column_config.TextColumn("Direction"),
                    },
                )
            else:
                st.info("No buy/sell imbalances detected")


render()

time.sleep(10)
st.rerun()
