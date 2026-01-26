"""Component for displaying recent trades feed."""

import pandas as pd
import streamlit as st

COLORS = {
    "green": "#2E7D32",
    "red": "#D32F2F",
    "bg": "#FFFFFF",
    "card": "#F5F5F5",
}


def render_trades_feed(trades: list[dict], height: int = 400, show_symbol: bool = False):
    """Render recent trades feed with color-coded buy/sell sides.

    Args:
        trades: List of trade dicts with timestamp, price, quantity, side
        height: Table height in pixels
        show_symbol: Whether to show symbol column (for multi-symbol feeds)
    """
    if not trades:
        st.info("No recent trades available")
        return

    df = pd.DataFrame(trades)

    # API returns ISO datetime string, convert to datetime for display
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Format display columns
    display_cols = {
        "timestamp": st.column_config.TimeColumn("Time", format="HH:mm:ss"),
        "price": st.column_config.NumberColumn("Price", format="$%.2f"),
        "quantity": st.column_config.NumberColumn("Qty", format="%.4f"),
        "total": st.column_config.NumberColumn("Total", format="$%.2f"),
        "side": st.column_config.TextColumn("Side", width="small"),
    }

    cols_to_show = ["timestamp", "price", "quantity", "total", "side"]

    if show_symbol and "symbol" in df.columns:
        cols_to_show.insert(1, "symbol")
        display_cols["symbol"] = st.column_config.TextColumn("Symbol", width="small")

    st.dataframe(
        df[cols_to_show],
        use_container_width=True,
        hide_index=True,
        height=height,
        column_config=display_cols,
    )


def metric_card(label: str, value: str, color: str):
    """Render a metric card with left border styling.

    Args:
        label: Card label text
        value: Card value text
        color: Border and value color
    """
    st.markdown(
        f"""
    <div style="background:{COLORS["card"]}; padding:20px; border-radius:8px; border-left:4px solid {color};">
        <div style="color:#666; font-size:12px; margin-bottom:8px;">{label}</div>
        <div style="color:{color}; font-size:28px; font-weight:bold;">{value}</div>
    </div>
    """,
        unsafe_allow_html=True,
    )


def render_trade_stats(trades: list[dict]):
    """Render quick stats about recent trades.

    Args:
        trades: List of trade dicts
    """
    if not trades:
        return

    df = pd.DataFrame(trades)

    total_volume = df["quantity"].sum()
    total_value = df["total"].sum()
    buy_trades = len(df[df["side"] == "BUY"])

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        metric_card("Total Trades", str(len(trades)), "#1976D2")

    with col2:
        metric_card("Volume", f"{total_volume:.4f}", "#2E7D32")

    with col3:
        metric_card("Total Value", f"${total_value:,.2f}", "#F57C00")

    with col4:
        buy_pct = (buy_trades / len(trades) * 100) if len(trades) > 0 else 0
        metric_card("Buy Ratio", f"{buy_pct:.1f}%", "#7B1FA2")
