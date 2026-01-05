import streamlit as st

st.set_page_config(
    page_title="Crypto Pipeline",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
.main-title { font-size: 2.5em; font-weight: bold; color: #FF9800; margin-bottom: 0; }
.sub-title { color: #888; font-size: 1.1em; margin-top: 0; }
.arch-box { background: #1E1E1E; padding: 20px; border-radius: 8px; margin: 10px 0; }
</style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-title">Crypto Streaming Pipeline</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-title">Real-time Cryptocurrency Data Pipeline Dashboard</p>', unsafe_allow_html=True)

st.markdown("---")

st.markdown("### Architecture")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="arch-box">
    <strong style="color:#5794F2">Data Flow</strong><br><br>
    Binance WebSocket<br>
    ↓<br>
    Apache Kafka<br>
    ↓<br>
    Apache Spark (1m OHLCV)<br>
    ↓<br>
    PostgreSQL + Redis
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="arch-box">
    <strong style="color:#73BF69">Tech Stack</strong><br><br>
    • <strong>Kafka</strong> - Message streaming<br>
    • <strong>Spark</strong> - Stream processing<br>
    • <strong>TimescaleDB</strong> - Time-series storage<br>
    • <strong>Redis</strong> - Real-time cache<br>
    • <strong>FastAPI</strong> - REST API
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

st.markdown("### Pages")

col1, col2 = st.columns(2)
with col1:
    st.page_link("pages/1_Market_Overview.py", label="📈 Market Overview", icon="📈")
    st.caption("Live prices, top movers, all tickers")
with col2:
    st.page_link("pages/2_Symbol_Deep_Dive.py", label="🔍 Symbol Deep Dive", icon="🔍")
    st.caption("Price, Volume, Trades, Buy/Sell charts")

st.markdown("---")
st.caption("Data refreshes every 5 seconds • Technical metrics available in Grafana")
