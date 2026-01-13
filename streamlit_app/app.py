import streamlit as st

st.set_page_config(
    page_title="Crypto Pipeline",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
.main-title { font-size: 2.5em; font-weight: bold; color: #1976D2; margin-bottom: 0; }
.sub-title { color: #666; font-size: 1.1em; margin-top: 0; }
.arch-box { background: #F5F5F5; padding: 20px; border-radius: 8px; margin: 10px 0; }
.page-card { background: #F5F5F5; padding: 15px; border-radius: 8px; margin: 5px 0; }
</style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-title">📊 Crypto Streaming Pipeline</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-title">Real-time Cryptocurrency Data Pipeline Dashboard</p>', unsafe_allow_html=True)

st.markdown("---")

st.markdown("### 🏗️ Architecture")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="arch-box">
    <strong style="color:#1976D2">📥 Data Flow</strong><br><br>
    <code>Binance WebSocket</code><br>
    ↓<br>
    <code>Apache Kafka</code><br>
    ↓<br>
    <code>Apache Spark</code> (1m OHLCV aggregation)<br>
    ↓<br>
    <code>PostgreSQL</code> + <code>Redis</code>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div class="arch-box">
    <strong style="color:#2E7D32">🛠️ Tech Stack</strong><br><br>
    • <strong>Kafka</strong> - Message streaming<br>
    • <strong>Spark</strong> - Stream processing<br>
    • <strong>TimescaleDB</strong> - Time-series storage<br>
    • <strong>Redis</strong> - Real-time cache<br>
    • <strong>FastAPI</strong> - REST API<br>
    • <strong>ML Model</strong> - Volatility prediction
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

st.markdown("### 📑 Pages")

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("""
    <div class="page-card">
    <strong>📈 Market Overview</strong><br>
    <span style="color:#666; font-size:0.9em;">Live prices, top movers, all tickers</span>
    </div>
    """, unsafe_allow_html=True)
    st.page_link("pages/1_Market_Overview.py", label="Go to Market Overview →")

with col2:
    st.markdown("""
    <div class="page-card">
    <strong>🔍 Symbol Analysis</strong><br>
    <span style="color:#666; font-size:0.9em;">Price, Volume, Trades, Buy/Sell charts</span>
    </div>
    """, unsafe_allow_html=True)
    st.page_link("pages/2_Symbol_Deep_Dive.py", label="Go to Symbol Analysis →")

with col3:
    st.markdown("""
    <div class="page-card">
    <strong>🔮 Prediction & Alerts</strong><br>
    <span style="color:#666; font-size:0.9em;">ML volatility prediction, price/volume spikes</span>
    </div>
    """, unsafe_allow_html=True)
    st.page_link("pages/3_Prediction.py", label="Go to Prediction →")

st.markdown("---")
st.caption("Data refreshes every 5 seconds • Technical metrics available in Grafana at :3000")
