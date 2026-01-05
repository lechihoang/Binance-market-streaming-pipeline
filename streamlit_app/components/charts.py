import plotly.graph_objects as go
import pandas as pd


def price_chart(data: list[dict], symbol: str) -> go.Figure:
    if not data:
        return go.Figure().update_layout(title=f"{symbol} - No data")
    
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["timestamp"],
        y=df["close"],
        mode="lines",
        name="Price",
        line=dict(color="#2E7D32", width=2)
    ))
    
    fig.update_layout(
        title=f"{symbol} Price",
        xaxis_title="Time",
        yaxis_title="Price (USDT)",
        template="plotly_white",
        height=350,
        margin=dict(l=50, r=30, t=50, b=50)
    )
    return fig


def volume_chart(data: list[dict], symbol: str) -> go.Figure:
    if not data:
        return go.Figure().update_layout(title=f"{symbol} - No data")
    
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["timestamp"],
        y=df["volume"],
        name="Volume",
        marker_color="#1976D2"
    ))
    
    fig.update_layout(
        title=f"{symbol} Volume",
        xaxis_title="Time",
        yaxis_title="Volume",
        template="plotly_white",
        height=350,
        margin=dict(l=50, r=30, t=50, b=50)
    )
    return fig


def trades_chart(data: list[dict], symbol: str) -> go.Figure:
    if not data:
        return go.Figure().update_layout(title=f"{symbol} - No data")
    
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["timestamp"],
        y=df["trade_count"],
        name="Trades",
        marker_color="#F57C00"
    ))
    
    fig.update_layout(
        title=f"{symbol} Trade Count",
        xaxis_title="Time",
        yaxis_title="Trades",
        template="plotly_white",
        height=350,
        margin=dict(l=50, r=30, t=50, b=50)
    )
    return fig


def buy_sell_chart(data: list[dict], symbol: str) -> go.Figure:
    if not data:
        return go.Figure().update_layout(title=f"{symbol} - No data")
    
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["timestamp"],
        y=df.get("buy_count", [0] * len(df)),
        mode="lines",
        name="Buy",
        fill="tozeroy",
        line=dict(color="#4CAF50")
    ))
    fig.add_trace(go.Scatter(
        x=df["timestamp"],
        y=df.get("sell_count", [0] * len(df)),
        mode="lines",
        name="Sell",
        fill="tozeroy",
        line=dict(color="#F44336")
    ))
    
    fig.update_layout(
        title=f"{symbol} Buy/Sell",
        xaxis_title="Time",
        yaxis_title="Count",
        template="plotly_white",
        height=350,
        margin=dict(l=50, r=30, t=50, b=50)
    )
    return fig


def candlestick_chart(data: list[dict], symbol: str) -> go.Figure:
    if not data:
        return go.Figure().update_layout(title=f"{symbol} - No data")
    
    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    
    fig = go.Figure(data=[go.Candlestick(
        x=df["timestamp"],
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
        name=symbol
    )])
    
    fig.update_layout(
        title=f"{symbol} OHLC",
        xaxis_title="Time",
        yaxis_title="Price (USDT)",
        template="plotly_white",
        height=400,
        margin=dict(l=50, r=30, t=50, b=50),
        xaxis_rangeslider_visible=False
    )
    return fig
