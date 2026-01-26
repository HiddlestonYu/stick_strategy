import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf
import pandas as pd
import numpy as np
from scipy.signal import argrelextrema

# --- 1. 頁面設定 ---
st.set_page_config(layout="wide", page_title="台指期程式交易看盤室")
st.title("📈 台指期全盤 K線圖 (含 10MA/20MA)")

# --- 2. 側邊欄控制項 ---
with st.sidebar:
    st.header("參數設定")
    
    # 商品選擇
    product_option = st.selectbox(
        "選擇商品",
        ("台指期 (模擬)", "台積電 (2330.TW)", "台灣加權指數 (^TWII)"),
        index=0
    )
    
    # 時段選擇
    session_option = st.selectbox(
        "選擇時段",
        ("全盤", "日盤", "夜盤"),
        index=0
    )
    
    # 模擬軟體的週期切換
    interval_option = st.selectbox(
        "選擇 K 線週期",
        ("1m", "5m", "15m", "30m", "60m", "1d"),
        index=5  # 預設日K
    )
    
    # 數據期間設定
    data_mode = st.radio(
        "數據模式",
        ("預設期間", "自定義日期"),
        index=0,
        horizontal=True
    )
    
    if data_mode == "預設期間":
        if interval_option == "1d":
            period_option = st.selectbox(
                "數據期間",
                ("1mo", "3mo", "6mo", "1y", "2y", "5y", "max"),
                index=2  # 預設 6個月
            )
            start_date = None
            end_date = None
        elif interval_option in ["1m", "5m"]:
            period_option = st.selectbox(
                "數據期間",
                ("1d", "5d", "1mo"),
                index=2  # 預設 1個月
            )
            start_date = None
            end_date = None
        else:  # 15m, 30m, 60m
            period_option = st.selectbox(
                "數據期間",
                ("5d", "1mo", "3mo"),
                index=2  # 預設 3個月
            )
            start_date = None
            end_date = None
    else:  # 自定義日期
        period_option = None
        col_date1, col_date2 = st.columns(2)
        with col_date1:
            start_date = st.date_input(
                "開始日期",
                value=pd.Timestamp.now() - pd.DateOffset(months=6)
            )
        with col_date2:
            end_date = st.date_input(
                "結束日期",
                value=pd.Timestamp.now()
            )
    
    # Y軸範圍設定
    y_axis_mode = st.radio(
        "Y軸範圍",
        ("自動縮放", "固定範圍"),
        index=0
    )
    
    if y_axis_mode == "固定範圍":
        col_y1, col_y2 = st.columns(2)
        with col_y1:
            y_min = st.number_input("最小值", value=28000, step=100)
        with col_y2:
            y_max = st.number_input("最大值", value=30000, step=100)
    else:
        y_min, y_max = None, None
    
    # 支撐壓力線設定
    show_support_resistance = st.checkbox("顯示支撐壓力線", value=True)
    if show_support_resistance:
        sr_sensitivity = st.slider("靈敏度（數值越小，線越少）", min_value=5, max_value=30, value=10, step=5)
    else:
        sr_sensitivity = None
    
    st.info("💡 提示：實戰中建議使用 Shioaji API 接收 Tick 資料並即時合成 K 棒。")
    st.info(f"📊 當前時段：{session_option}")
    if data_mode == "預設期間":
        st.info(f"📅 數據範圍：{period_option} | 可拖曳圖表查看不同時間的數據")
    else:
        st.info(f"📅 自定義日期：{start_date} 至 {end_date}")

# --- 3. 數據獲取與處理 (Data Handler) ---
def get_ticker_symbol(product):
    """根據商品選擇返回對應的股票代碼"""
    if product == "台指期 (模擬)":
        return "^TWII"  # 使用台灣加權指數模擬台指期
    elif product == "台積電 (2330.TW)":
        return "2330.TW"
    elif product == "台灣加權指數 (^TWII)":
        return "^TWII"
    return "^TWII"

def filter_by_session(df, session):
    """根據選擇的時段過濾數據"""
    if df is None or df.empty:
        return df
    
    # 確保索引是台灣時間
    if df.index.tz is None:
        df.index = df.index.tz_localize('Asia/Taipei')
    
    # 取得小時和分鐘
    hours = df.index.hour
    minutes = df.index.minute
    
    if session == "日盤":
        # 日盤：08:45 - 13:45
        mask = ((hours == 8) & (minutes >= 45)) | \
               ((hours >= 9) & (hours < 13)) | \
               ((hours == 13) & (minutes <= 45))
        return df[mask]
    elif session == "夜盤":
        # 夜盤：15:00 - 次日 05:00
        mask = (hours >= 15) | (hours < 5)
        return df[mask]
    else:  # 全盤
        return df

def find_support_resistance(df, order=10):
    """
    找出支撐和壓力位
    order: 用於檢測局部極值的窗口大小
    """
    if df is None or df.empty or len(df) < order * 2:
        return [], []
    
    # 找出局部高點（壓力位）
    high_indices = argrelextrema(df['High'].values, np.greater, order=order)[0]
    resistance_levels = []
    for idx in high_indices:
        resistance_levels.append({
            'price': df['High'].iloc[idx],
            'date': df.index[idx]
        })
    
    # 找出局部低點（支撐位）
    low_indices = argrelextrema(df['Low'].values, np.less, order=order)[0]
    support_levels = []
    for idx in low_indices:
        support_levels.append({
            'price': df['Low'].iloc[idx],
            'date': df.index[idx]
        })
    
    # 合併相近的支撐/壓力位
    def merge_levels(levels, threshold=0.005):
        if not levels:
            return []
        
        sorted_levels = sorted(levels, key=lambda x: x['price'])
        merged = [sorted_levels[0]]
        
        for level in sorted_levels[1:]:
            if abs(level['price'] - merged[-1]['price']) / merged[-1]['price'] < threshold:
                # 價格相近，取平均
                merged[-1]['price'] = (merged[-1]['price'] + level['price']) / 2
            else:
                merged.append(level)
        
        return merged
    
    resistance_levels = merge_levels(resistance_levels)
    support_levels = merge_levels(support_levels)
    
    return support_levels, resistance_levels
    """
    找出支撐和壓力位
    order: 用於檢測局部極值的窗口大小
    """
    if df is None or df.empty or len(df) < order * 2:
        return [], []
    
    # 找出局部高點（壓力位）
    high_indices = argrelextrema(df['High'].values, np.greater, order=order)[0]
    resistance_levels = []
    for idx in high_indices:
        resistance_levels.append({
            'price': df['High'].iloc[idx],
            'date': df.index[idx]
        })
    
    # 找出局部低點（支撐位）
    low_indices = argrelextrema(df['Low'].values, np.less, order=order)[0]
    support_levels = []
    for idx in low_indices:
        support_levels.append({
            'price': df['Low'].iloc[idx],
            'date': df.index[idx]
        })
    
    # 合併相近的支撐/壓力位
    def merge_levels(levels, threshold=0.005):
        if not levels:
            return []
        
        sorted_levels = sorted(levels, key=lambda x: x['price'])
        merged = [sorted_levels[0]]
        
        for level in sorted_levels[1:]:
            if abs(level['price'] - merged[-1]['price']) / merged[-1]['price'] < threshold:
                # 價格相近，取平均
                merged[-1]['price'] = (merged[-1]['price'] + level['price']) / 2
            else:
                merged.append(level)
        
        return merged
    
    resistance_levels = merge_levels(resistance_levels)
    support_levels = merge_levels(support_levels)
    
    return support_levels, resistance_levels

@st.cache_data(ttl=60)  # 設定快取，避免重複請求
def get_data(interval, product, session, period, start_date=None, end_date=None):
    # 這裡使用 Yahoo Finance 模擬
    # 實戰時請替換為 Shioaji: api.kline(contract, min_volume=1)
    ticker = get_ticker_symbol(product)
    
    # 下載數據
    try:
        if start_date and end_date:
            # 使用自定義日期範圍
            df = yf.download(ticker, start=start_date, end=end_date, interval=interval, progress=False)
        else:
            # 使用預設期間
            df = yf.download(ticker, period=period, interval=interval, progress=False)
    except Exception as e:
        st.error(f"數據下載失敗: {e}")
        return None
    
    if df.empty:
        st.warning(f"無法取得 {ticker} 的數據")
        return None
        return None
    
    # 處理多層索引 - yfinance 有時會返回 (Price, Ticker) 的多層索引
    if isinstance(df.columns, pd.MultiIndex):
        # 取第一層（Price: Open, High, Low, Close, Volume）
        df.columns = df.columns.get_level_values(0)
    
    # 確保欄位名稱是字串並標準化
    df.columns = [str(col).strip() for col in df.columns]
    
    # 標準化欄位名稱（首字母大寫）
    column_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower == 'open':
            column_map[col] = 'Open'
        elif col_lower == 'high':
            column_map[col] = 'High'
        elif col_lower == 'low':
            column_map[col] = 'Low'
        elif col_lower == 'close':
            column_map[col] = 'Close'
        elif col_lower == 'volume':
            column_map[col] = 'Volume'
        elif 'adj' in col_lower and 'close' in col_lower:
            column_map[col] = 'Adj Close'
    
    df.rename(columns=column_map, inplace=True)
    
    # 檢查必要欄位
    required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
    
    # C. 繪製支撐壓力線
    if show_support_resistance:
        # 繪製支撐線（綠色虛線）
        for level in support_levels:
    # E. 圖表美化 (模擬看盤軟體風格)e(
                y=level['price'], 
                line_dash="dash", 
                line_color="green", 
                line_width=1,
                opacity=0.6,
                annotation_text=f"支撐 {level['price']:.0f}",
                annotation_position="right",
                annotation_font_size=10,
                annotation_font_color="green",
                row=1, col=1
            )
        
        # 繪製壓力線（紅色虛線）
        for level in resistance_levels:
            fig.add_hline(
                y=level['price'], 
                line_dash="dash", 
                line_color="red", 
                line_width=1,
                opacity=0.6,
                annotation_text=f"壓力 {level['price']:.0f}",
                annotation_position="right",
                annotation_font_size=10,
                annotation_font_color="red",
                row=1, col=1
            )

    # D st.info(f"實際欄位: {list(df.columns)}")
        return None
    
    # 轉換時區
    try:
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC').tz_convert('Asia/Taipei')
        else:
            df.index = df.index.tz_convert('Asia/Taipei')
    except Exception:
        pass
    
    # 根據時段過濾數據
    df = filter_by_session(df, session)
    
    if df is None or df.empty:
        st.warning(f"過濾 {session} 後無數據")
        return None, start_date, end_date
    
    # --- 計算技術指標 (MA) ---
    df['MA10'] = df['Close'].rolling(window=10).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    
    return df

df = get_data(interval_option, product_option, sess
    
    # 顯示支撐壓力位統計
    if show_support_resistance and (support_levels or resistance_levels):
        st.markdown("---")
        col_sr1, col_sr2 = st.columns(2)
        with col_sr1:
            st.markdown("### 🟢 支撐位")
            if support_levels:
                for level in sorted(support_levels, key=lambda x: x['price'], reverse=True)[:5]:
                    distance = ((last_row['Close'] - level['price']) / level['price'] * 100)
                    st.write(f"**{level['price']:.0f}** ({distance:+.2f}%)")
            else:
                st.write("無明顯支撐位")
        
        with col_sr2:
            st.markdown("### 🔴 壓力位")
            if resistance_levels:
                for level in sorted(resistance_levels, key=lambda x: x['price'])[:5]:
                    distance = ((level['price'] - last_row['Close']) / last_row['Close'] * 100)
                    st.write(f"**{level['price']:.0f}** ({distance:+.2f}%)")
            else:
                st.write("無明顯壓力位")ion_option, period_option)

# --- 4. 繪製互動式 K 線圖 (Visualization) ---
if df is not None:
    # 建立雙軸圖表 (K線 + 成交量)
    fig = make_subplots(
        rows=2, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.03, 
        subplot_titles=('K 線與均線', '成交量'),
        row_width=[0.2, 0.7]
    )

    # A. 繪製 K 棒 (符合台灣習慣：紅漲綠跌)
    candlestick = go.Candlestick(
        x=df.index,
        open=df['Open'],
        high=df['High'],
        low=df['Low'],
        close=df['Close'],
        name='K棒',
        increasing_line_color='red', 
        decreasing_line_color='green'
    )
    fig.add_trace(candlestick, row=1, col=1)

    # B. 繪製 MA 線
    fig.add_trace(go.Scatter(x=df.index, y=df['MA10'], line=dict(color='orange', width=1.5), name='10 MA'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df['MA20'], line=dict(color='purple', width=1.5), name='20 MA'), row=1, col=1)

    # C. 繪製成交量
    colors = ['red' if row['Open'] - row['Close'] >= 0 else 'green' for index, row in df.iterrows()]
    fig.add_trace(go.Bar(x=df.index, y=df['Volume'], marker_color=colors, name='成交量'), row=2, col=1)

    # D. 圖表美化 (模擬看盤軟體風格)
    fig.update_layout(
        xaxis_rangeslider_visible=False,  # 隱藏下方滑桿以節省空間
        height=700,
        plot_bgcolor='rgb(20, 20, 20)',  # 深色背景
        paper_bgcolor='rgb(20, 20, 20)',
        font=dict(color='white'),
        title_text=f"{product_option} - {session_option} - {interval_option} K線圖",
        hovermode='x unified',  # 游標十字線
        dragmode='pan'  # 啟用拖曳平移功能
    )
    
    # 設定 X 軸可拖曳和縮放
    fig.update_xaxes(
        fixedrange=False,  # 允許 X 軸縮放和拖曳
        row=1, col=1
    )
    fig.update_xaxes(
        fixedrange=False,  # 允許 X 軸縮放和拖曳
        row=2, col=1
    )
    
    # 移除沒有交易的時間段（週末、假日等）
    # 使用 rangebreaks 來移除沒有數據的時間點
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),  # 隱藏週末
            dict(values=df.index[df['Volume'] == 0].tolist() if 'Volume' in df.columns else [])  # 隱藏成交量為0的時間
        ],
        row=1, col=1
    )
    
    fig.update_xaxes(
        rangebreaks=[
            dict(bounds=["sat", "mon"]),  # 隱藏週末
            dict(values=df.index[df['Volume'] == 0].tolist() if 'Volume' in df.columns else [])  # 隱藏成交量為0的時間
        ],
        row=2, col=1
    )
    
    # 設定 Y 軸範圍
    if y_axis_mode == "固定範圍":
        fig.update_yaxes(range=[y_min, y_max], row=1, col=1)
    
    # 顯示圖表
    st.plotly_chart(fig, width='stretch')

    # --- 5. 最新報價資訊 ---
    last_row = df.iloc[-1]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("最新收盤", f"{last_row['Close']:.0f}")
    col2.metric("10 MA", f"{last_row['MA10']:.0f}")
    col3.metric("20 MA", f"{last_row['MA20']:.0f}")
    col4.metric("成交量", f"{last_row['Volume']:.0f}")

else:
    st.error("目前無法獲取數據，請確認市場是否開盤或檢查網路連線。")
