"""
TXF Ticks Database 管理模組

功能：
1. 接收並儲存 Shioaji ticks 到 SQLite
2. 從 database 讀取 ticks 並組成 K 棒
3. 支援日盤/夜盤/全盤過濾
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import pytz
from pathlib import Path

# Database 路徑
DB_PATH = Path(__file__).parent / "data" / "txf_ticks.db"

def init_database():
    """初始化 ticks database"""
    DB_PATH.parent.mkdir(exist_ok=True)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 創建 ticks 表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            ts TIMESTAMP PRIMARY KEY,
            code TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            bid_price REAL,
            ask_price REAL,
            bid_volume INTEGER,
            ask_volume INTEGER
        )
    """)
    
    # 創建索引加速查詢
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_ts ON ticks(ts)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_code ON ticks(code)")
    
    conn.commit()
    conn.close()
    
    return True

def save_tick(tick_data):
    """
    儲存單筆 tick 到 database（不建議大量使用，請用 save_ticks_batch）
    """
    return save_ticks_batch([tick_data])

def save_ticks_batch(ticks_list):
    """
    批次儲存多筆 ticks 到 database（效能更好）
    
    參數:
        ticks_list (list): tick_data 字典的列表
    """
    if not ticks_list:
        return True
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        batch_data = []
        for tick_data in ticks_list:
            # 確保時間戳為 UTC 格式字串
            ts = tick_data.get('ts')
            if isinstance(ts, datetime):
                if ts.tzinfo is None:
                    ts_str = ts.isoformat() + 'Z'
                else:
                    ts_utc = ts.astimezone(pytz.UTC)
                    ts_str = ts_utc.isoformat()
            else:
                ts_str = str(ts)
            
            batch_data.append((
                ts_str,
                tick_data.get('code', 'TXF'),
                tick_data.get('open'),
                tick_data.get('high'),
                tick_data.get('low'),
                tick_data.get('close'),
                tick_data.get('volume', 0),
                tick_data.get('bid_price'),
                tick_data.get('ask_price'),
                tick_data.get('bid_volume'),
                tick_data.get('ask_volume')
            ))
        
        cursor.executemany("""
            INSERT OR REPLACE INTO ticks 
            (ts, code, open, high, low, close, volume, bid_price, ask_price, bid_volume, ask_volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch_data)
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ 批次儲存 ticks 失敗: {e}")
        return False

def get_ticks(start_time, end_time, code='TXF'):
    """
    從 database 讀取指定時間範圍的 ticks
    
    參數:
        start_time (datetime): 開始時間（可以是任意時區）
        end_time (datetime): 結束時間（可以是任意時區）
        code (str): 合約代碼
        
    返回:
        pd.DataFrame: ticks 數據（時間索引為 Asia/Taipei）
    """
    try:
        # 將查詢時間轉為 UTC（因為 database 儲存為 UTC）
        if start_time.tzinfo is not None:
            start_time_utc = start_time.astimezone(pytz.UTC)
        else:
            start_time_utc = pytz.timezone('Asia/Taipei').localize(start_time).astimezone(pytz.UTC)
            
        if end_time.tzinfo is not None:
            end_time_utc = end_time.astimezone(pytz.UTC)
        else:
            end_time_utc = pytz.timezone('Asia/Taipei').localize(end_time).astimezone(pytz.UTC)
        
        conn = sqlite3.connect(DB_PATH)
        
        # 如果 code 為 'TXF'，查詢所有 TXF 相關合約（TXF, TXFR1, TXFR2 等）
        if code == 'TXF':
            query = """
                SELECT ts, open, high, low, close, volume
                FROM ticks
                WHERE code LIKE 'TXF%' AND ts BETWEEN ? AND ?
                ORDER BY ts
            """
            df = pd.read_sql_query(
                query,
                conn,
                params=(start_time_utc.isoformat(), end_time_utc.isoformat())
            )
        else:
            query = """
                SELECT ts, open, high, low, close, volume
                FROM ticks
                WHERE code = ? AND ts BETWEEN ? AND ?
                ORDER BY ts
            """
            df = pd.read_sql_query(
                query,
                conn,
                params=(code, start_time_utc.isoformat(), end_time_utc.isoformat())
            )
        
        conn.close()
        
        if not df.empty:
            # 將字串轉為 datetime，使用 mixed 格式處理不同的時間格式
            df['ts'] = pd.to_datetime(df['ts'], format='mixed', utc=True).dt.tz_convert('Asia/Taipei')
            df.set_index('ts', inplace=True)
        
        return df
    except Exception as e:
        print(f"❌ 讀取 ticks 失敗: {e}")
        return pd.DataFrame()

def resample_ticks_to_kbars(ticks_df, interval='1d', session='全盤'):
    """
    將 ticks 重採樣為 K 棒
    
    參數:
        ticks_df (pd.DataFrame): ticks 數據
        interval (str): K線週期 (1m, 5m, 15m, 30m, 60m, 1d)
        session (str): 交易時段 (日盤, 夜盤, 全盤)
        
    返回:
        pd.DataFrame: K 棒數據
    """
    if ticks_df.empty:
        return pd.DataFrame()
    
    # 過濾時段
    if session != "全盤":
        hours = ticks_df.index.hour
        minutes = ticks_df.index.minute
        
        if session == "日盤":
            # 日盤：08:45 - 13:45（包含13:45收盤）
            mask = ((hours == 8) & (minutes >= 45)) | \
                   ((hours >= 9) & (hours < 13)) | \
                   ((hours == 13) & (minutes <= 45))
        else:  # 夜盤
            # 夜盤：15:00 - 05:00
            mask = (hours >= 15) | (hours < 5)
        
        ticks_df = ticks_df[mask]
        
        if ticks_df.empty:
            return pd.DataFrame()
    
    # 重採樣規則
    resample_rules = {
        '1m': '1min',
        '5m': '5min',
        '15m': '15min',
        '30m': '30min',
        '60m': '60min',
        '1d': '1D'
    }
    
    rule = resample_rules.get(interval, '1D')
    
    # 重採樣為 K 棒
    kbars = ticks_df.resample(rule).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    # 標準化欄位名稱
    kbars.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    
    return kbars

def get_kbars_from_db(interval='1d', session='日盤', days=30, code='TXF'):
    """
    從 database 讀取並組成 K 棒（主要接口）
    
    參數:
        interval (str): K線週期
        session (str): 交易時段
        days (int): 回溯天數
        code (str): 合約代碼（預設 'TXF' 會查詢所有 TXF 相關合約）
        
    返回:
        pd.DataFrame: K 棒數據
    """
    taipei_tz = pytz.timezone('Asia/Taipei')
    end_time = datetime.now(taipei_tz)
    start_time = end_time - timedelta(days=days)
    
    # 讀取 ticks
    ticks_df = get_ticks(start_time, end_time, code=code)
    
    if ticks_df.empty:
        return None
    
    # 組成 K 棒
    kbars_df = resample_ticks_to_kbars(ticks_df, interval, session)
    
    return kbars_df

# 初始化 database
init_database()
