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

from stock_city.project_paths import get_db_path

# Database 路徑（固定指向專案根目錄 /data，避免模組搬移導致路徑改變）
DB_PATH = get_db_path()

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

def get_latest_tick_timestamp(code='TXF', date=None):
    """
    取得最新一筆 tick 的時間戳（Asia/Taipei）
    
    參數:
        code (str): 合約代碼（'TXF' 代表所有 TXF 系列）
        date (datetime.date|None): 指定日期（台北時間）
        
    返回:
        datetime | None
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        if date is not None:
            taipei_tz = pytz.timezone('Asia/Taipei')
            start_local = taipei_tz.localize(datetime(date.year, date.month, date.day, 0, 0, 0))
            end_local = start_local + timedelta(days=1)
            start_utc = start_local.astimezone(pytz.UTC).isoformat()
            end_utc = end_local.astimezone(pytz.UTC).isoformat()
            
            if code == 'TXF':
                query = """
                    SELECT MAX(ts) FROM ticks
                    WHERE code LIKE 'TXF%' AND ts BETWEEN ? AND ?
                """
                cursor.execute(query, (start_utc, end_utc))
            else:
                query = """
                    SELECT MAX(ts) FROM ticks
                    WHERE code = ? AND ts BETWEEN ? AND ?
                """
                cursor.execute(query, (code, start_utc, end_utc))
        else:
            if code == 'TXF':
                query = "SELECT MAX(ts) FROM ticks WHERE code LIKE 'TXF%'"
                cursor.execute(query)
            else:
                query = "SELECT MAX(ts) FROM ticks WHERE code = ?"
                cursor.execute(query, (code,))
        
        row = cursor.fetchone()
        conn.close()
        
        if not row or row[0] is None:
            return None
        
        ts = pd.to_datetime(row[0], format='mixed', utc=True).tz_convert('Asia/Taipei')
        return ts
    except Exception as e:
        print(f"❌ 取得最新時間失敗: {e}")
        return None

def has_date_data(date, code='TXF'):
    """
    判斷指定日期是否已有資料（台北時間）
    """
    try:
        taipei_tz = pytz.timezone('Asia/Taipei')
        start_local = taipei_tz.localize(datetime(date.year, date.month, date.day, 0, 0, 0))
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(pytz.UTC).isoformat()
        end_utc = end_local.astimezone(pytz.UTC).isoformat()
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        if code == 'TXF':
            query = """
                SELECT COUNT(*) FROM ticks
                WHERE code LIKE 'TXF%' AND ts BETWEEN ? AND ?
            """
            cursor.execute(query, (start_utc, end_utc))
        else:
            query = """
                SELECT COUNT(*) FROM ticks
                WHERE code = ? AND ts BETWEEN ? AND ?
            """
            cursor.execute(query, (code, start_utc, end_utc))
        
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    except Exception as e:
        print(f"❌ 檢查日期資料失敗: {e}")
        return False

def resample_ticks_to_kbars(ticks_df, interval='1d', session='全盤'):
    """
    將 ticks 重採樣為 K 棒
    
    參數:
        ticks_df (pd.DataFrame): ticks 數據（注意：從 database 讀取的已經是 1分K 而非真正的 ticks）
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
        dates = ticks_df.index.date
        
        if session == "日盤":
            # 導入結算日判斷
            from stock_city.market.settlement_utils import is_settlement_day
            
            # 日盤：08:45 - 13:45（一般日）或 08:45 - 13:30（結算日）
            # 需要逐日判斷收盤時間
            mask = pd.Series(False, index=ticks_df.index)
            
            for date in pd.unique(dates):
                # 判斷當日是否為結算日
                end_minute = 30 if is_settlement_day(date) else 45
                
                # 該日的時間過濾
                date_mask = (dates == date) & (
                    ((hours == 8) & (minutes >= 45)) |
                    ((hours >= 9) & (hours < 13)) |
                    ((hours == 13) & (minutes <= end_minute))
                )
                mask |= date_mask
        else:  # 夜盤
            # 夜盤：15:00 - 05:00（包含 05:00 這一根）
            mask = (hours >= 15) | (hours < 5) | ((hours == 5) & (minutes == 0))
        
        ticks_df = ticks_df[mask]
        
        if ticks_df.empty:
            return pd.DataFrame()
    
    # 全盤日K：以「交易日」彙總（前一日 15:00 ~ 當日 13:45/13:30）
    # 避免夜盤被午夜切割、並支援結算日 13:30 收盤
    if interval == '1d' and session == '全盤':
        df = ticks_df.copy()
        idx = df.index

        if idx.tz is None:
            idx = idx.tz_localize('Asia/Taipei')
            df.index = idx

        hours = idx.hour
        minutes = idx.minute

        # 交易日歸屬：15:00~23:59 歸「隔日」；00:00~14:59 歸「當日」
        trade_date = pd.Series(idx.date, index=df.index)
        trade_date = trade_date.where(hours < 15, (idx + pd.Timedelta(days=1)).date)

        # 夜盤時間：15:00 - 05:00（含 05:00 這一根）
        night_mask = (hours >= 15) | (hours < 5) | ((hours == 5) & (minutes == 0))

        # 日盤時間：08:45 - 13:45（一般日）或 08:45 - 13:30（結算日）
        from stock_city.market.settlement_utils import is_settlement_day
        day_mask = pd.Series(False, index=df.index)
        dates = trade_date.values
        for d in pd.unique(dates):
            end_minute = 30 if is_settlement_day(d) else 45
            dm = (trade_date == d) & (
                ((hours == 8) & (minutes >= 45)) |
                ((hours >= 9) & (hours < 13)) |
                ((hours == 13) & (minutes <= end_minute))
            )
            day_mask |= dm

        df = df[night_mask | day_mask]
        if df.empty:
            return pd.DataFrame()

        group_idx = pd.to_datetime(trade_date.loc[df.index]).dt.tz_localize('Asia/Taipei')
        grouped = df.groupby(group_idx).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()

        grouped.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        grouped.index.name = 'ts'
        return grouped.sort_index()

    # 夜盤日K：以「交易時段」(15:00~隔日05:00) 彙總，避免被午夜切成兩根
    if interval == '1d' and session == '夜盤':
        df = ticks_df.copy()
        idx = df.index

        # 交易時段歸屬日：15:00~23:59 歸當日；00:00~05:00(含) 歸前一日
        hours = idx.hour
        session_date = pd.Series(idx.date, index=df.index)
        early_mask = hours < 15
        session_date = session_date.where(~early_mask, (idx - pd.Timedelta(days=1)).date)
        session_day = pd.to_datetime(session_date).dt.tz_localize('Asia/Taipei')

        grouped = df.groupby(session_day).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()

        grouped.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        grouped.index.name = 'ts'
        return grouped.sort_index()

    # 如果是 1 分 K，直接返回（不需要 resample，因為 database 中存的已經是 1分K）
    if interval == '1m':
        # 標準化欄位名稱
        result = ticks_df.copy()
        result.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        return result
    
    # 重採樣規則（5分K以上才需要 resample）
    resample_rules = {
        '5m': '5min',
        '15m': '15min',
        '30m': '30min',
        '60m': '60min',
        '1d': '1D'
    }
    
    rule = resample_rules.get(interval, '1D')
    
    # 重採樣為 K 棒
    # 券商常見以「右標籤」顯示（例如 15:05 代表 15:01~15:05），因此 5m/15m/30m/60m 採用 right/right。
    resample_kwargs = {}
    if rule != '1D':
        resample_kwargs = {'label': 'right', 'closed': 'right'}

    kbars = ticks_df.resample(rule, **resample_kwargs).agg({
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
