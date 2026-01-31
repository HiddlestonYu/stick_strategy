"""
插入測試數據 - 2026/01/30 日盤數據
開：32456  高：32526  低：32092  收：32247
"""

from tick_database import init_database, save_tick
from datetime import datetime, timedelta
import pytz
import random
import sqlite3
from pathlib import Path

# 初始化 database
init_database()
print("[OK] Database initialized\n")

# 清空舊數據
DB_PATH = Path(__file__).parent / "data" / "txf_ticks.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute("DELETE FROM ticks")
conn.commit()
conn.close()
print("[OK] Old data cleared\n")

# 台灣時區
taipei_tz = pytz.timezone('Asia/Taipei')

# 2026/01/30 日盤數據（使用 localize 避免 LMT 問題）
date = taipei_tz.localize(datetime(2026, 1, 30))

# 日盤時間：08:45 - 13:45
start_time = date.replace(hour=8, minute=45, second=0)
end_time = date.replace(hour=13, minute=45, second=0)

# 實際數據
actual_data = {
    'open': 32456,
    'high': 32526,
    'low': 32092,
    'close': 32247
}

print("\nInserting 2026/01/30 day session data...")
print(f"Open: {actual_data['open']}  High: {actual_data['high']}  Low: {actual_data['low']}  Close: {actual_data['close']}")

# 模擬每分鐘 tick（日盤共 300 分鐘）
current_time = start_time
current_price = actual_data['open']

tick_count = 0
while current_time <= end_time:
    # 生成該分鐘的 OHLC
    # 確保整體趨勢符合實際數據
    progress = (current_time - start_time).total_seconds() / (end_time - start_time).total_seconds()
    
    # 最後一筆使用精確收盤價
    if current_time == end_time:
        minute_close = actual_data['close']
    else:
        # 從開盤價逐漸走到收盤價，中間經過最高和最低
        if progress < 0.3:
            # 前30%：上漲到最高
            target = actual_data['open'] + (actual_data['high'] - actual_data['open']) * (progress / 0.3)
        elif progress < 0.6:
            # 中30%：從最高下跌到最低
            target = actual_data['high'] + (actual_data['low'] - actual_data['high']) * ((progress - 0.3) / 0.3)
        else:
            # 後40%：從最低回升到收盤
            target = actual_data['low'] + (actual_data['close'] - actual_data['low']) * ((progress - 0.6) / 0.4)
        
        # 加入隨機波動
        variation = random.uniform(-20, 20)
        minute_close = target + variation
        
        # 確保不超過最高最低限制
        minute_close = max(actual_data['low'], min(actual_data['high'], minute_close))
    
    minute_open = current_price
    minute_high = max(minute_open, minute_close) + random.uniform(0, 10)
    minute_low = min(minute_open, minute_close) - random.uniform(0, 10)
    
    # 確保整體限制
    minute_high = min(minute_high, actual_data['high'])
    minute_low = max(minute_low, actual_data['low'])
    
    tick_data = {
        'ts': current_time,  # 直接使用 datetime 物件而非 isoformat
        'code': 'TXFR1',
        'open': round(minute_open),
        'high': round(minute_high),
        'low': round(minute_low),
        'close': round(minute_close),
        'volume': random.randint(50, 500),
        'bid_price': round(minute_close - 1),
        'ask_price': round(minute_close + 1),
        'bid_volume': random.randint(10, 50),
        'ask_volume': random.randint(10, 50)
    }
    
    save_tick(tick_data)
    current_price = minute_close
    current_time += timedelta(minutes=1)
    tick_count += 1

print(f"[OK] Completed! Inserted {tick_count} tick records")
print(f"\nVerification: Run streamlit, select day session + 1d K, check 2026/01/30 data")
