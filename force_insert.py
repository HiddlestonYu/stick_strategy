"""
強制清空並重新插入數據 - 使用正確的 2026 年日期
"""
from datetime import datetime, timedelta
import pytz
import random
import sqlite3
from pathlib import Path
import time

DB_PATH = Path(__file__).parent / "data" / "txf_ticks.db"

# 等待資料庫解鎖
max_retries = 5
for attempt in range(max_retries):
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        cursor = conn.cursor()
        
        # 嘗試刪除舊數據
        cursor.execute("DELETE FROM ticks")
        conn.commit()
        print(f"[OK] Cleared old data (attempt {attempt + 1})")
        break
    except sqlite3.OperationalError as e:
        if attempt < max_retries - 1:
            print(f"Database locked, retrying in 2 seconds... ({attempt + 1}/{max_retries})")
            if conn:
                conn.close()
            time.sleep(2)
        else:
            print(f"[ERROR] Could not clear database: {e}")
            print("Please close Streamlit and run this script again")
            exit(1)

taipei_tz = pytz.timezone('Asia/Taipei')

# 使用 2026-01-30 作為最新日期
latest_date = taipei_tz.localize(datetime(2026, 1, 30))
print(f"\nGenerating 30 days ending on {latest_date.date()}...")

batch_data = []
days_count = 0

# 往前推 30 天
for days_back in range(29, -1, -1):
    current_date = latest_date - timedelta(days=days_back)
    
    # 跳過週末
    if current_date.weekday() >= 5:
        continue
    
    # 日盤：08:45-13:45
    start_time = current_date.replace(hour=8, minute=45, second=0)
    end_time = current_date.replace(hour=13, minute=45, second=0)
    
    # 生成當日價格（基於 32247）
    base_price = 32247 - (29 - days_back) * random.uniform(-30, 30)
    day_open = base_price + random.uniform(-100, 100)
    day_close = day_open + random.uniform(-200, 200)
    day_high = max(day_open, day_close) + random.uniform(50, 150)
    day_low = min(day_open, day_close) - random.uniform(50, 150)
    
    # 每 5 分鐘一筆 tick
    tick_time = start_time
    tick_price = day_open
    
    while tick_time <= end_time:
        progress = (tick_time - start_time).total_seconds() / (end_time - start_time).total_seconds()
        
        if tick_time == end_time:
            minute_close = day_close
        else:
            if progress < 0.4:
                target = day_open + (day_high - day_open) * (progress / 0.4)
            elif progress < 0.7:
                target = day_high + (day_low - day_high) * ((progress - 0.4) / 0.3)
            else:
                target = day_low + (day_close - day_low) * ((progress - 0.7) / 0.3)
            minute_close = target + random.uniform(-20, 20)
            minute_close = max(day_low, min(day_high, minute_close))
        
        minute_open = tick_price
        minute_high = max(minute_open, minute_close) + random.uniform(0, 5)
        minute_low = min(minute_open, minute_close) - random.uniform(0, 5)
        minute_high = min(minute_high, day_high)
        minute_low = max(minute_low, day_low)
        
        # 轉為 UTC 存入資料庫
        tick_time_utc = tick_time.astimezone(pytz.UTC)
        batch_data.append((
            tick_time_utc.isoformat(),
            'TXFR1',
            round(minute_open),
            round(minute_high),
            round(minute_low),
            round(minute_close),
            random.randint(50, 500),
            round(minute_close - 1),
            round(minute_close + 1),
            random.randint(10, 50),
            random.randint(10, 50)
        ))
        
        tick_price = minute_close
        tick_time += timedelta(minutes=5)
    
    days_count += 1
    print(f"Day {days_count:2d}: {current_date.strftime('%Y-%m-%d')} - Open:{day_open:6.0f} High:{day_high:6.0f} Low:{day_low:6.0f} Close:{day_close:6.0f}")

# 批次插入
print(f"\nInserting {len(batch_data)} ticks into database...")
try:
    cursor.executemany("""
        INSERT OR REPLACE INTO ticks 
        (ts, code, open, high, low, close, volume, bid_price, ask_price, bid_volume, ask_volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, batch_data)
    conn.commit()
    print(f"[OK] Successfully inserted {len(batch_data)} ticks")
except Exception as e:
    print(f"[ERROR] Insert failed: {e}")
    conn.rollback()
finally:
    conn.close()

print(f"\n{'='*60}")
print(f"[SUCCESS] Generated {days_count} trading days")
print(f"Date range: {(latest_date - timedelta(days=29)).date()} to {latest_date.date()}")
print(f"Total ticks: {len(batch_data)}")
print(f"\nNow refresh Streamlit at http://localhost:8502")
print(f"{'='*60}")
