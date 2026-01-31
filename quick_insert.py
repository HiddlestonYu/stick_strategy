"""
快速插入測試數據 - 最近 30 天
"""
from tick_database import init_database, save_tick
from datetime import datetime, timedelta
import pytz
import random
import sqlite3
from pathlib import Path

print("Initializing database...")
init_database()

# 檢查現有數據
DB_PATH = Path(__file__).parent / "data" / "txf_ticks.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM ticks")
existing_count = cursor.fetchone()[0]
print(f"Existing ticks: {existing_count}")

if existing_count > 0:
    cursor.execute("SELECT MIN(date(ts)), MAX(date(ts)) FROM ticks")
    dates = cursor.fetchone()
    print(f"Existing date range: {dates[0]} to {dates[1]}")
    
    # 如果已經有足夠數據，直接退出
    cursor.execute("SELECT COUNT(DISTINCT date(ts)) FROM ticks")
    existing_days = cursor.fetchone()[0]
    if existing_days >= 20:
        print(f"\n[OK] Already have {existing_days} days of data")
        print("Refresh Streamlit to see the data")
        conn.close()
        exit(0)
    
    # 清空舊數據
    print("Clearing old data...")
    cursor.execute("DELETE FROM ticks")
    conn.commit()

print("Cleared old data\n")

taipei_tz = pytz.timezone('Asia/Taipei')
base_date = taipei_tz.localize(datetime(2026, 1, 30))
base_price = 32247

print(f"Inserting 30 days of data starting from {base_date.date()}...")

batch_data = []
days_inserted = 0

for day_back in range(29, -1, -1):
    current_date = base_date - timedelta(days=day_back)
    
    # 跳過週末
    if current_date.weekday() >= 5:
        continue
    
    # 日盤時間
    start_time = current_date.replace(hour=8, minute=45, second=0)
    end_time = current_date.replace(hour=13, minute=45, second=0)
    
    # 當日 OHLC
    day_open = base_price + random.uniform(-200, 200)
    day_close = day_open + random.uniform(-150, 150)
    day_high = max(day_open, day_close) + random.uniform(50, 150)
    day_low = min(day_open, day_close) - random.uniform(50, 150)
    
    # 生成當日 ticks（每 5 分鐘一筆，減少數據量）
    tick_time = start_time
    tick_price = day_open
    
    while tick_time <= end_time:
        progress = (tick_time - start_time).total_seconds() / (end_time - start_time).total_seconds()
        
        if tick_time == end_time:
            minute_close = day_close
        else:
            if progress < 0.5:
                target = day_open + (day_high - day_open) * (progress / 0.5)
            else:
                target = day_high + (day_close - day_high) * ((progress - 0.5) / 0.5)
            minute_close = target + random.uniform(-20, 20)
            minute_close = max(day_low, min(day_high, minute_close))
        
        minute_open = tick_price
        minute_high = max(minute_open, minute_close) + random.uniform(0, 5)
        minute_low = min(minute_open, minute_close) - random.uniform(0, 5)
        minute_high = min(minute_high, day_high)
        minute_low = max(minute_low, day_low)
        
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
        tick_time += timedelta(minutes=5)  # 每 5 分鐘
    
    days_inserted += 1
    print(f"Day {days_inserted}: {current_date.date()} - O:{day_open:.0f} H:{day_high:.0f} L:{day_low:.0f} C:{day_close:.0f}")

# 批次插入所有數據
print(f"\nInserting {len(batch_data)} ticks...")
cursor.executemany("""
    INSERT OR REPLACE INTO ticks 
    (ts, code, open, high, low, close, volume, bid_price, ask_price, bid_volume, ask_volume)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", batch_data)
conn.commit()
conn.close()

print(f"\n[OK] Inserted {days_inserted} trading days with {len(batch_data)} ticks")
print(f"\nNow refresh your Streamlit page at http://localhost:8502")
