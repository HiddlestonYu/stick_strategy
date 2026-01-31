"""
插入歷史數據 - 過去 100 個交易日
基於 2026/01/30 的數據往前推算
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

# 2026/01/30 的實際數據作為最新一天
latest_date = taipei_tz.localize(datetime(2026, 1, 30))
base_close = 32247  # 2026/01/30 收盤價

print(f"Generating 100 days of historical data...")
print(f"Latest date: {latest_date.date()}, Close: {base_close}\n")

# 準備批次插入
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
batch_data = []
batch_size = 1000

# 生成過去 100 個交易日的數據
total_days = 100
current_price = base_close

# 從最新日期往前推算
for day_offset in range(total_days - 1, -1, -1):
    # 計算日期（跳過週末）
    current_date = latest_date - timedelta(days=day_offset)
    
    # 跳過週六日
    if current_date.weekday() >= 5:  # 5=週六, 6=週日
        continue
    
    # 日盤時間：08:45 - 13:45
    start_time = current_date.replace(hour=8, minute=45, second=0)
    end_time = current_date.replace(hour=13, minute=45, second=0)
    
    # 生成當日 OHLC（每日波動約 100-500 點）
    daily_change = random.uniform(-300, 300)
    day_open = current_price + random.uniform(-50, 50)
    day_close = day_open + daily_change
    
    # 最高最低
    high_range = abs(daily_change) + random.uniform(50, 200)
    low_range = abs(daily_change) + random.uniform(50, 200)
    
    if daily_change > 0:
        day_high = max(day_open, day_close) + random.uniform(20, 100)
        day_low = min(day_open, day_close) - random.uniform(20, 100)
    else:
        day_high = max(day_open, day_close) + random.uniform(20, 100)
        day_low = min(day_open, day_close) - random.uniform(20, 100)
    
    # 確保價格合理（不低於 20000，不高於 40000）
    day_open = max(25000, min(38000, day_open))
    day_high = max(25000, min(38000, day_high))
    day_low = max(25000, min(38000, day_low))
    day_close = max(25000, min(38000, day_close))
    
    # 確保 high/low 邏輯正確
    day_high = max(day_open, day_close, day_high)
    day_low = min(day_open, day_close, day_low)
    
    # 生成當日每分鐘 tick（08:45-13:45 共 301 分鐘）
    tick_time = start_time
    tick_price = day_open
    tick_count = 0
    
    while tick_time <= end_time:
        # 計算進度（0-1）
        progress = (tick_time - start_time).total_seconds() / (end_time - start_time).total_seconds()
        
        # 最後一筆使用精確收盤價
        if tick_time == end_time:
            minute_close = day_close
        else:
            # 根據進度決定價格走勢
            if progress < 0.3:
                # 前 30%：走向最高
                target = day_open + (day_high - day_open) * (progress / 0.3)
            elif progress < 0.6:
                # 中 30%：從最高走向最低
                target = day_high + (day_low - day_high) * ((progress - 0.3) / 0.3)
            else:
                # 後 40%：從最低走向收盤
                target = day_low + (day_close - day_low) * ((progress - 0.6) / 0.4)
            
            # 加入隨機波動
            variation = random.uniform(-15, 15)
            minute_close = target + variation
            minute_close = max(day_low, min(day_high, minute_close))
        
        minute_open = tick_price
        minute_high = max(minute_open, minute_close) + random.uniform(0, 5)
        minute_low = min(minute_open, minute_close) - random.uniform(0, 5)
        
        # 確保分鐘級別的 high/low 在當日範圍內
        minute_high = min(minute_high, day_high)
        minute_low = max(minute_low, day_low)
        
        # 準備批次數據（使用 UTC 時間字串）
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
        
        # 每 1000 筆批次插入
        if len(batch_data) >= batch_size:
            cursor.executemany("""
                INSERT OR REPLACE INTO ticks 
                (ts, code, open, high, low, close, volume, bid_price, ask_price, bid_volume, ask_volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch_data)
            conn.commit()
            batch_data = []
        
        tick_price = minute_close
        tick_time += timedelta(minutes=1)
        tick_count += 1
    
    # 更新當前價格為當日收盤價
    current_price = day_close
    
    # 每 10 天顯示一次進度
    days_from_start = total_days - day_offset
    if days_from_start % 10 == 0:
        print(f"[{days_from_start}/{total_days}] {current_date.date()} - O:{day_open:.0f} H:{day_high:.0f} L:{day_low:.0f} C:{day_close:.0f} ({tick_count} ticks)")

# 插入剩餘的批次數據
if batch_data:
    cursor.executemany("""
        INSERT OR REPLACE INTO ticks 
        (ts, code, open, high, low, close, volume, bid_price, ask_price, bid_volume, ask_volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, batch_data)
    conn.commit()

conn.close()

print(f"\n[OK] Completed! Generated {total_days} days of historical data")
print(f"Date range: {(latest_date - timedelta(days=total_days)).date()} ~ {latest_date.date()}")
print(f"\nVerification: Run streamlit, you should see ~100 daily K-bars")
