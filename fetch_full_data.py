"""
抓取 2026-01-01 到 2026-02-04 的數據
"""
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import os
import shioaji as sj
from tick_database import save_ticks_batch
import pytz

# ============================================================
# 1. 登入 Shioaji
# ============================================================
print("登入 Shioaji...")
api = sj.Shioaji()
api_key = os.getenv("SHIOAJI_API_KEY")
secret_key = os.getenv("SHIOAJI_SECRET_KEY")
if not api_key or not secret_key:
    raise RuntimeError("缺少 Shioaji 憑證：請設定環境變數 SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY 再執行")
api.login(
    api_key=api_key,
    secret_key=secret_key,
)
print("[OK] 登入成功\n")

# 取得 TXFR1 合約
contract_list = [c for c in api.Contracts.Futures.TXF if c.code == "TXFR1"]
if not contract_list:
    print("找不到 TXFR1 合約")
    exit(1)

contract = contract_list[0]
print(f"合約: {contract.code}\n")

# ============================================================
# 2. 計算要抓取的日期（2026-01-01 ~ 2026-02-04）
# ============================================================
taipei_tz = pytz.timezone('Asia/Taipei')
end_date = datetime(2026, 2, 4).date()
start_date = datetime(2026, 1, 1).date()

# 生成日期列表（排除週末）
dates_to_fetch = []
current = start_date
while current <= end_date:
    if current.weekday() < 5:  # 週一到週五
        dates_to_fetch.append(current)
    current += timedelta(days=1)

dates_to_fetch.reverse()  # 從最近日期開始抓

print(f"準備抓取 {len(dates_to_fetch)} 個交易日:")
for d in dates_to_fetch:
    print(f"  - {d}")
print()

# ============================================================
# 3. 逐日抓取
# ============================================================
success_count = 0
for target_date in dates_to_fetch:
    print(f"{'='*60}")
    print(f"抓取 {target_date}")
    print(f"{'='*60}")
    
    try:
        # 使用 ±1 天範圍（涵蓋夜盤跨日）
        start = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
        end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
        
        # 使用 api.kbars() 抓取 1 分 K
        kbars = api.kbars(contract=contract, start=start, end=end)
        
        if kbars is None:
            print(f"  [!] 無數據\n")
            continue
        
        # 轉換為 DataFrame
        df = pd.DataFrame({**kbars})
        if df.empty:
            print(f"  [!] 無數據\n")
            continue
        
        df["ts"] = pd.to_datetime(df["ts"])
        df = df.rename(columns={"ts": "datetime"}).sort_values("datetime").reset_index(drop=True)
        
        print(f"  原始資料: {len(df)} 筆 1分K")
        
        # 設定 datetime 為 index
        df = df.set_index("datetime").sort_index()
        
        # 只保留：目標日期 + 隔日 00:00-05:00（夜盤跨日，週末也要補齊）
        next_date = target_date + timedelta(days=1)
        df = df[(df.index.date == target_date) | ((df.index.date == next_date) & (df.index.hour < 5)) | ((df.index.date == next_date) & (df.index.hour == 5) & (df.index.minute == 0))]
        
        if df.empty:
            print(f"  [!] 過濾後無數據\n")
            continue
        
        print(f"  目標日期: {len(df)} 筆")
        
        # 統計日盤/夜盤
        day_session = df.between_time("08:45", "13:45", inclusive="both")
        night_session = df.between_time("15:00", "23:59", inclusive="both")
        
        print(f"  日盤 (08:45-13:45): {len(day_session)} 筆")
        print(f"  夜盤 (15:00-05:00): {len(night_session)} 筆")
        
        if not day_session.empty:
            first_idx = day_session.index[0]
            last_idx = day_session.index[-1]
            print(f"  日盤範圍: {first_idx.strftime('%H:%M:%S')} ~ {last_idx.strftime('%H:%M:%S')}")
            print(f"  開盤價: {day_session.loc[first_idx, 'Open']:.0f}")
            print(f"  收盤價: {day_session.loc[last_idx, 'Close']:.0f}")
            print(f"  最高價: {day_session['High'].max():.0f}")
            print(f"  最低價: {day_session['Low'].min():.0f}")
        
        # 將每根 K 線當作一個 "tick" 存入 database
        batch_ticks = []
        for idx, row in df.iterrows():
            # 確保時區為 Asia/Taipei
            if idx.tzinfo is None:
                idx = taipei_tz.localize(idx)
            else:
                idx = idx.tz_convert(taipei_tz)
            
            tick_data = {
                'ts': idx,
                'code': contract.code,
                'open': row.get('Open', row.get('Close', 0)),
                'high': row.get('High', row.get('Close', 0)),
                'low': row.get('Low', row.get('Close', 0)),
                'close': row.get('Close', 0),
                'volume': row.get('Volume', 0),
                'bid_price': row.get('Close', 0),
                'ask_price': row.get('Close', 0),
                'bid_volume': 0,
                'ask_volume': 0,
            }
            batch_ticks.append(tick_data)
        
        # 存入 database
        save_ticks_batch(batch_ticks)
        print(f"  [OK] 完成！共存入 {len(batch_ticks)} 筆 K 線\n")
        success_count += 1
        
    except Exception as e:
        print(f"  [X] 發生錯誤: {e}\n")
        continue

print(f"\n{'='*60}")
print(f"[OK] 完成！成功抓取 {success_count}/{len(dates_to_fetch)} 個交易日")
print(f"{'='*60}")
