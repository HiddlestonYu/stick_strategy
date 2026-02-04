"""
使用 api.kbars() 抓取最近 5 天的完整 K 線數據（改良版）
參考 test_shioaji_kbar.ipynb 的方法
"""
import argparse
import shioaji as sj
from datetime import datetime, timedelta
import pandas as pd
from tick_database import save_ticks_batch
import sqlite3
from pathlib import Path
import pytz


def parse_args():
    parser = argparse.ArgumentParser(description="抓取並更新 TXFR1 1 分 K 至 SQLite（含跨日夜盤）")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="指定單一日期（YYYY-MM-DD），會同時補齊隔日 00:00-05:00 夜盤跨日資料",
    )
    return parser.parse_args()


args = parse_args()

# 登入 Shioaji
api = sj.Shioaji()
print("登入 Shioaji...")
accounts = api.login(
    api_key="F97Uvg5MtkHWLzPzueMkxYYgZwo8h18Qsk6Y3Ah6BBox",
    secret_key="5a1Uenx7KtJN1CxxHC34MDJgHN67ePysroAPGmzTv1zG",
    contracts_timeout=10000
)
print(f"[OK] 登入成功\n")

# 選擇合約
contract = api.Contracts.Futures.TXF.TXFR1
print(f"合約: {contract.code}\n")

# 要抓取的日期清單（最近 8 個交易日 / 或指定單一日期）
taipei_tz = pytz.timezone('Asia/Taipei')
today = datetime.now(taipei_tz)

dates_to_fetch = []
if args.date:
    target = datetime.strptime(args.date, "%Y-%m-%d").date()
    dates_to_fetch = [target]
else:
    for days_back in range(15):  # 往回推 15 天找出 8 個交易日
        date = (today - timedelta(days=days_back)).date()
        # 跳過週末
        if date.weekday() < 5:
            dates_to_fetch.append(date)
        if len(dates_to_fetch) >= 8:  # 增加到 8 天
            break

print(f"準備抓取以下日期的 K 線:")
for d in dates_to_fetch:
    print(f"  - {d}")
print()

# 清除這些日期的舊數據（使用 UTC 範圍，並包含隔日 05:00）
db_path = Path(__file__).parent / "data" / "txf_ticks.db"
conn = sqlite3.connect(str(db_path))
cursor = conn.cursor()
for date in dates_to_fetch:
    start_local = taipei_tz.localize(datetime(date.year, date.month, date.day, 0, 0, 0))
    end_local = start_local + timedelta(days=1, hours=6)
    start_utc = start_local.astimezone(pytz.UTC).isoformat()
    end_utc = end_local.astimezone(pytz.UTC).isoformat()
    cursor.execute(
        "DELETE FROM ticks WHERE code=? AND ts >= ? AND ts < ?",
        ('TXFR1', start_utc, end_utc)
    )
conn.commit()
conn.close()
print("[OK] 已清除舊數據\n")

# 逐日抓取
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
        
        # 判斷是否為結算日（影響日盤收盤時間）
        from settlement_utils import is_settlement_day, get_day_session_end_time
        is_settle = is_settlement_day(target_date)
        end_time = get_day_session_end_time(target_date)
        
        # 統計日盤/夜盤
        day_session = df.between_time("08:45", end_time, inclusive="both")
        night_session = df.between_time("15:00", "23:59", inclusive="both")
        
        settle_note = " (結算日)" if is_settle else ""
        print(f"  日盤 (08:45-{end_time}): {len(day_session)} 筆{settle_note}")
        print(f"  夜盤 (15:00-05:00): {len(night_session)} 筆")
        
        if not day_session.empty:
            first_idx = day_session.index[0]
            last_idx = day_session.index[-1]
            print(f"  日盤範圍: {first_idx.strftime('%H:%M:%S')} ~ {last_idx.strftime('%H:%M:%S')}")
            print(f"  開盤價: {day_session.loc[first_idx, 'Open']:.0f}")  # 改用 Open
            print(f"  收盤價: {day_session.loc[last_idx, 'Close']:.0f}")
            print(f"  最高價: {day_session['High'].max():.0f}")
            print(f"  最低價: {day_session['Low'].min():.0f}")
        
        # 將每根 K 線當作一個 "tick" 存入 database
        # 注意：這裡將 1 分 K 當作 tick 儲存，保留完整資料
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
                'bid_price': row.get('Close', 0),  # kbar 沒有 bid/ask，用 close 代替
                'ask_price': row.get('Close', 0),
                'bid_volume': 0,
                'ask_volume': 0,
            }
            batch_ticks.append(tick_data)
        
        # 存入 database
        save_ticks_batch(batch_ticks)
        print(f"  [OK] 完成！共存入 {len(batch_ticks)} 筆 K 線\n")
        
    except Exception as e:
        print(f"  [X] 錯誤: {e}\n")
        import traceback
        traceback.print_exc()
        continue

api.logout()
print(f"\n{'='*60}")
print("[OK] 完成！")
print(f"{'='*60}")
