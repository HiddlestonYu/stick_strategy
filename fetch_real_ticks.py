"""
從 Shioaji API 抓取真實的歷史 ticks 並存入資料庫
"""
import shioaji as sj
import os
from datetime import datetime, timedelta
import pytz
from pathlib import Path
import time
import pandas as pd
from tick_database import save_ticks_batch, init_database

# 初始化資料庫
init_database()

# 登入 Shioaji
api = sj.Shioaji()
cert_path = Path.home() / "OneDrive" / "文件" / "Python" / "Sinopac.pfx"

print("登入 Shioaji...")
try:
    # 使用 API Key 登入
    api_key = os.getenv("SHIOAJI_API_KEY")
    secret_key = os.getenv("SHIOAJI_SECRET_KEY")
    if not api_key or not secret_key:
        raise RuntimeError("缺少 Shioaji 憑證：請設定環境變數 SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY")
    accounts = api.login(
        api_key=api_key,
        secret_key=secret_key,
        contracts_timeout=10000,
        contracts_cb=lambda security_type: print(f"[{security_type}] 合約下載完成")
    )
    print(f"[OK] 登入成功: {accounts}")
except Exception as e:
    print(f"[ERROR] 登入失敗: {e}")
    exit(1)

# 選擇台指期貨連續合約（R1 = 近月，會自動在結算日更換）
print("\n查詢台指期貨合約...")
try:
    # 使用連續合約 TXFR1（官方建議用於取得歷史資料）
    contract = api.Contracts.Futures.TXF.TXFR1
    print(f"[OK] 選擇合約: {contract.code} (連續合約 R1 - 近月)")
except Exception as e:
    print(f"[ERROR] 無法取得 TXFR1 合約: {e}")
    exit(1)

# 台灣時區
taipei_tz = pytz.timezone('Asia/Taipei')

# 清除舊的假數據
print("清除資料庫中的舊數據...")
import sqlite3
conn = sqlite3.connect(str(Path(__file__).parent / "data" / "txf_ticks.db"), timeout=10)
cursor = conn.cursor()
cursor.execute("DELETE FROM ticks")
conn.commit()
conn.close()
print("[OK] 舊數據已清除\n")

# 抓取最近 30 天的 ticks（往回推算交易日）
print("\n開始抓取歷史 ticks...")
end_date = datetime.now(taipei_tz)
days_to_fetch = 40  # 往回推 40 天，約可抓到 20-25 個交易日

for days_back in range(days_to_fetch):
    target_date = end_date - timedelta(days=days_back)
    
    # 跳過週末
    if target_date.weekday() >= 5:
        print(f"⏭️  {target_date.date()} (週末，跳過)")
        continue
    
    # 日盤時間：08:45-13:45
    start_time = target_date.replace(hour=8, minute=45, second=0, microsecond=0)
    end_time = target_date.replace(hour=13, minute=45, second=0, microsecond=0)
    
    print(f"\n📅 抓取 {target_date.date()} 日盤 ticks ({start_time.strftime('%H:%M')} - {end_time.strftime('%H:%M')})")
    
    try:
        # 使用 api.ticks() 抓取真實的歷史 ticks
        print(f"  使用 ticks 方法抓取...")
        ticks = api.ticks(
            contract=contract,
            date=target_date.strftime('%Y-%m-%d')
        )
        
        if ticks is None:
            print(f"  ⚠️  無數據")
            continue
        
        # 轉換為 DataFrame
        ticks_df = pd.DataFrame({**ticks})
        
        if ticks_df.empty:
            print(f"  ⚠️  無數據")
            continue
        
        print(f"  抓取到 {len(ticks_df)} 筆 ticks，過濾日盤時間...")
        
        # 確保時間欄位存在
        if 'ts' not in ticks_df.columns:
            print(f"  ❌ 錯誤：ticks 數據沒有 ts 欄位")
            continue
        
        # 轉換時間並過濾日盤時間（08:45-13:45）
        ticks_df['dt'] = pd.to_datetime(ticks_df['ts'], utc=True)
        ticks_df['dt_taipei'] = ticks_df['dt'].dt.tz_convert('Asia/Taipei')
        ticks_df['hour'] = ticks_df['dt_taipei'].dt.hour
        ticks_df['minute'] = ticks_df['dt_taipei'].dt.minute
        ticks_df['time_num'] = ticks_df['hour'] * 100 + ticks_df['minute']
        
        # 日盤：08:45-13:45
        day_session = ticks_df[
            (ticks_df['time_num'] >= 845) & (ticks_df['time_num'] <= 1345)
        ]
        
        print(f"  過濾後剩餘 {len(day_session)} 筆日盤 ticks")
        
        if day_session.empty:
            print(f"  ⚠️  過濾後無數據")
            continue
        
        # 準備批次數據
        batch_ticks = []
        for idx, row in day_session.iterrows():
            tick_time = row['dt_taipei']
            
            tick_data = {
                'ts': tick_time,
                'code': contract.code,
                'open': row.get('Open', row.get('close', 0)),
                'high': row.get('High', row.get('close', 0)),
                'low': row.get('Low', row.get('close', 0)),
                'close': row.get('close', 0),
                'volume': row.get('volume', 0),
                'bid_price': row.get('bid_price', [row.get('close', 0)])[0] if isinstance(row.get('bid_price'), list) else row.get('close', 0),
                'ask_price': row.get('ask_price', [row.get('close', 0)])[0] if isinstance(row.get('ask_price'), list) else row.get('close', 0),
                'bid_volume': row.get('bid_volume', [0])[0] if isinstance(row.get('bid_volume'), list) else 0,
                'ask_volume': row.get('ask_volume', [0])[0] if isinstance(row.get('ask_volume'), list) else 0,
            }
            batch_ticks.append(tick_data)
        
        # 批次存入資料庫
        save_ticks_batch(batch_ticks)
        print(f"  ✅ 完成！共存入 {len(batch_ticks)} 筆日盤 ticks")
        
        # 避免 API 請求過快
        time.sleep(1)
        
    except Exception as e:
        print(f"  ❌ 錯誤: {e}")
        continue

print("\n" + "="*80)
print("[SUCCESS] 歷史 ticks 抓取完成")
print("="*80)
print("\n執行 verify_kbars.py 驗證資料")

# 登出
api.logout()
