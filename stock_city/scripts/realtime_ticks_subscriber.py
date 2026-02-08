"""
即時 ticks 訂閱 - 持續接收並存入資料庫
"""
import shioaji as sj
import os
from datetime import datetime
import pytz
from stock_city.db.tick_database import save_tick, init_database
import time

# 初始化資料庫
init_database()

# 台灣時區
taipei_tz = pytz.timezone('Asia/Taipei')

# 登入 Shioaji
api = sj.Shioaji()

print("登入 Shioaji...")
try:
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
    print(f"[OK] 登入成功")
except Exception as e:
    print(f"[ERROR] 登入失敗: {e}")
    exit(1)

# 選擇台指期貨連續合約 TXFR1（官方建議用於即時資料）
print("\n查詢台指期貨合約...")
try:
    # 使用連續合約 TXFR1（R1 = 近月連續合約，會自動在結算日切換）
    contract = api.Contracts.Futures.TXF.TXFR1
    print(f"[OK] 選擇合約: {contract.code} (連續合約 R1 - 近月)")
except Exception as e:
    print(f"[ERROR] 無法取得 TXFR1 合約: {e}")
    exit(1)

# 定義 tick 回調函數
tick_count = 0

@api.on_tick_fop_v1()
def quote_callback(exchange, tick):
    """接收到 tick 時的回調函數"""
    global tick_count
    
    try:
        # 轉換時間戳
        tick_time = datetime.fromtimestamp(tick['datetime']/1e9, tz=taipei_tz)
        
        # 準備 tick 數據
        tick_data = {
            'ts': tick_time,
            'code': tick.get('code', contract.code),
            'open': tick.get('open', tick['close']),
            'high': tick.get('high', tick['close']),
            'low': tick.get('low', tick['close']),
            'close': tick['close'],
            'volume': tick.get('volume', 0),
            'bid_price': tick['bid_price'][0] if 'bid_price' in tick and len(tick['bid_price']) > 0 else tick['close'],
            'ask_price': tick['ask_price'][0] if 'ask_price' in tick and len(tick['ask_price']) > 0 else tick['close'],
            'bid_volume': tick['bid_volume'][0] if 'bid_volume' in tick and len(tick['bid_volume']) > 0 else 0,
            'ask_volume': tick['ask_volume'][0] if 'ask_volume' in tick and len(tick['ask_volume']) > 0 else 0,
        }
        
        # 存入資料庫
        save_tick(tick_data)
        tick_count += 1
        
        # 每 10 筆顯示一次
        if tick_count % 10 == 0:
            print(f"[{tick_time.strftime('%H:%M:%S')}] 已接收 {tick_count} 筆 ticks，最新價格: {tick['close']}")
    
    except Exception as e:
        print(f"[ERROR] 處理 tick 失敗: {e}")

# 訂閱即時報價
print(f"\n開始訂閱 {contract.code} 即時 ticks...")
api.quote.subscribe(
    contract,
    quote_type=sj.constant.QuoteType.Tick,
    version=sj.constant.QuoteVersion.v1
)

print("✅ 訂閱成功！正在接收即時數據...")
print("按 Ctrl+C 停止")

try:
    # 保持運行
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print(f"\n\n停止訂閱...")
    print(f"總共接收並存入 {tick_count} 筆 ticks")
    api.quote.unsubscribe(
        contract,
        quote_type=sj.constant.QuoteType.Tick
    )
    api.logout()
    print("✅ 已登出")
