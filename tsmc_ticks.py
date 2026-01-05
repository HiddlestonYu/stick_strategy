#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
台積電 (2330) 股票數據獲取
功能: 使用永豐 Shioaji API 獲取台積電的 K 線數據並繪製圖表
"""
#%% 
import shioaji as sj
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
from matplotlib import font_manager
import datetime
import os
from pathlib import Path
from dateutil.relativedelta import relativedelta

# 設置中文字體（根據您的系統調整字體路徑）
def set_chinese_font():
    try:
        font_path = 'C:/Windows/Fonts/msjh.ttc'
        font_prop = font_manager.FontProperties(fname=font_path)
        plt.rcParams['font.family'] = font_prop.get_name()
        plt.rcParams['axes.unicode_minus'] = False
        print("成功設置中文字體")
    except Exception as e:
        print(f"無法設置中文字體，可能會出現亂碼。錯誤訊息：{e}")

def initialize_api(api_key, secret_key, ca_path=None, ca_password=None, person_id=None):
    """
    初始化並登入 Shioaji API
    
    參數:
        api_key (str): API 金鑰
        secret_key (str): 秘密金鑰
        ca_path (str): 憑證路徑 (可選)
        ca_password (str): 憑證密碼 (可選)
        person_id (str): 個人ID (可選)
        
    返回:
        sj.Shioaji: 已登入的 API 實例
    """
    print("正在初始化 Shioaji API...")
    api = sj.Shioaji()
    
    try:
        print("正在登入...")
        accounts = api.login(api_key, secret_key)
        print(f"登入成功! 帳號資訊: {accounts}")
        
        # 啟用憑證 (如果提供)
        if (ca_path and os.path.exists(ca_path)):
            print("正在啟用憑證...")
            api.activate_ca(
                ca_path=ca_path,
                ca_passwd=ca_password,
                person_id=person_id,
            )
            print("憑證啟用成功!")
        elif ca_path:
            print(f"警告: 憑證路徑不存在 - {ca_path}")
            
        return api
    except Exception as e:
        print(f"登入失敗: {str(e)}")
        return None

def get_stock_contract(api, stock_id):
    """
    獲取股票合約資訊
    
    參數:
        api (sj.Shioaji): Shioaji API 實例
        stock_id (str): 股票代碼
        
    返回:
        Contract: 股票合約資訊
    """
    try:
        contract = api.Contracts.Stocks[stock_id]
        print(f"成功獲取 {stock_id} 合約: {contract}")
        return contract
    except Exception as e:
        print(f"獲取合約失敗: {str(e)}")
        return None

def get_kbars_data(api, contract, start_date=None, end_date=None):
    """
    獲取股票的 K 線數據
    
    參數:
        api (sj.Shioaji): Shioaji API 實例
        contract (Contract): 股票合約
        start_date (str): 開始日期，格式為 'YYYY-MM-DD'，預設為一個月前
        end_date (str): 結束日期，格式為 'YYYY-MM-DD'，預設為今天
        
    返回:
        pandas.DataFrame: K 線數據
    """
    try:
        # 如果沒有提供日期，使用預設值
        if start_date is None:
            start_date = (datetime.datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')
        if end_date is None:
            end_date = datetime.datetime.now().strftime('%Y-%m-%d')
            
        print(f"正在獲取 {contract.code} 從 {start_date} 到 {end_date} 的 K 線數據...")
        
        # 獲取 K 線數據
        kbars = api.kbars(contract=contract, start=start_date, end=end_date)
        
        if kbars is not None and hasattr(kbars, 'ts') and len(kbars.ts) > 0:
            print(f"成功獲取 {len(kbars.ts)} 筆 K 線數據")
            
            # 將數據轉換為 DataFrame
            df = pd.DataFrame({**kbars})
            df.ts = pd.to_datetime(df.ts)
            df = df.set_index('ts')
            
            return df
        else:
            print("未找到 K 線數據")
            return None
            
    except Exception as e:
        print(f"獲取 K 線數據時發生錯誤: {str(e)}")
        return None

def save_kbars_to_csv(kbars, stock_id):
    """
    將 K 線數據保存為 CSV 文件
    
    參數:
        kbars (pandas.DataFrame): K 線數據
        stock_id (str): 股票代碼
    """
    if kbars is None or kbars.empty:
        print("沒有 K 線數據可供保存")
        return None
        
    # 創建輸出目錄
    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)
    
    # 創建文件名 (使用當前時間)
    now = datetime.datetime.now()
    filename = output_dir / f"{stock_id}_kbars_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    
    # 保存為 CSV
    kbars.to_csv(filename)
    print(f"K 線數據已保存至 {filename}")
    
    return filename

def plot_kbars(df, stock_id, interval="5T", title=None):
    """
    繪製 K 線圖
    
    參數:
        df (pandas.DataFrame): K 線數據
        stock_id (str): 股票代碼
        interval (str): 重採樣間隔，如 '5T' 為 5 分鐘
        title (str): 圖表標題，若為 None 則自動生成
    """
    if df is None or df.empty:
        print("無數據可供繪製")
        return
    
    # 調整時間索引，確保精確到秒
    df.index = df.index.floor('S')
    
    # 重採樣為指定時間週期的 K 線
    df_resampled = df.resample(interval, label='left', closed='right').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }).dropna()
    
    print(f"重採樣後的數據 ({interval}):")
    print(df_resampled.head())
    
    # 設定繪圖樣式
    mc = mpf.make_marketcolors(
        up='red',           # 漲為紅色 (符合台灣市場慣例)
        down='green',       # 跌為綠色 (符合台灣市場慣例)
        edge='inherit',
        wick='inherit',
        volume='inherit'
    )
    
    s = mpf.make_mpf_style(
        marketcolors=mc,
        gridstyle='--',
        y_on_right=True,
        figcolor='white',   # 改用 figcolor 代替 figure_bgcolor
        gridcolor='gray'
    )
    
    # 設定圖表標題
    if title is None:
        title = f'股票 {stock_id} {interval} K 線圖'
    
    # 創建輸出目錄
    output_dir = Path("charts")
    output_dir.mkdir(exist_ok=True)
    
    # 圖表檔案名稱
    now = datetime.datetime.now()
    filename = output_dir / f"{stock_id}_{interval.replace('T', 'm')}_{now.strftime('%Y%m%d_%H%M%S')}.png"
    
    # 繪製 K 線圖
    fig, axes = mpf.plot(df_resampled, type='candle', style=s,
                 title=title,
                 mav=(5, 20), 
                 volume=True,
                 figsize=(12, 8),
                 savefig=filename,
                 returnfig=True)
    
    print(f"K 線圖已保存至 {filename}")
    
    # 顯示圖表
    plt.show()

def main():
    """主函數，程式執行入口"""
    # 設定中文字體
    set_chinese_font()
    
    # 設定憑證信息（請勿將這些信息提交到公共代碼庫）
    YOUR_API_KEY = "7g7VTSKEuPCZ33HdP7doStZupJExSofekKeyNoe7kXe3"
    YOUR_SECRET_KEY = "F49SkTYUrczd22ss6MKe8ZFowuQSAUqHXg3Yw3axGGsB"
    YOUR_CA_PASSWORD = "F128948483"
    Person_Of_This_Ca = "F128948483"
    
    # 憑證路徑 - 請根據實際情況修改
    ca_path = r"C:\Users\Angus\Desktop\futures_trade\Sinopac.pfx"
    
    # 股票代碼
    stock_id = "2330"  # 台積電
    
    # 設定日期範圍 (根據需要調整)
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.datetime.now() - relativedelta(days=7)).strftime('%Y-%m-%d')
    
    # 初始化 API
    api = initialize_api(
        api_key=YOUR_API_KEY, 
        secret_key=YOUR_SECRET_KEY, 
        ca_path=ca_path,
        ca_password=YOUR_CA_PASSWORD,
        person_id=Person_Of_This_Ca
    )
    
    if api:
        try:
            # 獲取股票合約
            contract = get_stock_contract(api, stock_id)
            
            if contract:
                # 獲取 K 線數據
                kbars_df = get_kbars_data(api, contract, start_date, end_date)
                
                if kbars_df is not None:
                    # 保存數據
                    csv_file = save_kbars_to_csv(kbars_df, stock_id)
                    
                    # 顯示收集到的數據概要
                    print(f"\nK 線數據樣本:")
                    print(kbars_df.head())
                    
                    # 繪製 5 分鐘 K 線圖
                    plot_kbars(kbars_df, stock_id, interval="5T", 
                               title=f"台積電 ({stock_id}) 5 分鐘 K 線圖")
                    
                    # 計算簡單統計數據
                    print("\n價格統計:")
                    print(f"平均收盤價: {kbars_df['Close'].mean()}")
                    print(f"最高收盤價: {kbars_df['Close'].max()}")
                    print(f"最低收盤價: {kbars_df['Close'].min()}")
                    print(f"總成交量: {kbars_df['Volume'].sum()}")
        except Exception as e:
            print(f"操作過程中發生錯誤: {str(e)}")
        finally:
            # 登出 API
            api.logout()
            print("已登出 API")
    
if __name__ == "__main__":
    main()