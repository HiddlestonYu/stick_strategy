#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
台積電 (2330) 股票 K 線圖繪製工具
功能: 抓取台積電最新一年的股票數據並繪製 K 線圖
"""

import yfinance as yf
import mplfinance as mpf
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os

def fetch_stock_data(stock_id, period="1y"):
    """
    使用 yfinance 抓取指定股票的歷史數據
    
    參數:
        stock_id (str): 股票代碼，台灣股票需加上.TW (如 2330.TW)
        period (str): 抓取的時間範圍，如 '1y' 為一年
        
    返回:
        pandas.DataFrame: 股票歷史數據
    """
    # 嘗試多種可能的股票代碼格式
    stock_symbols = []
    if not stock_id.endswith(".TW"):
        stock_symbols.append(f"{stock_id}.TW")
    if not stock_id.endswith(".TWO"):
        stock_symbols.append(f"{stock_id}.TWO")  # 台灣櫃買中心股票可能用.TWO
    stock_symbols.append(stock_id)  # 也嘗試原始代碼
    
    print(f"正在嘗試抓取股票數據，將嘗試以下代碼: {', '.join(stock_symbols)}")
    
    stock_data = None
    error_msgs = []
    
    for symbol in stock_symbols:
        try:
            print(f"正在嘗試下載 {symbol} 的股票數據...")
            data = yf.download(symbol, period=period)
            
            if not data.empty:
                print(f"成功獲取 {symbol} 的 {len(data)} 筆數據")
                stock_data = data
                break
            else:
                error_msgs.append(f"- {symbol}: 無法獲取數據")
        except Exception as e:
            error_msgs.append(f"- {symbol}: {str(e)}")
    
    if stock_data is None:
        print("所有嘗試均失敗，錯誤訊息:")
        for msg in error_msgs:
            print(msg)
        print("請檢查股票代碼是否正確或網絡連接是否正常")
        
        # 嘗試獲取台積電 ADR (以不同的代碼)
        try:
            print("嘗試獲取台積電美國ADR (TSM)...")
            tsm_data = yf.download("TSM", period=period)
            if not tsm_data.empty:
                print(f"成功獲取台積電ADR的 {len(tsm_data)} 筆數據")
                return tsm_data
        except Exception as e:
            print(f"獲取台積電ADR數據時發生錯誤: {e}")
            
    return stock_data

def save_data_to_csv(data, stock_id):
    """
    將股票數據儲存為 CSV 檔案
    
    參數:
        data (pandas.DataFrame): 股票數據
        stock_id (str): 股票代碼
    """
    if data is not None:
        output_dir = "data"
        os.makedirs(output_dir, exist_ok=True)
        
        file_path = os.path.join(output_dir, f"{stock_id}_data.csv")
        data.to_csv(file_path)
        print(f"數據已儲存至 {file_path}")

def plot_k_chart(data, stock_id, period="1y"):
    """
    繪製 K 線圖
    
    參數:
        data (pandas.DataFrame): 股票數據
        stock_id (str): 股票代碼
        period (str): 時間範圍描述
    """
    if data is None or data.empty:
        print("無數據可供繪製")
        return
    
    # 設定 K 線圖樣式
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
        figure_bgcolor='white',
        gridcolor='gray',
        facecolor='white'
    )
    
    # 創建輸出目錄
    output_dir = "charts"
    os.makedirs(output_dir, exist_ok=True)
    
    # 圖表設置
    title = f"台積電 ({stock_id}) K線圖 - 最近{period}"
    filename = os.path.join(output_dir, f"{stock_id}_{period}_k_chart.png")
    
    # 繪製 K 線圖
    fig, axes = mpf.plot(
        data,
        type='candle',
        style=s,
        title=title,
        volume=True,
        figsize=(12, 8),
        returnfig=True
    )
    
    # 保存圖表
    fig.savefig(filename)
    plt.close(fig)
    print(f"K線圖已儲存至 {filename}")
    return filename

def main():
    """主函數，程式執行入口"""
    stock_id = "2330"  # 台積電股票代碼
    period = "1y"      # 最新一年的數據
    
    # 抓取股票數據
    stock_data = fetch_stock_data(stock_id, period)
    
    # 儲存數據
    if stock_data is not None:
        # 將數據保存為 CSV
        save_data_to_csv(stock_data, stock_id)
        
        # 繪製 K 線圖
        chart_file = plot_k_chart(stock_data, stock_id, period)
        
        if chart_file:
            print(f"已成功生成台積電 ({stock_id}) 最新一年的 K 線圖: {chart_file}")
    
if __name__ == "__main__":
    main()