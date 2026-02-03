"""
判斷是否為台指期結算日
結算日：每月第三個星期三，日盤提前收盤到 13:30
"""
from datetime import datetime

def is_settlement_day(date):
    """
    判斷是否為台指期結算日（每月第三個星期三）
    
    參數:
        date: datetime.date 或 datetime.datetime
    
    返回:
        bool: True 表示是結算日
    """
    if isinstance(date, datetime):
        date = date.date()
    
    # 必須是星期三 (weekday() == 2)
    if date.weekday() != 2:
        return False
    
    # 計算是當月第幾個星期三
    day = date.day
    # 第一個星期三在 1-7 號，第二個在 8-14 號，第三個在 15-21 號
    if 15 <= day <= 21:
        return True
    
    return False

def get_day_session_end_time(date):
    """
    取得指定日期的日盤收盤時間
    
    參數:
        date: datetime.date 或 datetime.datetime
    
    返回:
        str: "13:30" (結算日) 或 "13:45" (一般日)
    """
    if is_settlement_day(date):
        return "13:30"
    else:
        return "13:45"

# 測試
if __name__ == "__main__":
    test_dates = [
        datetime(2026, 1, 21),  # 第三個星期三
        datetime(2026, 1, 22),  # 星期四
        datetime(2026, 2, 18),  # 2月第三個星期三
        datetime(2026, 3, 18),  # 3月第三個星期三
    ]
    
    for date in test_dates:
        is_settle = is_settlement_day(date)
        end_time = get_day_session_end_time(date)
        print(f"{date.strftime('%Y-%m-%d %A')}: 結算日={is_settle}, 收盤時間={end_time}")
