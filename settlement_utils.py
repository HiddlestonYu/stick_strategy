"""settlement_utils

結算日規則（台指期 TXF）：
- 原則：每月第三個星期三
- 若遇假日/非工作日：順延至下一個工作日
- 結算日當天日盤提前收盤到 13:30

備註：此處以 `holidays` 套件的 Taiwan 國定假日行事曆近似「非工作日」。
"""

from __future__ import annotations

from datetime import date as date_type
from datetime import datetime, timedelta
from functools import lru_cache

try:
    import holidays
except Exception:  # pragma: no cover
    holidays = None


def _to_date(value: date_type | datetime) -> date_type:
    if isinstance(value, datetime):
        return value.date()
    return value


@lru_cache(maxsize=256)
def _tw_holidays(year: int):
    if holidays is None:
        return set()
    # 只取該年度的假日即可
    return set(holidays.Taiwan(years=[year]).keys())


def is_workday(value: date_type | datetime) -> bool:
    """判斷是否為工作日（週一~週五且非台灣國定假日）。"""
    d = _to_date(value)
    if d.weekday() >= 5:
        return False
    if holidays is None:
        # 若缺少 holidays 套件，退回只用週末判斷（至少不會誤把週末當工作日）
        return True
    return d not in _tw_holidays(d.year)


@lru_cache(maxsize=256)
def get_settlement_date(year: int, month: int) -> date_type:
    """取得指定年月的結算日（第三個星期三，遇非工作日順延）。"""
    # 找到當月第三個星期三
    first_day = date_type(year, month, 1)
    # weekday: Mon=0 ... Wed=2
    days_to_wed = (2 - first_day.weekday()) % 7
    first_wed = first_day + timedelta(days=days_to_wed)
    third_wed = first_wed + timedelta(days=14)

    settle = third_wed
    while not is_workday(settle):
        settle = settle + timedelta(days=1)
    return settle

def is_settlement_day(value: date_type | datetime) -> bool:
    """判斷是否為台指期結算日（第三個星期三，遇非工作日順延）。"""
    d = _to_date(value)
    return d == get_settlement_date(d.year, d.month)

def get_day_session_end_time(value: date_type | datetime) -> str:
    """
    取得指定日期的日盤收盤時間
    
    參數:
        date: datetime.date 或 datetime.datetime
    
    返回:
        str: "13:30" (結算日) 或 "13:45" (一般日)
    """
    if is_settlement_day(value):
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

    # 額外示例：2022/07 結算日（你提供的案例：7/20）
    example = datetime(2022, 7, 20)
    print(f"2022-07 settlement date: {get_settlement_date(2022, 7)} | is_settlement_day(2022-07-20)={is_settlement_day(example)}")
