"""backfill_kbars

用途：
- 預先回填台指期 (TXFR1) 的 1 分 K 到 SQLite，讓日K可以顯示更長的歷史（例如 500 根）。

說明：
- 本專案的 SQLite（data/txf_ticks.db）存的是「1 分 K」，再由程式重採樣成 5m/15m/1d。
- 若資料庫只累積了最近幾天，日K就無法隨著「顯示K棒數量」變多。

使用範例：
- 回填最近 500 個交易日（日盤）：
  python backfill_kbars.py --days 500 --session 日盤

- 回填最近 120 個交易日（全盤，含夜盤跨日 00:00~05:00）：
  python backfill_kbars.py --days 120 --session 全盤

注意：
- 這裡的「交易日」用週一~週五近似（不含期交所特殊休市日）。
- 結算日提前收盤 13:30 的規則會套用在日盤過濾。
"""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from datetime import date as date_type
from datetime import datetime, timedelta
import os

import pandas as pd
import pytz
import shioaji as sj

from stock_city.market.settlement_utils import get_day_session_end_time, is_workday
from stock_city.db.tick_database import save_ticks_batch
from stock_city.project_paths import get_db_path


TAIPEI_TZ = pytz.timezone("Asia/Taipei")
UTC_TZ = pytz.UTC
DB_PATH = get_db_path()


@dataclass(frozen=True)
class SessionWindow:
    session: str


def parse_args():
    parser = argparse.ArgumentParser(description="預先回填 TXFR1 1分K 至 SQLite（支援日盤/夜盤/全盤）")
    parser.add_argument(
        "--days",
        type=int,
        default=500,
        help="回填最近 N 個交易日（以週一~週五近似），預設 500",
    )
    parser.add_argument(
        "--session",
        type=str,
        default="日盤",
        choices=["日盤", "夜盤", "全盤"],
        help="要保存的時段：日盤/夜盤/全盤（預設 日盤）",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="若該日已存在足夠資料則跳過（建議開啟）",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="強制重抓：會先刪除該日相關區間資料再回填（小心使用）",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="Shioaji API Key（若未提供，會讀取環境變數 SHIOAJI_API_KEY）",
    )
    parser.add_argument(
        "--secret-key",
        type=str,
        default=None,
        help="Shioaji Secret Key（若未提供，會讀取環境變數 SHIOAJI_SECRET_KEY）",
    )
    return parser.parse_args()


def iter_recent_weekdays(end_date: date_type, count: int) -> list[date_type]:
    """往回找最近 count 個工作日。

    這裡以 Taiwan 國定假日 + 週末近似「非交易日」，可避開常見的清明、五一等無資料日期。
    """
    result: list[date_type] = []
    cursor = end_date
    while len(result) < count:
        if is_workday(cursor):
            result.append(cursor)
        cursor -= timedelta(days=1)
    result.sort()
    return result


def _utc_range_for_local_window(start_local: datetime, end_local: datetime) -> tuple[str, str]:
    return (
        start_local.astimezone(UTC_TZ).isoformat(),
        end_local.astimezone(UTC_TZ).isoformat(),
    )


def delete_existing_for_date(target_date: date_type, session: str):
    """刪除指定日期的資料區間（UTC 字串比較），避免殘留造成 OHLC 亂掉。"""
    # 日盤只刪當天 08:45~14:00（留點緩衝）
    if session == "日盤":
        start_local = TAIPEI_TZ.localize(datetime(target_date.year, target_date.month, target_date.day, 8, 30, 0))
        end_local = TAIPEI_TZ.localize(datetime(target_date.year, target_date.month, target_date.day, 14, 0, 0))
    else:
        # 夜盤/全盤：刪當天 15:00 ~ 隔日 06:00（含 05:00）
        start_local = TAIPEI_TZ.localize(datetime(target_date.year, target_date.month, target_date.day, 15, 0, 0))
        end_local = start_local + timedelta(days=1, hours=15)

    start_utc, end_utc = _utc_range_for_local_window(start_local, end_local)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "DELETE FROM ticks WHERE code=? AND ts >= ? AND ts < ?",
        ("TXFR1", start_utc, end_utc),
    )
    conn.commit()
    conn.close()


def has_sufficient_data(target_date: date_type, session: str) -> bool:
    """用資料庫判斷該日是否已有足夠資料。

    - 日盤：至少要有 08:45~收盤的資料（用筆數粗略判斷）
    - 夜盤/全盤：至少要有當天 15:00~23:59 的資料（避免只因隔日 00:00~05:00 存在就誤判）
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if session == "日盤":
        end_time = get_day_session_end_time(target_date)
        end_h, end_m = map(int, end_time.split(":"))
        start_local = TAIPEI_TZ.localize(datetime(target_date.year, target_date.month, target_date.day, 8, 45, 0))
        end_local = TAIPEI_TZ.localize(datetime(target_date.year, target_date.month, target_date.day, end_h, end_m, 0))
        start_utc, end_utc = _utc_range_for_local_window(start_local, end_local)
        cursor.execute(
            "SELECT COUNT(*) FROM ticks WHERE code=? AND ts >= ? AND ts <= ?",
            ("TXFR1", start_utc, end_utc),
        )
        count = int(cursor.fetchone()[0] or 0)
        conn.close()
        # 日盤 1分K 理論約 300 根（結算日約 285），給個保守門檻
        return count >= 250

    # 夜盤/全盤：檢查 evening (>=15:00) 是否存在
    start_local = TAIPEI_TZ.localize(datetime(target_date.year, target_date.month, target_date.day, 15, 0, 0))
    end_local = TAIPEI_TZ.localize(datetime(target_date.year, target_date.month, target_date.day, 23, 59, 0))
    start_utc, end_utc = _utc_range_for_local_window(start_local, end_local)
    cursor.execute(
        "SELECT COUNT(*) FROM ticks WHERE code=? AND ts >= ? AND ts <= ?",
        ("TXFR1", start_utc, end_utc),
    )
    count = int(cursor.fetchone()[0] or 0)
    conn.close()
    # 夜盤 evening 約 540 根
    return count >= 400


def filter_kbars_for_session(df: pd.DataFrame, target_date: date_type, session: str) -> pd.DataFrame:
    if df.empty:
        return df

    idx = df.index

    if session == "日盤":
        end_time = get_day_session_end_time(target_date)
        end_h, end_m = map(int, end_time.split(":"))

        hours = idx.hour
        minutes = idx.minute
        dates = idx.date

        mask = (dates == target_date) & (
            ((hours == 8) & (minutes >= 45))
            | ((hours >= 9) & (hours < 13))
            | ((hours == 13) & (minutes <= end_m))
        )
        # 結算日 13:30，非結算日 13:45
        if end_h != 13:
            # 正常情況 end_h 應為 13
            pass
        return df.loc[mask]

    next_date = target_date + timedelta(days=1)

    if session == "夜盤":
        # 15:00~23:59（當日） + 隔日 00:00~05:00(含)
        mask = ((idx.date == target_date) & (idx.hour >= 15)) | (
            (idx.date == next_date)
            & (
                (idx.hour < 5)
                | ((idx.hour == 5) & (idx.minute == 0))
            )
        )
        return df.loc[mask]

    # 全盤：保存目標日全日 + 隔日 00:00~05:00(含)（讓夜盤跨日完整）
    mask = (idx.date == target_date) | (
        (idx.date == next_date)
        & (
            (idx.hour < 5)
            | ((idx.hour == 5) & (idx.minute == 0))
        )
    )
    return df.loc[mask]


def main():
    args = parse_args()

    if not DB_PATH.exists():
        DB_PATH.parent.mkdir(exist_ok=True)

    today = datetime.now(TAIPEI_TZ).date()
    dates_to_fetch = iter_recent_weekdays(today, args.days)

    print("============================================================")
    print(f"預計回填：{len(dates_to_fetch)} 天 | 時段：{args.session} | skip_existing={args.skip_existing} | force={args.force}")
    print("============================================================")

    api = sj.Shioaji()
    print("登入 Shioaji...")
    api_key = args.api_key or os.getenv("SHIOAJI_API_KEY")
    secret_key = args.secret_key or os.getenv("SHIOAJI_SECRET_KEY")
    if not api_key or not secret_key:
        raise RuntimeError(
            "缺少 Shioaji 憑證。請使用參數 --api-key/--secret-key，或設定環境變數 SHIOAJI_API_KEY/SHIOAJI_SECRET_KEY。"
        )
    api.login(
        api_key=api_key,
        secret_key=secret_key,
        contracts_timeout=10000,
    )
    print("[OK] 登入成功")

    contract = api.Contracts.Futures.TXF.TXFR1
    print(f"合約: {contract.code}")

    success = 0
    skipped = 0
    failed = 0

    for i, d in enumerate(dates_to_fetch, start=1):
        try:
            if args.skip_existing and has_sufficient_data(d, args.session) and not args.force:
                skipped += 1
                if i % 25 == 0 or i == 1:
                    print(f"[{i}/{len(dates_to_fetch)}] {d} 已有資料，跳過")
                continue

            if args.force:
                delete_existing_for_date(d, args.session)

            start = d.strftime("%Y-%m-%d")
            end = (d + timedelta(days=1)).strftime("%Y-%m-%d")

            kbars = api.kbars(contract=contract, start=start, end=end)
            if not kbars:
                # 常見原因：國定假日/休市日
                skipped += 1
                if i % 25 == 0 or i == 1:
                    print(f"[{i}/{len(dates_to_fetch)}] {d} 無數據（可能休市），跳過")
                continue

            df = pd.DataFrame({**kbars})
            if df.empty:
                skipped += 1
                if i % 25 == 0 or i == 1:
                    print(f"[{i}/{len(dates_to_fetch)}] {d} 無數據（可能休市），跳過")
                continue

            df["ts"] = pd.to_datetime(df["ts"])
            df = df.rename(columns={"ts": "datetime"}).sort_values("datetime").reset_index(drop=True)
            df = df.set_index("datetime").sort_index()

            df = filter_kbars_for_session(df, d, args.session)
            if df.empty:
                skipped += 1
                if i % 25 == 0 or i == 1:
                    print(f"[{i}/{len(dates_to_fetch)}] {d} 過濾後無數據（可能休市），跳過")
                continue

            batch_ticks = []
            for idx, row in df.iterrows():
                if idx.tzinfo is None:
                    idx = TAIPEI_TZ.localize(idx)
                else:
                    idx = idx.tz_convert(TAIPEI_TZ)

                batch_ticks.append(
                    {
                        "ts": idx,
                        "code": contract.code,
                        "open": row.get("Open", row.get("Close", 0)),
                        "high": row.get("High", row.get("Close", 0)),
                        "low": row.get("Low", row.get("Close", 0)),
                        "close": row.get("Close", 0),
                        "volume": row.get("Volume", 0),
                        "bid_price": row.get("Close", 0),
                        "ask_price": row.get("Close", 0),
                        "bid_volume": 0,
                        "ask_volume": 0,
                    }
                )

            save_ticks_batch(batch_ticks)
            success += 1

            if i % 10 == 0 or i == 1 or i == len(dates_to_fetch):
                print(
                    f"[{i}/{len(dates_to_fetch)}] {d} [OK] 存入 {len(batch_ticks)} 筆 ({args.session})"
                )

        except Exception as e:
            failed += 1
            print(f"[{i}/{len(dates_to_fetch)}] {d} [X] 錯誤: {e}")

    api.logout()
    print("============================================================")
    print(f"完成：success={success}, skipped={skipped}, failed={failed}")
    print("============================================================")


if __name__ == "__main__":
    main()
