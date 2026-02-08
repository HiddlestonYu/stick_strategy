"""\
使用 api.kbars() 抓取 TXFR1 的 1 分 K，並存入 SQLite（以 1 分 K 當作 tick 儲存）。

注意：本檔不再硬編碼 Shioaji API 金鑰，請改用環境變數。
  - SHIOAJI_API_KEY
  - SHIOAJI_SECRET_KEY
"""

import argparse
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytz
import shioaji as sj

from settlement_utils import get_day_session_end_time, is_settlement_day
from tick_database import save_ticks_batch


def parse_args():
    parser = argparse.ArgumentParser(description="抓取並更新 TXFR1 1 分 K 至 SQLite（含跨日夜盤）")
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="指定單一日期（YYYY-MM-DD），會同時補齊隔日 00:00-05:00 夜盤跨日資料",
    )
    return parser.parse_args()


def get_shioaji_credentials():
    api_key = os.getenv("SHIOAJI_API_KEY")
    secret_key = os.getenv("SHIOAJI_SECRET_KEY")
    if not api_key or not secret_key:
        raise RuntimeError(
            "缺少 Shioaji 憑證：請設定環境變數 SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY 再執行"
        )
    return api_key, secret_key


def main():
    args = parse_args()

    taipei_tz = pytz.timezone("Asia/Taipei")
    today = datetime.now(taipei_tz)

    api_key, secret_key = get_shioaji_credentials()

    api = sj.Shioaji()
    print("登入 Shioaji...")
    api.login(api_key=api_key, secret_key=secret_key, contracts_timeout=10000)
    print("[OK] 登入成功\n")

    contract = api.Contracts.Futures.TXF.TXFR1
    print(f"合約: {contract.code}\n")

    dates_to_fetch = []
    if args.date:
        target = datetime.strptime(args.date, "%Y-%m-%d").date()
        dates_to_fetch = [target]
    else:
        for days_back in range(15):
            date = (today - timedelta(days=days_back)).date()
            if date.weekday() < 5:
                dates_to_fetch.append(date)
            if len(dates_to_fetch) >= 8:
                break

    print("準備抓取以下日期的 K 線:")
    for d in dates_to_fetch:
        print(f"  - {d}")
    print()

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
            ("TXFR1", start_utc, end_utc),
        )
    conn.commit()
    conn.close()
    print("[OK] 已清除舊數據\n")

    for target_date in dates_to_fetch:
        print(f"{'=' * 60}")
        print(f"抓取 {target_date}")
        print(f"{'=' * 60}")

        try:
            start = (target_date - timedelta(days=1)).strftime("%Y-%m-%d")
            end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

            kbars = api.kbars(contract=contract, start=start, end=end)
            if kbars is None:
                print("  [!] 無數據\n")
                continue

            df = pd.DataFrame({**kbars})
            if df.empty:
                print("  [!] 無數據\n")
                continue

            df["ts"] = pd.to_datetime(df["ts"])
            df = (
                df.rename(columns={"ts": "datetime"})
                .sort_values("datetime")
                .reset_index(drop=True)
            )
            print(f"  原始資料: {len(df)} 筆 1分K")

            df = df.set_index("datetime").sort_index()

            next_date = target_date + timedelta(days=1)
            df = df[
                (df.index.date == target_date)
                | ((df.index.date == next_date) & (df.index.hour < 5))
                | (
                    (df.index.date == next_date)
                    & (df.index.hour == 5)
                    & (df.index.minute == 0)
                )
            ]
            if df.empty:
                print("  [!] 過濾後無數據\n")
                continue

            print(f"  目標日期: {len(df)} 筆")

            is_settle = is_settlement_day(target_date)
            end_time = get_day_session_end_time(target_date)

            day_session = df.between_time("08:45", end_time, inclusive="both")
            night_session = df.between_time("15:00", "23:59", inclusive="both")

            settle_note = " (結算日)" if is_settle else ""
            print(f"  日盤 (08:45-{end_time}): {len(day_session)} 筆{settle_note}")
            print(f"  夜盤 (15:00-05:00): {len(night_session)} 筆")

            if not day_session.empty:
                first_idx = day_session.index[0]
                last_idx = day_session.index[-1]
                print(
                    f"  日盤範圍: {first_idx.strftime('%H:%M:%S')} ~ {last_idx.strftime('%H:%M:%S')}"
                )
                print(f"  開盤價: {day_session.loc[first_idx, 'Open']:.0f}")
                print(f"  收盤價: {day_session.loc[last_idx, 'Close']:.0f}")
                print(f"  最高價: {day_session['High'].max():.0f}")
                print(f"  最低價: {day_session['Low'].min():.0f}")

            batch_ticks = []
            for idx, row in df.iterrows():
                if idx.tzinfo is None:
                    idx = taipei_tz.localize(idx)
                else:
                    idx = idx.tz_convert(taipei_tz)

                tick_data = {
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
                batch_ticks.append(tick_data)

            save_ticks_batch(batch_ticks)
            print(f"  [OK] 完成！共存入 {len(batch_ticks)} 筆 K 線\n")

        except Exception as e:
            print(f"  [X] 錯誤: {e}\n")
            import traceback

            traceback.print_exc()
            continue

    api.logout()
    print(f"\n{'=' * 60}")
    print("[OK] 完成！")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
