import math
import os
import time
from itertools import product
from datetime import datetime, timedelta

import pandas as pd
import pytz
import shioaji as sj

from stock_city.db.tick_database import (
    get_ticks,
    get_kbars_from_db,
    get_latest_tick_timestamp,
    has_date_data,
    init_database,
    save_ticks_batch,
)
from stock_city.strategy.ma20_ma60 import get_strategy_registry, run_selected_strategy

DEFAULT_PRODUCT = "台指期貨 (TXF)"
DEFAULT_STRATEGY_KEYS = ["ma60_ma100_sr_entry"]
CONTRACT_COST_PRESETS = {
    "大台": {"point_value": 200.0, "commission_per_side": 50.0, "tax_per_side": 0.0},
    "小台": {"point_value": 50.0, "commission_per_side": 25.0, "tax_per_side": 28.0},
}
DEFAULT_COST_CONFIG = {
    "contract_type": "小台",
    "point_value": 50.0,
    "commission_per_side": 25.0,
    "tax_per_side": 28.0,
    "slippage_points_per_side": 2.0,
}
AUTO_RISK_STOP_LOSS_QUANTILE = 0.80

# ── 測試/模擬模式：True = 僅顯示訊息，不實際送出委託 ──
ORDER_SIMULATION_MODE = True
AUTO_RISK_PROFIT_TRIGGER_QUANTILE = 0.65
AUTO_RISK_TRAILING_RATIO = 0.50
ENTRY_FILTER_SWEEP = {
    "min_ma60_slope_points": [0, 1, 2, 3, 5],
    "min_body_points": [0, 5, 10, 15, 20],
    "min_body_atr_ratio": [0, 0.2, 0.35, 0.5],
    "min_volume_ratio": [0, 1.0, 1.2, 1.5],
    "entry_exclude_open_minutes": [0, 10, 15],
}
OPTIMIZATION_MAX_COMBINATIONS = 96
AUTO_BACKTEST_PERIOD_OPTIONS = {
    "1個月": 30,
    "1季": 90,
    "半年": 180,
    "1年": 365,
    "2年": 730,
}


def get_strategy_options():
    return {key: value["name"] for key, value in get_strategy_registry().items()}


def get_contract_cost_presets():
    return {key: dict(value) for key, value in CONTRACT_COST_PRESETS.items()}


def login_shioaji(api_key=None, secret_key=None, cert_path=None, cert_password=None, fetch_contract=False):
    """登入 Shioaji，失敗時回傳錯誤訊息。"""

    def _is_too_many_connections(message: str) -> bool:
        msg = str(message or "")
        return ("Too Many Connections" in msg) or ("連線數過多" in msg)

    max_attempts = 2
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            api = sj.Shioaji()
            contracts_cb = None
            if fetch_contract:
                contracts_cb = lambda security_type: print(f"{repr(security_type)} fetch done.")

            if cert_path:
                if fetch_contract:
                    result = api.login(person_id=api_key, passwd=cert_password, contracts_cb=contracts_cb)
                else:
                    result = api.login(person_id=api_key, passwd=cert_password)
            else:
                if fetch_contract:
                    result = api.login(api_key=api_key, secret_key=secret_key, contracts_cb=contracts_cb)
                else:
                    result = api.login(api_key=api_key, secret_key=secret_key)

            if hasattr(result, "get"):
                status = result.get("status", {})
                if isinstance(status, dict):
                    status_code = status.get("status_code", 0)
                    if status_code == 200:
                        return api, None
                    detail = result.get("response", {}).get("detail", "未知錯誤")
                    error_msg = f"狀態碼: {status_code}, 詳情: {detail}"
                    last_error = error_msg
                    if _is_too_many_connections(error_msg) and attempt < max_attempts:
                        time.sleep(2)
                        continue
                    if _is_too_many_connections(error_msg):
                        return None, "連線數過多，系統已自動重試仍失敗。請先登出其他裝置後再試。"
                    return None, error_msg

            return api, None
        except Exception as exc:
            error_msg = str(exc)
            last_error = error_msg
            if _is_too_many_connections(error_msg) and attempt < max_attempts:
                time.sleep(2)
                continue
            if _is_too_many_connections(error_msg):
                return None, "連線數過多，系統已自動重試仍失敗。請先登出其他裝置後再試。"
            return None, error_msg

    return None, (last_error or "登入失敗")


def logout_shioaji(api):
    if api is None:
        return
    try:
        api.logout()
    except Exception:
        pass


def get_market_status():
    taipei_tz = pytz.timezone("Asia/Taipei")
    now = datetime.now(taipei_tz)
    current_hour = now.hour
    current_minute = now.minute
    current_weekday = now.weekday()

    if current_weekday == 6:
        return "週末休市", False, "休市"

    current_time = current_hour * 60 + current_minute
    day_start = 8 * 60 + 45
    day_end = 13 * 60 + 45
    night_start = 15 * 60
    night_end = 5 * 60

    if day_start <= current_time <= day_end and current_weekday < 5:
        return "日盤交易中", True, "日盤"
    if current_time >= night_start and current_weekday < 5:
        return "夜盤交易中", True, "夜盤"
    if current_time <= night_end and 1 <= current_weekday <= 5:
        return "夜盤交易中", True, "夜盤"
    if current_weekday >= 5:
        return "週末休市", False, "休市"
    return "盤中休息", False, "休息"


def filter_by_session(df, session, interval):
    if df is None or df.empty:
        return df
    if interval == "1d" or session == "全盤":
        return df
    if df.index.tz is None:
        df.index = df.index.tz_localize("Asia/Taipei")

    hours = df.index.hour
    minutes = df.index.minute

    if session == "日盤":
        mask = (((hours == 8) & (minutes >= 45)) |
                ((hours >= 9) & (hours < 13)) |
                ((hours == 13) & (minutes <= 45)))
        return df[mask]
    if session == "夜盤":
        mask = (hours >= 15) | (hours < 5) | ((hours == 5) & (minutes == 0))
        return df[mask]
    return df


def process_kline_data(df, interval, session):
    if df is None or df.empty:
        return None

    try:
        df.index = df.index.tz_convert("Asia/Taipei")
    except (TypeError, AttributeError):
        try:
            df.index = df.index.tz_localize("UTC").tz_convert("Asia/Taipei")
        except Exception:
            df.index = df.index.tz_localize("Asia/Taipei")

    if interval == "1d":
        df = df[df.index.dayofweek < 5]

    df = filter_by_session(df, session, interval)
    if df.empty:
        return None

    df = df.copy()
    df.loc[:, "MA20"] = df["Close"].rolling(window=20).mean()
    df.loc[:, "MA60"] = df["Close"].rolling(window=60).mean()
    df.loc[:, "MA100"] = df["Close"].rolling(window=100).mean()
    return df


def estimate_lookback_days(interval_value, session_value, kbars_needed):
    if interval_value == "1d":
        return min(max(int(kbars_needed * 7 / 5) + 30, 60), 1200)

    bars_per_day = {
        "1m": {"日盤": 300, "夜盤": 840, "全盤": 1140},
        "5m": {"日盤": 60, "夜盤": 168, "全盤": 228},
        "15m": {"日盤": 20, "夜盤": 56, "全盤": 76},
        "30m": {"日盤": 10, "夜盤": 28, "全盤": 38},
        "60m": {"日盤": 5, "夜盤": 14, "全盤": 19},
    }
    per_day = bars_per_day.get(interval_value, {}).get(session_value, 60)
    days_needed = int((kbars_needed + per_day - 1) / per_day) + 2
    return min(max(days_needed, 3), 90)


def update_today_data_if_needed(api, session):
    if api is None:
        return 0

    taipei_tz = pytz.timezone("Asia/Taipei")
    now = datetime.now(taipei_tz)
    today = now.date()
    market_status_text, market_is_open, _ = get_market_status()
    if today.weekday() >= 5 and now.hour >= 6:
        return 0

    # 凌晨 00:00~05:59 的夜盤延續時段，需要同時檢查「昨天 + 今天」是否有資料。
    # 避免今天已經有 00:xx 資料時，誤判為不需更新，導致昨晚 15:00~23:59 缺失。
    check_dates = [today]
    if now.hour < 6:
        check_dates = [today - timedelta(days=1), today]

    latest_by_date = {
        d: get_latest_tick_timestamp(code="TXFR1", date=d)
        for d in check_dates
    }

    # 只要有任一日期完全沒資料，就必須更新。
    need_update = any(ts is None for ts in latest_by_date.values())

    # 盤中再加上時效檢查：最新資料若落後 2 分鐘也更新。
    if (not need_update) and market_is_open:
        newest_ts = max(latest_by_date.values())
        latest_local = newest_ts.astimezone(taipei_tz) if getattr(newest_ts, "tzinfo", None) else pytz.UTC.localize(newest_ts).astimezone(taipei_tz)
        need_update = latest_local < now - timedelta(minutes=2)

    if not need_update:
        return 0

    contract = api.Contracts.Futures.TXF.TXFR1
    start = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    end = (today + timedelta(days=1)).strftime("%Y-%m-%d")
    kbars = api.kbars(contract=contract, start=start, end=end)
    if kbars is None:
        return 0
    df = pd.DataFrame({**kbars})
    if df.empty:
        return 0

    df["ts"] = pd.to_datetime(df["ts"])
    df = df.rename(columns={"ts": "datetime"}).sort_values("datetime").reset_index(drop=True)
    df = df.set_index("datetime").sort_index()

    # 凌晨 00:00~05:59 屬於夜盤延續時段，需同時保留「昨晚 + 今日」資料，
    # 否則會遺失同一交易夜盤的前半段（15:00~23:59）。
    if now.hour < 6:
        yesterday = today - timedelta(days=1)
        valid_dates = {yesterday, today}
        df = df[pd.Index(df.index.date).isin(valid_dates)]
    else:
        df = df[df.index.date == today]
    if df.empty:
        return 0

    batch_ticks = []
    for idx, row in df.iterrows():
        idx_local = taipei_tz.localize(idx) if idx.tzinfo is None else idx.tz_convert(taipei_tz)
        batch_ticks.append(
            {
                "ts": idx_local,
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
    return len(batch_ticks)


def _kbars_to_tick_rows(kbars, contract_code):
    taipei_tz = pytz.timezone("Asia/Taipei")
    df = pd.DataFrame({**kbars}) if kbars is not None else pd.DataFrame()
    if df.empty or "ts" not in df.columns:
        return []

    df["ts"] = pd.to_datetime(df["ts"])
    df = df.rename(columns={"ts": "datetime"}).sort_values("datetime").reset_index(drop=True)
    rows = []
    for _, row in df.iterrows():
        ts = row["datetime"]
        if getattr(ts, "tzinfo", None) is None:
            ts_local = taipei_tz.localize(ts.to_pydatetime())
        else:
            ts_local = ts.tz_convert(taipei_tz).to_pydatetime()
        close_price = row.get("Close", 0)
        rows.append(
            {
                "ts": ts_local,
                "code": contract_code,
                "open": row.get("Open", close_price),
                "high": row.get("High", close_price),
                "low": row.get("Low", close_price),
                "close": close_price,
                "volume": row.get("Volume", 0),
                "bid_price": close_price,
                "ask_price": close_price,
                "bid_volume": 0,
                "ask_volume": 0,
            }
        )
    return rows


def _has_regular_day_session_data(date_value, code="TXFR1", min_rows=30):
    taipei_tz = pytz.timezone("Asia/Taipei")
    start_time = taipei_tz.localize(
        datetime(date_value.year, date_value.month, date_value.day, 8, 45)
    )
    end_time = taipei_tz.localize(
        datetime(date_value.year, date_value.month, date_value.day, 13, 46)
    )
    ticks_df = get_ticks(start_time, end_time, code=code)
    return ticks_df is not None and len(ticks_df) >= int(min_rows)


def backfill_missing_kbars_from_shioaji(api, lookback_days=180, code="TXFR1"):
    """登入後掃描近期交易日缺口，使用 Shioaji kbars 補齊 SQLite 資料。"""
    if api is None:
        return {"missing_dates": [], "updated_rows": 0, "fetched_ranges": []}

    init_database()
    taipei_tz = pytz.timezone("Asia/Taipei")
    today = datetime.now(taipei_tz).date()
    start_date = today - timedelta(days=int(lookback_days))
    dates = [
        start_date + timedelta(days=offset)
        for offset in range((today - start_date).days + 1)
    ]
    candidate_dates = [d for d in dates if d.weekday() < 5 and d < today]
    missing_dates = [
        d
        for d in candidate_dates
        if (not has_date_data(d, code=code)) or (not _has_regular_day_session_data(d, code=code))
    ]
    if not missing_dates:
        return {"missing_dates": [], "updated_rows": 0, "fetched_ranges": []}

    contract = api.Contracts.Futures.TXF.TXFR1
    updated_rows = 0
    fetched_ranges = []

    range_start = missing_dates[0]
    range_end = missing_dates[0]
    ranges = []
    for d in missing_dates[1:]:
        if (d - range_end).days <= 3 and (d - range_start).days <= 30:
            range_end = d
        else:
            ranges.append((range_start, range_end))
            range_start = d
            range_end = d
    ranges.append((range_start, range_end))

    for start, end in ranges:
        kbars = api.kbars(
            contract=contract,
            start=start.strftime("%Y-%m-%d"),
            end=(end + timedelta(days=1)).strftime("%Y-%m-%d"),
        )
        rows = _kbars_to_tick_rows(kbars, contract.code)
        if rows:
            save_ticks_batch(rows)
            updated_rows += len(rows)
        fetched_ranges.append(
            {
                "start": start.strftime("%Y-%m-%d"),
                "end": end.strftime("%Y-%m-%d"),
                "rows": len(rows),
            }
        )

    return {
        "missing_dates": [d.strftime("%Y-%m-%d") for d in missing_dates],
        "updated_rows": updated_rows,
        "fetched_ranges": fetched_ranges,
    }


def load_display_data(interval, session, max_kbars, api=None, auto_update=True):
    init_database()
    if auto_update and api is not None:
        update_today_data_if_needed(api, session)

    days = estimate_lookback_days(interval, session, max_kbars)
    df = get_kbars_from_db(interval=interval, session=session, days=days)
    if df is None or df.empty:
        fallback_days = 1200 if interval == "1d" else 300
        if days < fallback_days:
            df = get_kbars_from_db(interval=interval, session=session, days=fallback_days)
            days = fallback_days

    if df is not None and not df.empty and len(df) < max_kbars:
        for extra_days in [30, 60, 120, 240, 400, 800, 1200]:
            if extra_days <= days:
                continue
            bigger_df = get_kbars_from_db(interval=interval, session=session, days=extra_days)
            if bigger_df is None or bigger_df.empty:
                continue
            df = bigger_df
            days = extra_days
            if len(df) >= max_kbars:
                break

    processed_df = process_kline_data(df, interval, session)
    if processed_df is None or processed_df.empty:
        raise ValueError("目前無可用數據，請先回填 DB 或登入 Shioaji 更新當日資料。")

    if len(processed_df) > max_kbars:
        needed_for_ma = max_kbars + 100
        if len(processed_df) >= needed_for_ma:
            processed_df = processed_df.tail(needed_for_ma).copy()
            processed_df["MA20"] = processed_df["Close"].rolling(window=20).mean()
            processed_df["MA60"] = processed_df["Close"].rolling(window=60).mean()
            processed_df["MA100"] = processed_df["Close"].rolling(window=100).mean()
        processed_df = processed_df.tail(max_kbars)

    _, market_is_open, market_session = get_market_status()
    last_db_ts = get_latest_tick_timestamp(code="TXFR1")
    is_realtime = False
    if last_db_ts is not None:
        now = datetime.now(pytz.timezone("Asia/Taipei"))
        is_fresh = (now - last_db_ts) <= timedelta(minutes=2)
        is_realtime = bool(market_is_open and is_fresh and (session == "全盤" or session == market_session))

    return {
        "df": processed_df,
        "meta": {
            "days": days,
            "count": len(processed_df),
            "data_source": "SQLite DB（Shioaji 更新）" if api is not None else "SQLite DB（僅讀取）",
            "last_db_ts": last_db_ts.strftime("%Y-%m-%d %H:%M:%S") if last_db_ts is not None else "無",
            "is_realtime": is_realtime,
            "market_status": get_market_status()[0],
        },
    }


def _chart_kbar_count(interval, requested_count):
    if interval == "1d":
        return max(160, min(600, int(requested_count)))
    if interval == "60m":
        return max(300, min(2000, int(requested_count)))
    if interval == "30m":
        return max(500, min(2500, int(requested_count)))
    return max(1000, min(3000, int(requested_count)))


def load_dashboard_data(session, max_5m_kbars, api=None, auto_update=True, top_interval="1d", bottom_interval="5m"):
    strategy_5m = load_display_data(
        "5m",
        session,
        _chart_kbar_count("5m", max_5m_kbars),
        api=api,
        auto_update=auto_update,
    )
    try:
        top = load_display_data(
            top_interval,
            session,
            _chart_kbar_count(top_interval, max_5m_kbars),
            api=None,
            auto_update=False,
        )
    except Exception:
        top = {"df": pd.DataFrame(), "meta": {}}
    try:
        bottom = load_display_data(
            bottom_interval,
            session,
            _chart_kbar_count(bottom_interval, max_5m_kbars),
            api=None,
            auto_update=False,
        )
    except Exception:
        bottom = {"df": pd.DataFrame(), "meta": {}}

    result = dict(strategy_5m)
    result["df_5m"] = strategy_5m["df"]
    result["df_daily"] = top["df"] if top_interval == "1d" else pd.DataFrame()
    result["df_top"] = top["df"]
    result["df_bottom"] = bottom["df"]
    result["top_meta"] = top["meta"]
    result["bottom_meta"] = bottom["meta"]
    result["top_interval"] = top_interval
    result["bottom_interval"] = bottom_interval
    return result


def calculate_performance_metrics(trades: list[dict]) -> dict:
    total_trades = len(trades)
    pnl_list = [float(t.get("pnl", 0) or 0) for t in trades]
    total_pnl = float(sum(pnl_list)) if total_trades > 0 else 0.0
    win_trades = int(sum(1 for pnl in pnl_list if pnl > 0)) if total_trades > 0 else 0
    loss_trades = int(sum(1 for pnl in pnl_list if pnl < 0)) if total_trades > 0 else 0
    win_rate = (win_trades / total_trades * 100.0) if total_trades > 0 else 0.0

    gross_profit = float(sum(pnl for pnl in pnl_list if pnl > 0)) if total_trades > 0 else 0.0
    gross_loss_abs = float(abs(sum(pnl for pnl in pnl_list if pnl < 0))) if total_trades > 0 else 0.0
    if gross_loss_abs > 0:
        profit_factor = gross_profit / gross_loss_abs
    elif gross_profit > 0:
        profit_factor = float("inf")
    else:
        profit_factor = 0.0

    equity_curve = pd.Series(pnl_list, dtype="float64").cumsum() if total_trades > 0 else pd.Series([], dtype="float64")
    if equity_curve.empty:
        max_drawdown = 0.0
    else:
        running_peak = equity_curve.cummax()
        drawdowns = running_peak - equity_curve
        max_drawdown = float(drawdowns.max())

    return {
        "total_trades": total_trades,
        "total_pnl": float(total_pnl),
        "win_trades": win_trades,
        "loss_trades": loss_trades,
        "win_rate": float(win_rate),
        "gross_profit": float(gross_profit),
        "gross_loss_abs": float(gross_loss_abs),
        "profit_factor": float(profit_factor),
        "max_drawdown": float(max_drawdown),
    }


def _trade_date_from_ts(value):
    if value is None:
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("Asia/Taipei")
    else:
        ts = ts.tz_convert("Asia/Taipei")
    return ts.date()


def _metrics_record(label, trades, min_trades=5):
    metrics = calculate_performance_metrics(trades)
    total_trades = metrics["total_trades"]
    avg_pnl = metrics["total_pnl"] / total_trades if total_trades else 0.0
    gross_pnl = float(sum(float(t.get("gross_pnl", t.get("pnl", 0)) or 0) for t in trades))
    total_cost = float(sum(float(t.get("total_cost_points", 0) or 0) for t in trades))
    max_single_loss = min([float(t.get("pnl", 0) or 0) for t in trades], default=0.0)
    max_single_profit = max([float(t.get("pnl", 0) or 0) for t in trades], default=0.0)
    max_loss_streak = 0
    current_loss_streak = 0
    for trade in trades:
        if float(trade.get("pnl", 0) or 0) < 0:
            current_loss_streak += 1
            max_loss_streak = max(max_loss_streak, current_loss_streak)
        else:
            current_loss_streak = 0
    return {
        "區間": label,
        "交易數": total_trades,
        "勝率(%)": round(metrics["win_rate"], 2),
        "淨損益(點)": round(metrics["total_pnl"], 2),
        "毛損益(點)": round(gross_pnl, 2),
        "成本(點)": round(total_cost, 2),
        "平均每筆(點)": round(avg_pnl, 2),
        "獲利因子": round(metrics["profit_factor"], 4) if math.isfinite(metrics["profit_factor"]) else "inf",
        "最大回撤(點)": round(metrics["max_drawdown"], 2),
        "最大單筆獲利": round(max_single_profit, 2),
        "最大單筆虧損": round(max_single_loss, 2),
        "最大連虧": max_loss_streak,
        "樣本狀態": "OK" if total_trades >= min_trades else "樣本不足",
    }


def build_period_analysis_dataframe(trades, as_of=None):
    if not trades:
        return pd.DataFrame(columns=[
            "區間", "交易數", "勝率(%)", "淨損益(點)", "毛損益(點)", "成本(點)",
            "平均每筆(點)", "獲利因子", "最大回撤(點)", "最大單筆獲利",
            "最大單筆虧損", "最大連虧", "樣本狀態"
        ])

    dated_trades = []
    for trade in trades:
        exit_ts = trade.get("exit_ts") or trade.get("entry_ts")
        trade_date = _trade_date_from_ts(exit_ts)
        if trade_date is None:
            continue
        trade_copy = dict(trade)
        trade_copy["_trade_date"] = trade_date
        dated_trades.append(trade_copy)

    if not dated_trades:
        return pd.DataFrame()

    if as_of is None:
        as_of = max(t["_trade_date"] for t in dated_trades)
    elif hasattr(as_of, "date"):
        as_of = as_of.date()

    records = [_metrics_record("全部", dated_trades, min_trades=10)]
    for days in (90, 180, 365, 730):
        start_date = as_of - timedelta(days=days)
        subset = [t for t in dated_trades if t["_trade_date"] >= start_date]
        records.append(_metrics_record(f"近{days}天", subset, min_trades=15 if days <= 180 else 30))

    by_month = {}
    by_quarter = {}
    for trade in dated_trades:
        d = trade["_trade_date"]
        month_key = f"{d.year}-{d.month:02d}"
        quarter_key = f"{d.year}Q{((d.month - 1) // 3) + 1}"
        by_month.setdefault(month_key, []).append(trade)
        by_quarter.setdefault(quarter_key, []).append(trade)

    for key in sorted(by_month):
        records.append(_metrics_record(f"月:{key}", by_month[key], min_trades=5))
    for key in sorted(by_quarter):
        records.append(_metrics_record(f"季:{key}", by_quarter[key], min_trades=10))

    return pd.DataFrame(records)


def _expected_day_session_rows(date_value):
    try:
        from stock_city.market.settlement_utils import is_settlement_day
        return 286 if is_settlement_day(date_value) else 301
    except Exception:
        return 301


def build_data_health_dataframe(period_days=365, session="日盤", code="TXFR1"):
    taipei_tz = pytz.timezone("Asia/Taipei")
    today = datetime.now(taipei_tz).date()
    start_date = today - timedelta(days=int(period_days))
    try:
        from stock_city.market.settlement_utils import is_workday
    except Exception:
        is_workday = lambda value: value.weekday() < 5
    records = []
    for offset in range((today - start_date).days + 1):
        d = start_date + timedelta(days=offset)
        if d >= today or not is_workday(d):
            continue

        expected_rows = 0
        actual_rows = 0
        first_ts = ""
        last_ts = ""
        status = "OK"
        note = ""

        if session == "夜盤":
            start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 15, 0))
            end_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 23, 59)) + timedelta(minutes=1)
            expected_rows = 540
        else:
            start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 8, 45))
            end_minute = 30 if _expected_day_session_rows(d) == 286 else 45
            end_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 13, end_minute)) + timedelta(minutes=1)
            expected_rows = _expected_day_session_rows(d)

        ticks_df = get_ticks(start_local, end_local, code=code)
        if ticks_df is not None and not ticks_df.empty:
            actual_rows = len(ticks_df)
            first_ts = ticks_df.index[0].strftime("%Y-%m-%d %H:%M")
            last_ts = ticks_df.index[-1].strftime("%Y-%m-%d %H:%M")

        completeness = (actual_rows / expected_rows * 100.0) if expected_rows else 0.0
        if expected_rows and completeness < 95.0:
            status = "缺資料"
            note = "低於95%完整率"
        if actual_rows == 0:
            status = "缺資料"
            note = "無資料"

        records.append({
            "日期": d.strftime("%Y-%m-%d"),
            "時段": session,
            "預期1分K": expected_rows,
            "實際1分K": actual_rows,
            "完整率(%)": round(completeness, 2),
            "首筆時間": first_ts,
            "末筆時間": last_ts,
            "狀態": status,
            "備註": note,
        })

    return pd.DataFrame(records)


def summarize_data_health(data_health_df):
    if data_health_df is None or data_health_df.empty:
        return {
            "checked_days": 0,
            "problem_days": 0,
            "completeness": 0.0,
            "worst_date": "",
            "worst_completeness": 0.0,
            "status": "無資料健康檢查",
        }
    checked_days = len(data_health_df)
    problem_df = data_health_df[data_health_df["狀態"] != "OK"]
    expected = float(data_health_df["預期1分K"].sum())
    actual = float(data_health_df["實際1分K"].sum())
    completeness = (actual / expected * 100.0) if expected else 0.0
    worst_row = data_health_df.sort_values("完整率(%)").head(1)
    worst_date = ""
    worst_completeness = 0.0
    if not worst_row.empty:
        worst_date = str(worst_row.iloc[0]["日期"])
        worst_completeness = float(worst_row.iloc[0]["完整率(%)"])
    return {
        "checked_days": int(checked_days),
        "problem_days": int(len(problem_df)),
        "completeness": round(completeness, 2),
        "worst_date": worst_date,
        "worst_completeness": round(worst_completeness, 2),
        "status": "OK" if problem_df.empty else "資料可能不完整",
    }


def _coerce_cost_config(risk_config=None):
    config = dict(DEFAULT_COST_CONFIG)
    if risk_config:
        for key in (
            "contract_type",
            "point_value",
            "commission_per_side",
            "tax_per_side",
            "slippage_points_per_side",
        ):
            if key in risk_config:
                config[key] = risk_config[key]

    point_value = float(config.get("point_value") or DEFAULT_COST_CONFIG["point_value"])
    if point_value <= 0:
        point_value = DEFAULT_COST_CONFIG["point_value"]

    return {
        "contract_type": str(config.get("contract_type") or DEFAULT_COST_CONFIG["contract_type"]),
        "point_value": point_value,
        "commission_per_side": float(config.get("commission_per_side") or 0),
        "tax_per_side": float(config.get("tax_per_side") or 0),
        "slippage_points_per_side": float(config.get("slippage_points_per_side") or 0),
    }


def apply_trade_costs(trades: list[dict], risk_config=None) -> list[dict]:
    cost_config = _coerce_cost_config(risk_config)
    commission_round_trip = 2.0 * cost_config["commission_per_side"]
    tax_round_trip = 2.0 * cost_config["tax_per_side"]
    cash_cost_points = (commission_round_trip + tax_round_trip) / cost_config["point_value"]
    slippage_points = 2.0 * cost_config["slippage_points_per_side"]
    total_cost_points = cash_cost_points + slippage_points

    adjusted = []
    for trade in trades:
        trade_copy = dict(trade)
        gross_pnl = float(trade_copy.get("gross_pnl", trade_copy.get("pnl", 0)) or 0)
        trade_copy["gross_pnl"] = gross_pnl
        trade_copy["commission_cost"] = commission_round_trip
        trade_copy["tax_cost"] = tax_round_trip
        trade_copy["slippage_cost_points"] = slippage_points
        trade_copy["total_cost_points"] = total_cost_points
        trade_copy["point_value"] = cost_config["point_value"]
        trade_copy["contract_type"] = cost_config["contract_type"]
        trade_copy["pnl"] = gross_pnl - total_cost_points
        adjusted.append(trade_copy)
    return adjusted


def get_risk_config(
    stop_loss_quantile,
    profit_trigger_quantile,
    trailing_ratio,
    min_ma60_slope_points=0,
    min_body_points=0,
    min_body_atr_ratio=0,
    min_volume_ratio=0,
    entry_exclude_open_minutes=0,
    contract_type=None,
    point_value=None,
    commission_per_side=None,
    tax_per_side=None,
    slippage_points_per_side=None,
):
    config = {
        "stop_loss_quantile": float(stop_loss_quantile),
        "profit_trigger_quantile": float(profit_trigger_quantile),
        "trailing_ratio": float(trailing_ratio),
        "min_ma60_slope_points": float(min_ma60_slope_points),
        "min_body_points": float(min_body_points),
        "min_body_atr_ratio": float(min_body_atr_ratio),
        "min_volume_ratio": float(min_volume_ratio),
        "entry_exclude_open_minutes": int(entry_exclude_open_minutes),
    }
    if contract_type is not None:
        config["contract_type"] = str(contract_type)
    if point_value is not None:
        config["point_value"] = float(point_value)
    if commission_per_side is not None:
        config["commission_per_side"] = float(commission_per_side)
    if tax_per_side is not None:
        config["tax_per_side"] = float(tax_per_side)
    if slippage_points_per_side is not None:
        config["slippage_points_per_side"] = float(slippage_points_per_side)
    return config


def build_trade_dataframe(trades: list[dict]) -> pd.DataFrame:
    records = []
    for idx, trade in enumerate(trades, 1):
        entry_ts = trade.get("entry_ts")
        exit_ts = trade.get("exit_ts")
        records.append(
            {
                "編號": idx,
                "進場時間": entry_ts.strftime("%m-%d %H:%M") if hasattr(entry_ts, "strftime") else str(entry_ts),
                "進場點": f"{float(trade.get('entry_price', 0)):.0f}",
                "退場時間": exit_ts.strftime("%m-%d %H:%M") if hasattr(exit_ts, "strftime") else str(exit_ts),
                "退場點": f"{float(trade.get('exit_price', 0)):.0f}",
                "持倉K": int(trade.get("bars_held", 0) or 0),
                "最大不利點": f"-{float(trade.get('max_loss_points', 0) or 0):.0f}",
                "最大有利點": f"+{float(trade.get('max_profit_points', 0) or 0):.0f}",
                "退場原因": trade.get("exit_reason", ""),
                "損益點": f"{float(trade.get('pnl', 0) or 0):+.0f}",
            }
        )
    return pd.DataFrame(records)


def build_trade_dataframe_v2(trades: list[dict]) -> pd.DataFrame:
    records = []
    for idx, trade in enumerate(trades, 1):
        entry_ts = trade.get("entry_ts")
        exit_ts = trade.get("exit_ts")
        records.append(
            {
                "序號": idx,
                "策略": trade.get("strategy_name", trade.get("strategy_key", "")),
                "方向": trade.get("direction", ""),
                "進場時間": entry_ts.strftime("%m-%d %H:%M") if hasattr(entry_ts, "strftime") else str(entry_ts),
                "進場價": f"{float(trade.get('entry_price', 0)):.0f}",
                "出場時間": exit_ts.strftime("%m-%d %H:%M") if hasattr(exit_ts, "strftime") else str(exit_ts),
                "出場價": f"{float(trade.get('exit_price', 0)):.0f}",
                "持有K數": int(trade.get("bars_held", 0) or 0),
                "最大不利(點)": f"-{float(trade.get('max_loss_points', 0) or 0):.0f}",
                "最大有利(點)": f"+{float(trade.get('max_profit_points', 0) or 0):.0f}",
                "毛損益(點)": f"{float(trade.get('gross_pnl', trade.get('pnl', 0)) or 0):+.1f}",
                "成本(點)": f"{float(trade.get('total_cost_points', 0) or 0):.1f}",
                "淨損益(點)": f"{float(trade.get('pnl', 0) or 0):+.1f}",
                "出場原因": trade.get("exit_reason", ""),
            }
        )
    return pd.DataFrame(records)


def run_strategy_bundle(df, session, strategy_keys=None, risk_config=None, is_realtime=False):
    strategy_keys = strategy_keys or DEFAULT_STRATEGY_KEYS
    trades, add_events = run_selected_strategy(
        df,
        strategy=strategy_keys,
        session=session,
        is_realtime=is_realtime,
        risk_config=risk_config,
    )
    trades = apply_trade_costs(trades, risk_config=risk_config)
    metrics = calculate_performance_metrics(trades)
    trade_df = build_trade_dataframe_v2(trades)
    return {
        "trades": trades,
        "trade_df": trade_df,
        "metrics": metrics,
        "events": add_events,
    }


def run_backtest_bundle(interval, session, strategy_keys=None, risk_config=None, period_days=365):
    raw_df = get_kbars_from_db(interval=interval, session=session, days=period_days)
    df = process_kline_data(raw_df, interval, session)
    if df is None or df.empty:
        raise ValueError("回測期間內無可用資料。")

    result = run_strategy_bundle(df, session=session, strategy_keys=strategy_keys, risk_config=risk_config, is_realtime=False)
    period_analysis_df = build_period_analysis_dataframe(result["trades"])
    data_health_df = build_data_health_dataframe(period_days=period_days, session=session, code="TXFR1")
    data_health_summary = summarize_data_health(data_health_df)
    compare_rows = []
    for days_count in (365, 730):
        compare_raw = get_kbars_from_db(interval=interval, session=session, days=days_count)
        compare_df = process_kline_data(compare_raw, interval, session)
        if compare_df is None or compare_df.empty:
            compare_rows.append({
                "期間": f"{days_count}天",
                "交易數": 0,
                "勝率(%)": 0.0,
                "總損益": 0.0,
                "最大資產回撤": 0.0,
                "獲利因子": 0.0,
                "平均每筆": 0.0,
            })
            continue
        compare_result = run_strategy_bundle(compare_df, session=session, strategy_keys=strategy_keys, risk_config=risk_config, is_realtime=False)
        metrics = compare_result["metrics"]
        avg_pnl = metrics["total_pnl"] / metrics["total_trades"] if metrics["total_trades"] > 0 else 0.0
        compare_rows.append({
            "期間": f"{days_count}天",
            "交易數": metrics["total_trades"],
            "勝率(%)": round(metrics["win_rate"], 2),
            "總損益": round(metrics["total_pnl"], 2),
            "最大資產回撤": round(metrics["max_drawdown"], 2),
            "獲利因子": round(metrics["profit_factor"], 4) if math.isfinite(metrics["profit_factor"]) else "inf",
            "平均每筆": round(avg_pnl, 2),
        })

    return {
        "df": df,
        "trades": result["trades"],
        "trade_df": result["trade_df"],
        "metrics": result["metrics"],
        "compare_df": pd.DataFrame(compare_rows),
        "period_analysis_df": period_analysis_df,
        "data_health_df": data_health_df,
        "data_health_summary": data_health_summary,
        "period_days": period_days,
    }


def _min_trades_for_period(period_days):
    if period_days >= 730:
        return 70
    if period_days >= 365:
        return 40
    if period_days >= 90:
        return 15
    return max(8, int(period_days / 6))


def _max_combinations_for_period(period_days):
    if period_days >= 730:
        return 12
    if period_days >= 365:
        return 24
    if period_days >= 180:
        return 48
    return OPTIMIZATION_MAX_COMBINATIONS


def _score_optimization_metrics(metrics, min_trades):
    total_trades = int(metrics["total_trades"])
    if total_trades < min_trades:
        return None

    profit_factor = float(metrics["profit_factor"])
    if not math.isfinite(profit_factor):
        profit_factor = 5.0
    profit_factor = min(profit_factor, 5.0)
    avg_pnl = metrics["total_pnl"] / total_trades if total_trades > 0 else 0.0

    return (
        float(metrics["win_rate"])
        + profit_factor * 8.0
        + float(metrics["total_pnl"]) / 100.0
        + avg_pnl / 5.0
        - float(metrics["max_drawdown"]) / 50.0
    )


def _format_filter_label(config):
    return (
        f"斜率>={config['min_ma60_slope_points']:.0f}, "
        f"實體>={config['min_body_points']:.0f}, "
        f"ATRx{config['min_body_atr_ratio']:.2f}, "
        f"量x{config['min_volume_ratio']:.1f}, "
        f"開盤避{config['entry_exclude_open_minutes']}分"
    )


def build_optimization_dataframe(results):
    records = []
    for idx, item in enumerate(results, 1):
        metrics = item["metrics"]
        config = item["risk_config"]
        records.append({
            "排名": idx,
            "分數": round(float(item["score"]), 2),
            "交易數": metrics["total_trades"],
            "勝率(%)": round(metrics["win_rate"], 2),
            "總損益": round(metrics["total_pnl"], 2),
            "獲利因子": round(metrics["profit_factor"], 4) if math.isfinite(metrics["profit_factor"]) else "inf",
            "最大回撤": round(metrics["max_drawdown"], 2),
            "限制": _format_filter_label(config),
        })
    return pd.DataFrame(records)


def _build_optimization_candidates(keys, max_combinations):
    all_candidates = list(product(*(ENTRY_FILTER_SWEEP[key] for key in keys)))
    total_count = len(all_candidates)
    if max_combinations is None or max_combinations <= 0 or total_count <= max_combinations:
        return all_candidates, total_count

    selected_indexes = {0}
    if max_combinations > 1:
        step = (total_count - 1) / float(max_combinations - 1)
        for idx in range(max_combinations):
            selected_indexes.add(int(round(idx * step)))
    selected = [all_candidates[idx] for idx in sorted(selected_indexes)]
    return selected[:max_combinations], total_count


def run_entry_filter_optimization_bundle(
    interval,
    session,
    strategy_keys=None,
    base_risk_config=None,
    period_days=365,
    top_n=10,
    max_combinations=None,
):
    raw_df = get_kbars_from_db(interval=interval, session=session, days=period_days)
    df = process_kline_data(raw_df, interval, session)
    if df is None or df.empty:
        raise ValueError("最佳化期間內無可用資料。")

    base_config = dict(base_risk_config or get_risk_config(
        AUTO_RISK_STOP_LOSS_QUANTILE,
        AUTO_RISK_PROFIT_TRIGGER_QUANTILE,
        AUTO_RISK_TRAILING_RATIO,
    ))
    keys = list(ENTRY_FILTER_SWEEP.keys())
    if max_combinations is None:
        max_combinations = _max_combinations_for_period(period_days)
    candidates, total_count = _build_optimization_candidates(keys, max_combinations)
    results = []
    calibration_bundle = run_strategy_bundle(
        df,
        session=session,
        strategy_keys=strategy_keys,
        risk_config=base_config,
        is_realtime=False,
    )
    auto_risk_params = None
    for event in calibration_bundle.get("events", []):
        if event.get("type") == "auto_risk_params":
            auto_risk_params = {
                "stop_loss_points": event.get("stop_loss_points"),
                "profit_trigger_points": event.get("profit_trigger_points"),
                "trailing_gap_points": event.get("trailing_gap_points"),
            }
            break
    baseline_trades = int(calibration_bundle["metrics"]["total_trades"])
    min_trades = max(_min_trades_for_period(period_days), int(baseline_trades * 0.35))

    for values in candidates:
        filter_config = dict(zip(keys, values))
        risk_config = dict(base_config)
        risk_config.update(filter_config)
        if auto_risk_params is not None:
            risk_config["_active_risk_params"] = auto_risk_params
        bundle = run_strategy_bundle(
            df,
            session=session,
            strategy_keys=strategy_keys,
            risk_config=risk_config,
            is_realtime=False,
        )
        metrics = bundle["metrics"]
        score = _score_optimization_metrics(metrics, min_trades)
        if score is None:
            continue
        results.append({
            "score": score,
            "risk_config": risk_config,
            "metrics": metrics,
        })

    results.sort(
        key=lambda item: (
            item["score"],
            item["metrics"]["win_rate"],
            item["metrics"]["profit_factor"] if math.isfinite(item["metrics"]["profit_factor"]) else 999.0,
            item["metrics"]["total_pnl"],
        ),
        reverse=True,
    )
    top_results = results[:top_n]
    return {
        "df": df,
        "results": top_results,
        "result_df": build_optimization_dataframe(top_results),
        "period_days": period_days,
        "min_trades": min_trades,
        "baseline_trades": baseline_trades,
        "tested_count": len(candidates),
        "total_count": total_count,
        "qualified_count": len(results),
    }


def _dataframe_to_markdown(df, max_rows=20):
    if df is None or df.empty:
        return "_無資料_"
    view = df.head(max_rows).copy()
    columns = [str(c) for c in view.columns]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in view.iterrows():
        cells = []
        for col in view.columns:
            value = row[col]
            if isinstance(value, float):
                value = round(value, 4)
            cells.append(str(value).replace("\n", " ").replace("|", "/"))
        lines.append("| " + " | ".join(cells) + " |")
    if len(df) > max_rows:
        lines.append(f"\n_僅顯示前 {max_rows} 筆，共 {len(df)} 筆。_")
    return "\n".join(lines)


def _format_metric_value(value, decimals=2):
    if isinstance(value, float):
        if math.isfinite(value):
            return f"{value:.{decimals}f}"
        return "inf"
    return str(value)


def build_backtest_report_markdown(
    bt_trades,
    metrics,
    interval,
    session,
    period_label,
    selected_strategy_keys,
    period_analysis_df=None,
    data_health_df=None,
    data_health_summary=None,
):
    strategy_tag = "+".join([str(key) for key in (selected_strategy_keys or DEFAULT_STRATEGY_KEYS)])
    health = data_health_summary or summarize_data_health(data_health_df)
    first_trade = bt_trades[0] if bt_trades else {}
    cost_text = (
        f"{first_trade.get('contract_type', '未指定')} / "
        f"每筆成本約 {float(first_trade.get('total_cost_points', 0) or 0):.2f} 點"
    )
    win_rate = _format_metric_value(float(metrics.get("win_rate", 0)), 2)
    pf = metrics.get("profit_factor", 0)
    pf_text = _format_metric_value(float(pf), 4) if isinstance(pf, (float, int)) else str(pf)
    problem_rows = pd.DataFrame()
    if data_health_df is not None and not data_health_df.empty:
        problem_rows = data_health_df[data_health_df["狀態"] != "OK"]

    lines = [
        "# 台指期策略回測報告",
        "",
        "## 1. 摘要",
        f"- 產生時間：{datetime.now(pytz.timezone('Asia/Taipei')).strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 策略：{strategy_tag}",
        f"- 回測期間：{period_label}",
        f"- 商品/週期：TXFR1 / {interval}",
        f"- 時段：{session}",
        f"- 交易成本假設：{cost_text}",
        f"- 總交易數：{metrics.get('total_trades', 0)}",
        f"- 勝率：{win_rate}%",
        f"- 總淨損益：{_format_metric_value(float(metrics.get('total_pnl', 0)), 2)} 點",
        f"- 獲利因子：{pf_text}",
        f"- 最大回撤：{_format_metric_value(float(metrics.get('max_drawdown', 0)), 2)} 點",
        f"- 資料健康：{health.get('status', '未知')}，完整率 {health.get('completeness', 0)}%，異常日 {health.get('problem_days', 0)} 天",
        "",
        "## 2. 策略規格",
        "- 策略規格由目前 App 策略 registry 與交易紀錄產生。",
        "- 後續建議改為 StrategySpec，固定描述進場、出場、限制條件、截圖規則與報告欄位。",
        "",
        "## 3. 資料品質",
        f"- 檢查交易日數：{health.get('checked_days', 0)}",
        f"- 異常交易日數：{health.get('problem_days', 0)}",
        f"- 整體完整率：{health.get('completeness', 0)}%",
        f"- 最嚴重日期：{health.get('worst_date', '')} ({health.get('worst_completeness', 0)}%)",
        "",
        _dataframe_to_markdown(problem_rows if not problem_rows.empty else data_health_df, max_rows=20),
        "",
        "## 4. 整體績效",
        f"- 毛利：{_format_metric_value(float(metrics.get('gross_profit', 0)), 2)} 點",
        f"- 毛損：{_format_metric_value(float(metrics.get('gross_loss_abs', 0)), 2)} 點",
        f"- 勝筆：{metrics.get('win_trades', 0)}",
        f"- 敗筆：{metrics.get('loss_trades', 0)}",
        "",
        "## 5. 區間勝率 / 穩健性",
        _dataframe_to_markdown(period_analysis_df, max_rows=40),
        "",
        "## 6. 交易分佈與風險",
        "- 建議後續新增資金曲線、回撤曲線、月損益熱力圖、依星期/進場時間/出場原因統計。",
        "- 目前最大回撤以逐筆已實現損益計算，尚未完整反映持倉期間浮動回撤。",
        "",
        "## 7. 逐筆交易審查",
        "- 詳細交易請見 `trades.csv`。",
        "- 交易截圖請見 `trade_images/`。",
        "",
        "## 8. 結論與實戰建議",
        "- 若資料健康異常日不為 0，應先補齊資料後再評估策略。",
        "- 若區間勝率顯示獲利集中於少數月份，應避免直接進入實戰。",
        "- 若近 90/180 天明顯劣化，建議列為觀察策略而非實戰策略。",
        "",
        "## 9. 免責聲明",
        "本報告僅依歷史資料與指定規則進行回測分析，不代表未來績效。期貨交易具有高槓桿與高風險，實際交易可能受到滑價、流動性、資料延遲、下單失敗與心理執行因素影響。",
        "",
    ]
    return "\n".join(lines)


def export_backtest_results_to_folder(
    bt_trades,
    interval,
    session,
    period_label,
    selected_strategy_keys,
    backtest_df=None,
    period_analysis_df=None,
    data_health_df=None,
    data_health_summary=None,
):
    taipei_tz = pytz.timezone("Asia/Taipei")
    now_str = datetime.now(taipei_tz).strftime("%Y%m%d_%H%M%S")
    strategy_keys = [str(key) for key in (selected_strategy_keys or DEFAULT_STRATEGY_KEYS)]
    strategy_tag = "+".join(strategy_keys)

    output_root = "backtest_outputs"
    os.makedirs(output_root, exist_ok=True)
    folder_name = f"pyqt_{strategy_tag}_{session}_{interval}_{period_label}_{now_str}"
    out_dir = os.path.join(output_root, folder_name)
    os.makedirs(out_dir, exist_ok=True)
    image_dir = os.path.join(out_dir, "trade_images")
    os.makedirs(image_dir, exist_ok=True)

    records = []
    for i, trade in enumerate(bt_trades, 1):
        records.append({
            "id": i,
            "strategy_key": trade.get("strategy_key", ""),
            "strategy_name": trade.get("strategy_name", ""),
            "direction": trade.get("direction", ""),
            "entry_ts": trade.get("entry_ts"),
            "entry_price": float(trade.get("entry_price", 0) or 0),
            "exit_ts": trade.get("exit_ts"),
            "exit_price": float(trade.get("exit_price", 0) or 0),
            "bars_held": int(trade.get("bars_held", 0) or 0),
            "gross_pnl": float(trade.get("gross_pnl", trade.get("pnl", 0)) or 0),
            "total_cost_points": float(trade.get("total_cost_points", 0) or 0),
            "commission_cost": float(trade.get("commission_cost", 0) or 0),
            "tax_cost": float(trade.get("tax_cost", 0) or 0),
            "slippage_cost_points": float(trade.get("slippage_cost_points", 0) or 0),
            "contract_type": trade.get("contract_type", ""),
            "point_value": float(trade.get("point_value", 0) or 0),
            "pnl": float(trade.get("pnl", 0) or 0),
            "max_loss_points": float(trade.get("max_loss_points", 0) or 0),
            "max_profit_points": float(trade.get("max_profit_points", 0) or 0),
            "exit_reason": trade.get("exit_reason", ""),
            "entry_image": "",
            "exit_image": "",
            "full_image": "",
            "daily_entry_image": "",
            "five_min_entry_box_image": "",
        })

    trades_df = pd.DataFrame(records)
    trade_csv_path = os.path.join(out_dir, "trades.csv")
    trades_df.to_csv(trade_csv_path, index=False, encoding="utf-8-sig")

    metrics = calculate_performance_metrics(bt_trades)
    if period_analysis_df is None:
        period_analysis_df = build_period_analysis_dataframe(bt_trades)
    if data_health_df is None:
        data_health_df = pd.DataFrame()
    if data_health_summary is None:
        data_health_summary = summarize_data_health(data_health_df)
    summary_df = pd.DataFrame([
        {
            "generated_at": datetime.now(taipei_tz).strftime("%Y-%m-%d %H:%M:%S"),
            "strategy_keys": strategy_tag,
            "session": session,
            "interval": interval,
            "period": period_label,
            "total_trades": metrics["total_trades"],
            "win_rate": round(metrics["win_rate"], 2),
            "total_pnl": round(metrics["total_pnl"], 2),
            "max_drawdown": round(metrics["max_drawdown"], 2),
            "profit_factor": round(metrics["profit_factor"], 4) if math.isfinite(metrics["profit_factor"]) else "inf",
        }
    ])
    summary_csv_path = os.path.join(out_dir, "summary.csv")
    summary_df.to_csv(summary_csv_path, index=False, encoding="utf-8-sig")

    period_analysis_path = os.path.join(out_dir, "period_analysis.csv")
    period_analysis_df.to_csv(period_analysis_path, index=False, encoding="utf-8-sig")
    data_health_path = os.path.join(out_dir, "data_quality.csv")
    data_health_df.to_csv(data_health_path, index=False, encoding="utf-8-sig")

    report_md = build_backtest_report_markdown(
        bt_trades=bt_trades,
        metrics=metrics,
        interval=interval,
        session=session,
        period_label=period_label,
        selected_strategy_keys=selected_strategy_keys,
        period_analysis_df=period_analysis_df,
        data_health_df=data_health_df,
        data_health_summary=data_health_summary,
    )
    report_path = os.path.join(out_dir, "report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_md)

    return out_dir, trade_csv_path, summary_csv_path, image_dir, report_path


def export_masa_bottom_pullback_entry_screenshots(trades, source_df, out_dir, trade_csv_path):
    """為麻紗底部拉回策略匯出每筆進單的日K與5分K截圖，並更新 trades.csv。"""
    if source_df is None or source_df.empty or not trades:
        return os.path.join(out_dir, "trade_images"), 0

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
    except Exception:
        return os.path.join(out_dir, "trade_images"), 0

    image_dir = os.path.join(out_dir, "trade_images")
    os.makedirs(image_dir, exist_ok=True)
    df = source_df.copy()
    daily = _build_daily_bars_for_export(df)
    if daily.empty:
        return image_dir, 0

    def _plot_candles(ax, data, width=0.58):
        for x, (_, row) in enumerate(data.iterrows()):
            open_px = float(row["Open"])
            high_px = float(row["High"])
            low_px = float(row["Low"])
            close_px = float(row["Close"])
            color = "#18c46f" if close_px >= open_px else "#e84b4b"
            ax.vlines(x, low_px, high_px, color=color, linewidth=1.0, alpha=0.95)
            body_low = min(open_px, close_px)
            body_height = abs(close_px - open_px)
            if body_height < 0.5:
                ax.hlines(close_px, x - width / 2, x + width / 2, color=color, linewidth=1.5)
            else:
                ax.add_patch(
                    Rectangle(
                        (x - width / 2, body_low),
                        width,
                        body_height,
                        facecolor=color,
                        edgecolor=color,
                        alpha=0.9,
                    )
                )
        ax.grid(True, color="#303030", alpha=0.35, linewidth=0.6)
        ax.set_facecolor("#111111")
        ax.tick_params(colors="#333333", labelsize=8)

    def _find_setup_idx(trade):
        entry_ts = trade.get("entry_ts")
        if not hasattr(entry_ts, "date"):
            return None
        before = daily[daily.index.date < entry_ts.date()]
        if before.empty:
            return None
        setup_close = float(trade.get("setup_close", 0) or 0)
        breakdown_low = float(trade.get("breakdown_low", 0) or 0)
        candidates = before[
            (abs(before["Close"] - setup_close) < 0.01)
            & (abs(before["Low"] - breakdown_low) < 0.01)
        ]
        if candidates.empty:
            candidates = before[abs(before["Close"] - setup_close) < 0.01]
        if candidates.empty:
            return daily.index.get_loc(before.index[-1])
        return daily.index.get_loc(candidates.index[-1])

    def _save_daily_entry_image(trade, trade_no):
        setup_idx = _find_setup_idx(trade)
        if setup_idx is None:
            return ""

        start = max(0, setup_idx - 26)
        end = min(len(daily), setup_idx + 8)
        window = daily.iloc[start:end]
        setup_x = setup_idx - start
        box_start_x = max(0, setup_x - 20)
        box_end_x = setup_x + 1.4
        box_low = float(trade.get("box_low", 0) or 0)
        box_high = float(trade.get("box_high", 0) or 0)
        entry_support = float(trade.get("entry_support", box_low) or box_low)
        if box_high <= box_low:
            return ""

        fig, ax = plt.subplots(figsize=(13, 7), dpi=150)
        fig.patch.set_facecolor("white")
        _plot_candles(ax, window)
        ax.add_patch(
            Rectangle(
                (box_start_x - 0.45, box_low),
                box_end_x - box_start_x,
                box_high - box_low,
                fill=False,
                edgecolor="#f7d154",
                linewidth=2.2,
            )
        )
        ax.axhline(box_low, color="#f7d154", linestyle="--", linewidth=1.3, label=f"box_low {box_low:.0f}")
        ax.axhline(box_high, color="#d79cff", linestyle="--", linewidth=1.0, label=f"box_high {box_high:.0f}")
        ax.axhline(entry_support, color="#43a5ff", linestyle=":", linewidth=1.6, label=f"entry_support {entry_support:.0f}")
        ax.scatter([setup_x], [float(trade.get("breakdown_low", box_low) or box_low)], marker="v", s=90, color="#ff6b6b", zorder=5, label="breakdown")
        ax.scatter([setup_x], [float(trade.get("setup_close", entry_support) or entry_support)], marker="^", s=90, color="#7CFC00", zorder=5, label="reclaim")
        labels = [idx.strftime("%m-%d") for idx in window.index]
        step = max(1, len(labels) // 10)
        ax.set_xticks(range(0, len(labels), step))
        ax.set_xticklabels(labels[::step], rotation=45, ha="right")
        ax.set_title(f"Trade {trade_no:04d} Daily Entry Setup: box + breakdown/reclaim")
        ax.legend(loc="upper left", fontsize=8)
        fig.tight_layout()
        filename = f"trade_{trade_no:04d}_masa_daily_entry_box.png"
        path = os.path.join(image_dir, filename)
        fig.savefig(path)
        plt.close(fig)
        return os.path.relpath(path, out_dir)

    def _save_5m_entry_image(trade, trade_no):
        entry_idx = int(trade.get("entry_idx", -1))
        if entry_idx < 0 or entry_idx >= len(df):
            return ""

        start = max(0, entry_idx - 45)
        end = min(len(df), entry_idx + 46)
        window = df.iloc[start:end]
        entry_x = entry_idx - start
        box_low = float(trade.get("box_low", 0) or 0)
        box_high = float(trade.get("box_high", 0) or 0)
        entry_support = float(trade.get("entry_support", box_low) or box_low)
        if box_high <= box_low:
            return ""

        fig, ax = plt.subplots(figsize=(14, 7), dpi=150)
        fig.patch.set_facecolor("white")
        _plot_candles(ax, window, width=0.62)
        ax.add_patch(
            Rectangle(
                (-0.5, box_low),
                len(window),
                box_high - box_low,
                fill=False,
                edgecolor="#f7d154",
                linewidth=2.0,
            )
        )
        ax.axhline(box_low, color="#f7d154", linestyle="--", linewidth=1.3, label=f"box_low {box_low:.0f}")
        ax.axhline(box_high, color="#d79cff", linestyle="--", linewidth=1.0, label=f"box_high {box_high:.0f}")
        ax.axhline(entry_support, color="#43a5ff", linestyle=":", linewidth=1.8, label=f"entry_support {entry_support:.0f}")
        ax.scatter([entry_x], [float(trade.get("entry_price", 0) or 0)], marker="^", s=120, color="#00e676", edgecolor="black", zorder=6, label=f"entry {float(trade.get('entry_price', 0) or 0):.0f}")
        labels = [idx.strftime("%m-%d %H:%M") for idx in window.index]
        step = max(1, len(labels) // 12)
        ax.set_xticks(range(0, len(labels), step))
        ax.set_xticklabels(labels[::step], rotation=45, ha="right")
        ax.set_title(f"Trade {trade_no:04d} 5m Entry: box + support + entry")
        ax.legend(loc="upper left", fontsize=8)
        fig.tight_layout()
        filename = f"trade_{trade_no:04d}_masa_5m_entry_box.png"
        path = os.path.join(image_dir, filename)
        fig.savefig(path)
        plt.close(fig)
        return os.path.relpath(path, out_dir)

    image_rows = []
    created_count = 0
    for idx, trade in enumerate(trades, 1):
        if trade.get("strategy_key") != "masa_bottom_pullback" and "box_low" not in trade:
            image_rows.append({"daily_entry_image": "", "five_min_entry_box_image": ""})
            continue
        daily_image = _save_daily_entry_image(trade, idx)
        five_min_image = _save_5m_entry_image(trade, idx)
        created_count += int(bool(daily_image)) + int(bool(five_min_image))
        image_rows.append({
            "daily_entry_image": daily_image,
            "five_min_entry_box_image": five_min_image,
        })

    try:
        trades_df = pd.read_csv(trade_csv_path, encoding="utf-8-sig")
        trades_df["daily_entry_image"] = ""
        trades_df["five_min_entry_box_image"] = ""
        for idx, row in enumerate(image_rows):
            if idx >= len(trades_df):
                break
            trades_df.at[idx, "daily_entry_image"] = row.get("daily_entry_image", "")
            trades_df.at[idx, "five_min_entry_box_image"] = row.get("five_min_entry_box_image", "")
        trades_df.to_csv(trade_csv_path, index=False, encoding="utf-8-sig")
    except Exception:
        pass

    return image_dir, created_count


def _build_daily_bars_for_export(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or not isinstance(df.index, pd.DatetimeIndex):
        return pd.DataFrame()
    daily = (
        df.assign(_trade_date=df.index.date)
        .groupby("_trade_date")
        .agg(
            Open=("Open", "first"),
            High=("High", "max"),
            Low=("Low", "min"),
            Close=("Close", "last"),
            Volume=("Volume", "sum"),
        )
        .dropna()
    )
    daily.index = pd.to_datetime(daily.index)
    return daily


# ══════════════════ Tick 訂閱 ══════════════════

def subscribe_realtime_tick(api, on_tick_callback):
    """訂閱台指近月即時 Tick。on_tick_callback(dict) 在每筆 tick 呼叫。"""
    if api is None:
        return
    try:
        contract = api.Contracts.Futures.TXF.TXFR1

        def _on_tick(exchange, tick):
            try:
                on_tick_callback({
                    "close": float(getattr(tick, "close", 0)),
                    "volume": int(getattr(tick, "volume", 0)),
                    "tick_type": str(getattr(tick, "tick_type", "")),
                })
            except Exception:
                pass

        api.quote.subscribe(
            contract,
            quote_type=sj.constant.QuoteType.Tick,
            version=sj.constant.QuoteVersion.v1,
        )
        api.quote.set_on_tick_fop_v1_callback(_on_tick)
    except Exception:
        pass


def unsubscribe_realtime_tick(api):
    """取消即時 Tick 訂閱。"""
    if api is None:
        return
    try:
        contract = api.Contracts.Futures.TXF.TXFR1
        api.quote.unsubscribe(
            contract,
            quote_type=sj.constant.QuoteType.Tick,
            version=sj.constant.QuoteVersion.v1,
        )
    except Exception:
        pass


# ══════════════════ 持倉 / 帳戶 ══════════════════

def get_positions_and_balance(api):
    """取得期貨持倉清單與帳戶餘額。"""
    if api is None:
        return {"positions": [], "balance": {}}
    positions = []
    try:
        raw = api.list_positions(api.futopt_account)
        for pos in (raw or []):
            direction = (
                "多"
                if str(getattr(pos.direction, "name", "Buy")).startswith("Buy")
                else "空"
            )
            positions.append({
                "code": getattr(pos, "code", "-"),
                "direction": direction,
                "quantity": int(getattr(pos, "quantity", 0)),
                "price": float(getattr(pos, "price", 0)),
                "last_price": float(
                    getattr(pos, "last_price", getattr(pos, "price", 0))
                ),
                "pnl": float(getattr(pos, "pnl", 0)),
            })
    except Exception:
        pass
    balance = {}
    try:
        acc = api.account_balance()
        if acc is not None:
            balance = {
                "acc_balance": float(getattr(acc, "acc_balance", 0)),
                "available_margin": float(getattr(acc, "available_margin", 0)),
                "unrealized_pnl": float(getattr(acc, "unrealized_pnl", 0)),
            }
    except Exception:
        pass
    return {"positions": positions, "balance": balance}


# ══════════════════ 下單 ══════════════════

def place_futures_order(api, action: str, quantity: int, price=None) -> dict:
    """下台指期貨單。action: 'Buy'/'Sell'，price=None 市價。"""
    if api is None:
        raise ValueError("尚未登入 Shioaji。")
    action_label = "多單" if action == "Buy" else "空單"
    price_label = "市價" if price is None else f"限價 {float(price):.0f}"

    if ORDER_SIMULATION_MODE:
        msg = f"【模擬】{action_label} {quantity}口 {price_label}　（未實際送出委託）"
        return {"order_id": "SIM", "status": "simulated", "msg": msg}

    contract = api.Contracts.Futures.TXF.TXFR1
    act = sj.constant.Action.Buy if action == "Buy" else sj.constant.Action.Sell
    if price is None:
        order = sj.Order(
            action=act,
            price=0,
            quantity=quantity,
            order_type=sj.constant.OrderType.IOC,
            price_type=sj.constant.FuturesPriceType.MKT,
            octype=sj.constant.FuturesOCType.Auto,
            account=api.futopt_account,
        )
    else:
        order = sj.Order(
            action=act,
            price=float(price),
            quantity=quantity,
            order_type=sj.constant.OrderType.ROD,
            price_type=sj.constant.FuturesPriceType.LMT,
            octype=sj.constant.FuturesOCType.Auto,
            account=api.futopt_account,
        )
    trade = api.place_order(contract, order)
    order_id = "-"
    status = "submitted"
    try:
        order_id = (
            trade.order.id
            if hasattr(trade.order, "id")
            else str(getattr(trade, "id", "-"))
        )
        status = str(getattr(trade.status, "status", "submitted"))
    except Exception:
        pass
    return {
        "order_id": order_id,
        "status": status,
        "msg": f"{action_label} {quantity}口 {price_label}　委託號：{order_id}",
    }


def close_all_positions(api) -> dict:
    """一鍵平倉所有台指期倉位。"""
    if api is None:
        raise ValueError("尚未登入 Shioaji。")
    result_data = get_positions_and_balance(api)
    positions = result_data["positions"]
    if not positions:
        return {"status": "no_positions", "msg": "目前無持倉。", "order_id": "-"}

    if ORDER_SIMULATION_MODE:
        sim_msgs = [f"{p['direction']} {p['quantity']}口平倉" for p in positions if int(p['quantity']) > 0]
        msg = "【模擬】平倉委託：" + "，".join(sim_msgs) + "　（未實際送出委託）" if sim_msgs else "無需平倉"
        return {"status": "simulated", "msg": msg, "order_id": "SIM"}

    contract = api.Contracts.Futures.TXF.TXFR1
    msgs = []
    for pos in positions:
        close_action = (
            sj.constant.Action.Sell
            if pos["direction"] == "多"
            else sj.constant.Action.Buy
        )
        qty = int(pos["quantity"])
        if qty <= 0:
            continue
        order = sj.Order(
            action=close_action,
            price=0,
            quantity=qty,
            order_type=sj.constant.OrderType.IOC,
            price_type=sj.constant.FuturesPriceType.MKT,
            octype=sj.constant.FuturesOCType.Auto,
            account=api.futopt_account,
        )
        api.place_order(contract, order)
        msgs.append(f"{pos['direction']} {qty}口平倉")
    return {
        "status": "submitted",
        "msg": "平倉委託：" + "，".join(msgs) if msgs else "無需平倉",
        "order_id": "-",
    }
