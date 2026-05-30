import math
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import pytz
import shioaji as sj

from stock_city.db.tick_database import (
    get_kbars_from_db,
    get_latest_tick_timestamp,
    init_database,
    save_ticks_batch,
)
from stock_city.strategy.ma20_ma60 import get_strategy_registry, run_selected_strategy

DEFAULT_PRODUCT = "台指期貨 (TXF)"
DEFAULT_STRATEGY_KEYS = ["ma60_ma100_sr_entry"]
AUTO_RISK_STOP_LOSS_QUANTILE = 0.80

# ── 測試/模擬模式：True = 僅顯示訊息，不實際送出委託 ──
ORDER_SIMULATION_MODE = True
AUTO_RISK_PROFIT_TRIGGER_QUANTILE = 0.65
AUTO_RISK_TRAILING_RATIO = 0.50
AUTO_BACKTEST_PERIOD_OPTIONS = {
    "1個月": 30,
    "1季": 90,
    "半年": 180,
    "1年": 365,
    "2年": 730,
}


def get_strategy_options():
    return {key: value["name"] for key, value in get_strategy_registry().items()}


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

    latest_ts = get_latest_tick_timestamp(code="TXFR1", date=today)
    need_update = latest_ts is None
    if latest_ts is not None and market_is_open:
        latest_local = latest_ts.astimezone(taipei_tz) if getattr(latest_ts, "tzinfo", None) else pytz.UTC.localize(latest_ts).astimezone(taipei_tz)
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


def get_risk_config(stop_loss_quantile, profit_trigger_quantile, trailing_ratio):
    return {
        "stop_loss_quantile": float(stop_loss_quantile),
        "profit_trigger_quantile": float(profit_trigger_quantile),
        "trailing_ratio": float(trailing_ratio),
    }


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


def run_strategy_bundle(df, session, strategy_keys=None, risk_config=None, is_realtime=False):
    strategy_keys = strategy_keys or DEFAULT_STRATEGY_KEYS
    trades, add_events = run_selected_strategy(
        df,
        strategy=strategy_keys,
        session=session,
        is_realtime=is_realtime,
        risk_config=risk_config,
    )
    metrics = calculate_performance_metrics(trades)
    trade_df = build_trade_dataframe(trades)
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
        "period_days": period_days,
    }


def export_backtest_results_to_folder(bt_trades, interval, session, period_label, selected_strategy_keys):
    taipei_tz = pytz.timezone("Asia/Taipei")
    now_str = datetime.now(taipei_tz).strftime("%Y%m%d_%H%M%S")
    strategy_keys = [str(key) for key in (selected_strategy_keys or DEFAULT_STRATEGY_KEYS)]
    strategy_tag = "+".join(strategy_keys)

    output_root = "backtest_outputs"
    os.makedirs(output_root, exist_ok=True)
    folder_name = f"pyqt_{strategy_tag}_{session}_{interval}_{period_label}_{now_str}"
    out_dir = os.path.join(output_root, folder_name)
    os.makedirs(out_dir, exist_ok=True)

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
            "pnl": float(trade.get("pnl", 0) or 0),
            "max_loss_points": float(trade.get("max_loss_points", 0) or 0),
            "max_profit_points": float(trade.get("max_profit_points", 0) or 0),
            "exit_reason": trade.get("exit_reason", ""),
        })

    trades_df = pd.DataFrame(records)
    trade_csv_path = os.path.join(out_dir, "trades.csv")
    trades_df.to_csv(trade_csv_path, index=False, encoding="utf-8-sig")

    metrics = calculate_performance_metrics(bt_trades)
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
    return out_dir, trade_csv_path, summary_csv_path


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
