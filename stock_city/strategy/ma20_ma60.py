from datetime import datetime, timedelta
import pytz
import pandas as pd


STRATEGY_MA60_MA100_SR_ENTRY = "ma60_ma100_sr_entry"
STRATEGY_MASA_BOTTOM_PULLBACK = "masa_bottom_pullback"
AUTO_RISK_MIN_LOOKBACK_DAYS = 365
AUTO_RISK_MIN_TRADES = 12
AUTO_RISK_STOP_LOSS_QUANTILE = 0.80
AUTO_RISK_PROFIT_TRIGGER_QUANTILE = 0.65
AUTO_RISK_TRAILING_RATIO = 0.50
AUTO_RISK_MIN_POINTS = 5.0


def get_strategy_registry():
    """策略註冊表（可擴充多策略）。"""
    return {
        STRATEGY_MA60_MA100_SR_ENTRY: {
            "name": "MA60/MA100 撐壓進場",
            "func": calculate_ma60_ma100_support_resistance_signals,
        },
        STRATEGY_MASA_BOTTOM_PULLBACK: {
            "name": "麻紗底部拉回（日K破底翻/5分進出）",
            "func": calculate_masa_bottom_pullback_signals,
        }
    }


def _normalize_strategy_keys(strategy):
    """將單一/多個策略輸入統一轉成策略 key 清單。"""
    if strategy is None:
        return [STRATEGY_MA60_MA100_SR_ENTRY]

    if isinstance(strategy, str):
        raw_items = [item.strip().lower() for item in strategy.split(",") if item.strip()]
    elif isinstance(strategy, (list, tuple, set)):
        raw_items = [str(item).strip().lower() for item in strategy if str(item).strip()]
    else:
        raw_items = [str(strategy).strip().lower()]

    alias_map = {
        "1": STRATEGY_MA60_MA100_SR_ENTRY,
        "strategy1": STRATEGY_MA60_MA100_SR_ENTRY,
        "ma60_ma100": STRATEGY_MA60_MA100_SR_ENTRY,
        "ma60_ma100_sr_entry": STRATEGY_MA60_MA100_SR_ENTRY,
        "ma60_ma100_support_resistance_entry": STRATEGY_MA60_MA100_SR_ENTRY,
        "2": STRATEGY_MASA_BOTTOM_PULLBACK,
        "masa": STRATEGY_MASA_BOTTOM_PULLBACK,
        "masa_bottom": STRATEGY_MASA_BOTTOM_PULLBACK,
        "bottom_pullback": STRATEGY_MASA_BOTTOM_PULLBACK,
        "masa_bottom_pullback": STRATEGY_MASA_BOTTOM_PULLBACK,
    }

    normalized = []
    for key in raw_items:
        mapped = alias_map.get(key, key)
        if mapped not in normalized:
            normalized.append(mapped)

    return normalized or [STRATEGY_MA60_MA100_SR_ENTRY]


def _select_recent_df_by_days(df: pd.DataFrame, days: int = AUTO_RISK_MIN_LOOKBACK_DAYS) -> pd.DataFrame:
    """取最近 N 天資料做風控參數校準。"""
    if df is None or df.empty:
        return df
    if not isinstance(df.index, pd.DatetimeIndex):
        return df

    end_ts = df.index.max()
    start_ts = end_ts - pd.Timedelta(days=days)
    return df[df.index >= start_ts]


def _derive_auto_risk_params_from_trades(
    trades: list[dict],
    stop_loss_quantile: float = AUTO_RISK_STOP_LOSS_QUANTILE,
    profit_trigger_quantile: float = AUTO_RISK_PROFIT_TRIGGER_QUANTILE,
    trailing_ratio: float = AUTO_RISK_TRAILING_RATIO,
):
    """由歷史交易的最大虧損/最大獲利分佈，推導停損與動態停利參數。"""
    if not trades:
        return None

    losses = []
    profits = []
    for trade in trades:
        loss_points = float(trade.get("max_loss_points", 0.0) or 0.0)
        profit_points = float(trade.get("max_profit_points", 0.0) or 0.0)
        if loss_points > 0:
            losses.append(loss_points)
        if profit_points > 0:
            profits.append(profit_points)

    if len(losses) < AUTO_RISK_MIN_TRADES or len(profits) < AUTO_RISK_MIN_TRADES:
        return None

    loss_series = pd.Series(losses)
    profit_series = pd.Series(profits)

    stop_loss_points = float(loss_series.quantile(stop_loss_quantile))
    profit_trigger_points = float(profit_series.quantile(profit_trigger_quantile))

    stop_loss_points = max(AUTO_RISK_MIN_POINTS, stop_loss_points)
    profit_trigger_points = max(stop_loss_points * 0.8, AUTO_RISK_MIN_POINTS, profit_trigger_points)
    trailing_gap_points = max(AUTO_RISK_MIN_POINTS, stop_loss_points * trailing_ratio)

    return {
        "stop_loss_points": round(stop_loss_points, 2),
        "profit_trigger_points": round(profit_trigger_points, 2),
        "trailing_gap_points": round(trailing_gap_points, 2),
        "stop_loss_quantile": round(float(stop_loss_quantile), 2),
        "profit_trigger_quantile": round(float(profit_trigger_quantile), 2),
        "trailing_ratio": round(float(trailing_ratio), 2),
        "sample_trades": min(len(losses), len(profits)),
    }


def calculate_ma_trend_engulfing_signals(df, min_bars=25, session="日盤", is_realtime=False):
    """
    計算 MA 趨勢觸及吞噬策略信號

     規則：
     1. 趨勢判斷：MA20 與 MA60 同方向，且 MA20 與 MA60 呈現多空排列
         - 多頭：MA20_slope > 0、MA60_slope > 0 且 MA20 > MA60
         - 空頭：MA20_slope < 0、MA60_slope < 0 且 MA20 < MA60
     2. 進場：第 N 根 K 棒觸及 MA20，且第 N+1 根收盤吞噬前一根
         - 做多：趨勢向上 + N 根觸及 MA20 + N+1 收盤 > 前一根 max(Open, Close) 且 收盤 > 兩條 MA
         - 做空：趨勢向下 + N 根觸及 MA20 + N+1 收盤 < 前一根 min(Open, Close) 且 收盤 < 兩條 MA
     3. 停損 / 退場：
         - 多頭：若當前 K 棒 Low < min(前一根 Open, 前一根 Close) 視為停損出場
         - 空頭：若當前 K 棒 High > max(前一根 Open, 前一根 Close) 視為停損出場
         - 另外，出現反向吞噬時同樣視為出場訊號
     4. 收盤前 30 分鐘風控：
         - 每個交易時段（依 session）收盤前 30 分鐘內：
             • 不再產生新的進場訊號
             • 若仍有持倉，於觸及「距收盤 30 分鐘」的第一根 K 棒強制平倉

        輸出：
                trades: 交易紀錄
                add_events: 補單信號列表

        備註：
                - 為了回測方便，函數在「非即時模式」下會將最後仍未平倉的部位，
                    於資料集最後一根 K 棒視為以收盤價強制平倉（exit_reason = "最後一根收盤"）。
                - 在即時看盤模式（is_realtime=True）下，避免這種回測式強制平倉，
                    以免造成「最新一根同時出現進場與出場」的視覺混淆。
    """
    if df is None or len(df) < min_bars:
        return [], []

    df = df.copy()
    trades = []
    add_events = []

    # 確保有 MA20/MA60
    if "MA20" not in df.columns or "MA60" not in df.columns:
        df["MA20"] = df["Close"].rolling(window=20).mean()
        df["MA60"] = df["Close"].rolling(window=60).mean()

    # 計算 MA 斜率（用簡單差分表示趨勢）
    df["MA20_slope"] = df["MA20"].diff()
    df["MA60_slope"] = df["MA60"].diff()

    # 偵測是否 K 棒「碰到」MA（touch）
    df["touch_ma20"] = (df["Low"] <= df["MA20"]) & (df["MA20"] <= df["High"])
    df["touch_ma60"] = (df["Low"] <= df["MA60"]) & (df["MA60"] <= df["High"])

    position = None
    entry_idx = None
    entry_price = None
    bars_in_position = 0
    has_added = False

    # 輔助：依 session 判斷每根 K 棒距離收盤時間（Asia/Taipei）
    def minutes_to_session_close(ts):
        """計算該時間點距離當日該交易時段收盤還有幾分鐘（負值代表已過收盤）。"""
        if not hasattr(ts, "tzinfo") or ts.tzinfo is None:
            taipei_tz = pytz.timezone("Asia/Taipei")
            ts = taipei_tz.localize(ts)
        else:
            ts = ts.astimezone(pytz.timezone("Asia/Taipei"))

        day = ts.date()
        taipei_tz = pytz.timezone("Asia/Taipei")

        if session == "日盤":
            close_dt = taipei_tz.localize(datetime(day.year, day.month, day.day, 13, 45))
        elif session == "夜盤":
            # 夜盤收盤：次日 05:00
            next_day = day + timedelta(days=1)
            close_dt = taipei_tz.localize(datetime(next_day.year, next_day.month, next_day.day, 5, 0))
        else:
            # 全盤或其他：依時間自動判斷屬於哪個時段
            if ts.hour < 12:
                close_dt = taipei_tz.localize(datetime(day.year, day.month, day.day, 13, 45))
            else:
                next_day = day + timedelta(days=1)
                close_dt = taipei_tz.localize(datetime(next_day.year, next_day.month, next_day.day, 5, 0))

        delta = close_dt - ts
        return delta.total_seconds() / 60.0

    for i in range(1, len(df)):
        row_prev = df.iloc[i - 1]
        row_curr = df.iloc[i]

        # 計算距離收盤時間（分鐘）
        minutes_left = minutes_to_session_close(df.index[i])

        # 多空排列 + 斜率同向，過濾雜訊以提高勝率
        uptrend = (
            row_curr["MA20_slope"] > 0
            and row_curr["MA60_slope"] > 0
            and row_curr["MA20"] > row_curr["MA60"]
        )
        downtrend = (
            row_curr["MA20_slope"] < 0
            and row_curr["MA60_slope"] < 0
            and row_curr["MA20"] < row_curr["MA60"]
        )

        touch_ma20 = bool(row_prev["touch_ma20"])

        # 吞噬定義：
        # 多頭：收盤 > 前一根 max(Open, Close)
        # 空頭：收盤 < 前一根 min(Open, Close)
        prev_low_ref = min(row_prev["Open"], row_prev["Close"])
        prev_high_ref = max(row_prev["Open"], row_prev["Close"])
        engulf_up = row_curr["Close"] > prev_high_ref
        engulf_down = row_curr["Close"] < prev_low_ref

        # 進場限制：只適用當日訊號，跨日不計
        prev_date = df.index[i - 1].date()
        curr_date = df.index[i].date()
        same_day_signal = prev_date == curr_date

        # 收盤前 30 分鐘內：不再開新倉
        cutoff_reached = minutes_left <= 30

        if position is None:
            # 做多進場：多頭排列 + 前一根觸及 MA20 + 吞噬且收盤站上兩條 MA
            if (
                uptrend
                and touch_ma20
                and engulf_up
                and row_curr["Close"] > row_curr["MA20"]
                and row_curr["Close"] > row_curr["MA60"]
            ) and (not cutoff_reached) and same_day_signal:
                position = "LONG"
                entry_idx = i
                entry_price = row_curr["Close"]
                bars_in_position = 1
                has_added = False
            # 做空進場：空頭排列 + 前一根觸及 MA20 + 吞噬且收盤跌破兩條 MA
            elif (
                downtrend
                and touch_ma20
                and engulf_down
                and row_curr["Close"] < row_curr["MA20"]
                and row_curr["Close"] < row_curr["MA60"]
            ) and (not cutoff_reached) and same_day_signal:
                position = "SHORT"
                entry_idx = i
                entry_price = row_curr["Close"]
                bars_in_position = 1
                has_added = False
            continue

        # 已持倉
        bars_in_position += 1

        # 若已進入收盤前 30 分鐘，強制在第一根觸及時平倉
        if cutoff_reached and position is not None:
            exit_idx = i
            exit_price = row_curr["Close"]
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": (exit_price - entry_price) if position == "LONG" else (entry_price - exit_price),
                "exit_reason": "收盤前30分鐘強制平倉",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            continue

        # 停損 & 退場條件
        # 1) 多頭停損：當前收盤 < 前一根 min(Open, Close)
        if position == "LONG" and row_curr["Close"] < prev_low_ref:
            exit_idx = i
            exit_price = row_curr["Close"]
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": exit_price - entry_price,
                "exit_reason": "多頭停損(收盤跌破前一根實體低點)",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            continue

        # 2) 空頭停損：當前收盤 > 前一根 max(Open, Close)
        if position == "SHORT" and row_curr["Close"] > prev_high_ref:
            exit_idx = i
            exit_price = row_curr["Close"]
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": entry_price - exit_price,
                "exit_reason": "空頭停損(收盤突破前一根實體高點)",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            continue

        # 3) 反向吞噬出場（若尚未觸發停損）
        if position == "LONG" and engulf_down:
            exit_idx = i
            exit_price = row_curr["Close"]
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": exit_price - entry_price,
                "exit_reason": "多頭反向吞噬出場",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            continue

        if position == "SHORT" and engulf_up:
            exit_idx = i
            exit_price = row_curr["Close"]
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": entry_price - exit_price,
                "exit_reason": "空頭反向吞噬出場",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            continue

    # 若最後仍持倉，且為回測模式，強制以最後一根收盤退場
    # 即時模式 (is_realtime=True) 不執行此步驟，避免最新一根同時出現進/出場標記
    if (not is_realtime) and position is not None and entry_idx is not None:
        exit_idx = len(df) - 1
        exit_price = df.iloc[exit_idx]["Close"]
        trades.append({
            "entry_idx": entry_idx,
            "entry_ts": df.index[entry_idx],
            "entry_price": entry_price,
            "exit_idx": exit_idx,
            "exit_ts": df.index[exit_idx],
            "exit_price": exit_price,
            "direction": position,
            "bars_held": bars_in_position,
            "pnl": (exit_price - entry_price) if position == "LONG" else (entry_price - exit_price),
            "exit_reason": "最後一根收盤",
        })

    return trades, add_events


def calculate_ma60_ma100_support_resistance_signals(
    df,
    min_bars=105,
    session="日盤",
    is_realtime=False,
    auto_risk=True,
    risk_params=None,
    risk_config=None,
    _calibration_mode=False,
):
    """
    策略：MA60/MA100 撐壓進場。

        進場定義：
        - 關鍵K（第N根）觸及判斷採 buffer：Low-10 <= MA <= High+10
        - 多方：第N+1根 Close > 第N根 max(Open, Close)
    - 空方：第N+1根 Close < 第N根 min(Open, Close)
        - 關鍵K close 位置：
            多方需關鍵K close > 觸碰到的 MA；空方需關鍵K close < 觸碰到的 MA
    並搭配：MA60 斜率方向、跨日不計、收盤前30分鐘不開新倉。
    """
    if df is None or len(df) < min_bars:
        return [], []

    df = df.copy()
    trades = []
    add_events = []

    config = risk_config or {}
    active_risk_params = risk_params or config.get("_active_risk_params")
    stop_loss_quantile_cfg = float(config.get("stop_loss_quantile", AUTO_RISK_STOP_LOSS_QUANTILE))
    profit_trigger_quantile_cfg = float(config.get("profit_trigger_quantile", AUTO_RISK_PROFIT_TRIGGER_QUANTILE))
    trailing_ratio_cfg = float(config.get("trailing_ratio", AUTO_RISK_TRAILING_RATIO))
    min_ma60_slope_points = float(config.get("min_ma60_slope_points", 0.0) or 0.0)
    min_body_points = float(config.get("min_body_points", 0.0) or 0.0)
    min_body_atr_ratio = float(config.get("min_body_atr_ratio", 0.0) or 0.0)
    min_volume_ratio = float(config.get("min_volume_ratio", 0.0) or 0.0)
    entry_exclude_open_minutes = int(config.get("entry_exclude_open_minutes", 0) or 0)
    atr_window = int(config.get("atr_window", 14) or 14)
    volume_window = int(config.get("volume_window", 20) or 20)
    if auto_risk and (not _calibration_mode) and active_risk_params is None:
        calibrate_df = _select_recent_df_by_days(df, AUTO_RISK_MIN_LOOKBACK_DAYS)
        if calibrate_df is not None and len(calibrate_df) >= min_bars:
            base_trades, _ = calculate_ma60_ma100_support_resistance_signals(
                calibrate_df,
                min_bars=min_bars,
                session=session,
                is_realtime=False,
                auto_risk=False,
                risk_params=None,
                risk_config=risk_config,
                _calibration_mode=True,
            )
            active_risk_params = _derive_auto_risk_params_from_trades(
                base_trades,
                stop_loss_quantile=stop_loss_quantile_cfg,
                profit_trigger_quantile=profit_trigger_quantile_cfg,
                trailing_ratio=trailing_ratio_cfg,
            )
            if active_risk_params is not None:
                add_events.append({
                    "type": "auto_risk_params",
                    **active_risk_params,
                })

    if "MA60" not in df.columns:
        df["MA60"] = df["Close"].rolling(window=60).mean()
    if "MA100" not in df.columns:
        df["MA100"] = df["Close"].rolling(window=100).mean()
    df["MA60_slope"] = df["MA60"].diff()
    if min_body_atr_ratio > 0:
        prev_close = df["Close"].shift(1)
        true_range = pd.concat(
            [
                df["High"] - df["Low"],
                (df["High"] - prev_close).abs(),
                (df["Low"] - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        df["ATR_FILTER"] = true_range.rolling(window=atr_window).mean()
    if min_volume_ratio > 0:
        df["VOLUME_AVG_FILTER"] = df["Volume"].rolling(window=volume_window).mean().shift(1)

    position = None
    buffer_points = 10.0
    stop_loss_limit = float((active_risk_params or {}).get("stop_loss_points", 0.0) or 0.0)
    profit_trigger_limit = float((active_risk_params or {}).get("profit_trigger_points", 0.0) or 0.0)
    trailing_gap_limit = float((active_risk_params or {}).get("trailing_gap_points", 0.0) or 0.0)
    use_dynamic_risk = stop_loss_limit > 0 and profit_trigger_limit > 0 and trailing_gap_limit > 0

    entry_idx = None
    entry_price = None
    bars_in_position = 0
    best_profit_points = 0.0
    trailing_armed = False

    def _compute_trade_excursions(start_idx, end_idx, direction, base_entry_price):
        """計算單筆交易期間最大虧損/最大獲利（點）。"""
        if start_idx is None or end_idx is None:
            return 0.0, 0.0

        window = df.iloc[int(start_idx): int(end_idx) + 1]
        if window.empty:
            return 0.0, 0.0

        high_max = float(window["High"].max())
        low_min = float(window["Low"].min())
        entry_px = float(base_entry_price)

        if direction == "LONG":
            max_loss = max(0.0, entry_px - low_min)
            max_profit = max(0.0, high_max - entry_px)
        else:
            max_loss = max(0.0, high_max - entry_px)
            max_profit = max(0.0, entry_px - low_min)

        return float(max_loss), float(max_profit)

    def minutes_to_session_close(ts):
        if not hasattr(ts, "tzinfo") or ts.tzinfo is None:
            taipei_tz = pytz.timezone("Asia/Taipei")
            ts = taipei_tz.localize(ts)
        else:
            ts = ts.astimezone(pytz.timezone("Asia/Taipei"))

        day = ts.date()
        taipei_tz = pytz.timezone("Asia/Taipei")

        if session == "日盤":
            close_dt = taipei_tz.localize(datetime(day.year, day.month, day.day, 13, 45))
        elif session == "夜盤":
            next_day = day + timedelta(days=1)
            close_dt = taipei_tz.localize(datetime(next_day.year, next_day.month, next_day.day, 5, 0))
        else:
            if ts.hour < 12:
                close_dt = taipei_tz.localize(datetime(day.year, day.month, day.day, 13, 45))
            else:
                next_day = day + timedelta(days=1)
                close_dt = taipei_tz.localize(datetime(next_day.year, next_day.month, next_day.day, 5, 0))

        delta = close_dt - ts
        return delta.total_seconds() / 60.0

    def minutes_from_session_open(ts):
        if not hasattr(ts, "tzinfo") or ts.tzinfo is None:
            taipei_tz = pytz.timezone("Asia/Taipei")
            ts = taipei_tz.localize(ts)
        else:
            ts = ts.astimezone(pytz.timezone("Asia/Taipei"))

        day = ts.date()
        taipei_tz = pytz.timezone("Asia/Taipei")
        if session == "夜盤":
            if ts.hour < 12:
                open_day = day - timedelta(days=1)
            else:
                open_day = day
            open_dt = taipei_tz.localize(datetime(open_day.year, open_day.month, open_day.day, 15, 0))
        else:
            open_dt = taipei_tz.localize(datetime(day.year, day.month, day.day, 8, 45))
        return (ts - open_dt).total_seconds() / 60.0

    def passes_entry_filters(row_curr, minutes_from_open):
        body_points = abs(float(row_curr["Close"]) - float(row_curr["Open"]))

        if min_ma60_slope_points > 0 and abs(float(row_curr["MA60_slope"])) < min_ma60_slope_points:
            return False
        if min_body_points > 0 and body_points < min_body_points:
            return False
        if min_body_atr_ratio > 0:
            atr_value = row_curr.get("ATR_FILTER")
            if pd.isna(atr_value) or body_points < float(atr_value) * min_body_atr_ratio:
                return False
        if min_volume_ratio > 0:
            volume_avg = row_curr.get("VOLUME_AVG_FILTER")
            if pd.isna(volume_avg) or float(volume_avg) <= 0:
                return False
            if float(row_curr.get("Volume", 0) or 0) < float(volume_avg) * min_volume_ratio:
                return False
        if entry_exclude_open_minutes > 0 and minutes_from_open < entry_exclude_open_minutes:
            return False
        return True

    for i in range(1, len(df)):
        row_prev = df.iloc[i - 1]
        row_curr = df.iloc[i]

        if pd.isna(row_prev.get("MA60")) or pd.isna(row_curr.get("MA60")) or pd.isna(row_prev.get("MA100")):
            continue

        buffered_low = float(row_prev["Low"]) - buffer_points
        buffered_high = float(row_prev["High"]) + buffer_points

        key_touch_ma60 = bool(buffered_low <= row_prev["MA60"] <= buffered_high)
        key_touch_ma100 = bool(buffered_low <= row_prev["MA100"] <= buffered_high)

        # 新增條件：關鍵K open/close 相對於「觸碰到的 MA」位置
        key_close_long_valid = (
            (key_touch_ma60 and row_prev["Close"] > row_prev["MA60"] and row_prev["Open"] > row_prev["MA60"])
            or (key_touch_ma100 and row_prev["Close"] > row_prev["MA100"] and row_prev["Open"] > row_prev["MA100"])
        )
        key_close_short_valid = (
            (key_touch_ma60 and row_prev["Close"] < row_prev["MA60"] and row_prev["Open"] < row_prev["MA60"])
            or (key_touch_ma100 and row_prev["Close"] < row_prev["MA100"] and row_prev["Open"] < row_prev["MA100"])
        )
        ma60_up = (row_curr["MA60_slope"] > 0) and (row_curr["Close"] > row_curr["MA60"])
        ma60_down = (row_curr["MA60_slope"] < 0) and (row_curr["Close"] < row_curr["MA60"])

        engulf_up = row_curr["Close"] > max(row_prev["Open"], row_prev["Close"])
        engulf_down = row_curr["Close"] < min(row_prev["Open"], row_prev["Close"])

        prev_date = df.index[i - 1].date()
        curr_date = df.index[i].date()
        same_day_signal = prev_date == curr_date

        minutes_left = minutes_to_session_close(df.index[i])
        minutes_from_open = minutes_from_session_open(df.index[i])
        cutoff_reached = minutes_left <= 30

        if position is None:
            if (
                key_close_long_valid
                and ma60_up
                and engulf_up
                and same_day_signal
                and (not cutoff_reached)
                and passes_entry_filters(row_curr, minutes_from_open)
            ):
                position = "LONG"
                entry_idx = i
                entry_price = row_curr["Close"]
                bars_in_position = 1
                best_profit_points = 0.0
                trailing_armed = False
            elif (
                key_close_short_valid
                and ma60_down
                and engulf_down
                and same_day_signal
                and (not cutoff_reached)
                and passes_entry_filters(row_curr, minutes_from_open)
            ):
                position = "SHORT"
                entry_idx = i
                entry_price = row_curr["Close"]
                bars_in_position = 1
                best_profit_points = 0.0
                trailing_armed = False
            continue

        bars_in_position += 1

        current_high = float(row_curr["High"])
        current_low = float(row_curr["Low"])

        if position == "LONG":
            current_adverse_points = max(0.0, float(entry_price) - current_low)
            current_profit_points = max(0.0, current_high - float(entry_price))
        else:
            current_adverse_points = max(0.0, current_high - float(entry_price))
            current_profit_points = max(0.0, float(entry_price) - current_low)

        best_profit_points = max(best_profit_points, current_profit_points)

        if use_dynamic_risk:
            if current_adverse_points >= stop_loss_limit:
                exit_idx = i
                if position == "LONG":
                    exit_price = float(entry_price) - stop_loss_limit
                else:
                    exit_price = float(entry_price) + stop_loss_limit
                max_loss_points, max_profit_points = _compute_trade_excursions(entry_idx, exit_idx, position, entry_price)
                trades.append({
                    "entry_idx": entry_idx,
                    "entry_ts": df.index[entry_idx],
                    "entry_price": entry_price,
                    "exit_idx": exit_idx,
                    "exit_ts": df.index[exit_idx],
                    "exit_price": exit_price,
                    "direction": position,
                    "bars_held": bars_in_position,
                    "pnl": (exit_price - entry_price) if position == "LONG" else (entry_price - exit_price),
                    "max_loss_points": max_loss_points,
                    "max_profit_points": max_profit_points,
                    "exit_reason": f"動態風控停損({stop_loss_limit:.1f}點)",
                })
                position = None
                entry_idx = None
                entry_price = None
                bars_in_position = 0
                best_profit_points = 0.0
                trailing_armed = False
                continue

            if (not trailing_armed) and best_profit_points >= profit_trigger_limit:
                trailing_armed = True

            if trailing_armed and (best_profit_points - current_profit_points) >= trailing_gap_limit:
                exit_idx = i
                if position == "LONG":
                    exit_price = float(entry_price) + max(0.0, best_profit_points - trailing_gap_limit)
                else:
                    exit_price = float(entry_price) - max(0.0, best_profit_points - trailing_gap_limit)
                max_loss_points, max_profit_points = _compute_trade_excursions(entry_idx, exit_idx, position, entry_price)
                trades.append({
                    "entry_idx": entry_idx,
                    "entry_ts": df.index[entry_idx],
                    "entry_price": entry_price,
                    "exit_idx": exit_idx,
                    "exit_ts": df.index[exit_idx],
                    "exit_price": exit_price,
                    "direction": position,
                    "bars_held": bars_in_position,
                    "pnl": (exit_price - entry_price) if position == "LONG" else (entry_price - exit_price),
                    "max_loss_points": max_loss_points,
                    "max_profit_points": max_profit_points,
                    "exit_reason": f"動態停利回撤({trailing_gap_limit:.1f}點)",
                })
                position = None
                entry_idx = None
                entry_price = None
                bars_in_position = 0
                best_profit_points = 0.0
                trailing_armed = False
                continue

        if cutoff_reached:
            exit_idx = i
            exit_price = row_curr["Close"]
            max_loss_points, max_profit_points = _compute_trade_excursions(entry_idx, exit_idx, position, entry_price)
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": (exit_price - entry_price) if position == "LONG" else (entry_price - exit_price),
                "max_loss_points": max_loss_points,
                "max_profit_points": max_profit_points,
                "exit_reason": "收盤前30分鐘強制平倉",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            best_profit_points = 0.0
            trailing_armed = False
            continue

        prev_body_low = min(row_prev["Open"], row_prev["Close"])
        prev_body_high = max(row_prev["Open"], row_prev["Close"])

        # 依使用者規則：
        # 多方出場：當 N+1 收盤 < 第 N 根 min(Open, Close)
        # 空方出場：當 N+1 收盤 > 第 N 根 max(Open, Close)
        if position == "LONG" and row_curr["Close"] < prev_body_low:
            exit_idx = i
            exit_price = row_curr["Close"]
            max_loss_points, max_profit_points = _compute_trade_excursions(entry_idx, exit_idx, position, entry_price)
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": exit_price - entry_price,
                "max_loss_points": max_loss_points,
                "max_profit_points": max_profit_points,
                "exit_reason": "多方出場(N+1收盤<前一根實體低點)",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            best_profit_points = 0.0
            trailing_armed = False
            continue

        if position == "SHORT" and row_curr["Close"] > prev_body_high:
            exit_idx = i
            exit_price = row_curr["Close"]
            max_loss_points, max_profit_points = _compute_trade_excursions(entry_idx, exit_idx, position, entry_price)
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": entry_price - exit_price,
                "max_loss_points": max_loss_points,
                "max_profit_points": max_profit_points,
                "exit_reason": "空方出場(N+1收盤>前一根實體高點)",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            best_profit_points = 0.0
            trailing_armed = False
            continue

    if (not is_realtime) and position is not None and entry_idx is not None:
        exit_idx = len(df) - 1
        exit_price = df.iloc[exit_idx]["Close"]
        max_loss_points, max_profit_points = _compute_trade_excursions(entry_idx, exit_idx, position, entry_price)
        trades.append({
            "entry_idx": entry_idx,
            "entry_ts": df.index[entry_idx],
            "entry_price": entry_price,
            "exit_idx": exit_idx,
            "exit_ts": df.index[exit_idx],
            "exit_price": exit_price,
            "direction": position,
            "bars_held": bars_in_position,
            "pnl": (exit_price - entry_price) if position == "LONG" else (entry_price - exit_price),
            "max_loss_points": max_loss_points,
            "max_profit_points": max_profit_points,
            "exit_reason": "最後一根收盤",
        })

    return trades, add_events


def _build_daily_bars_from_intraday(df: pd.DataFrame) -> pd.DataFrame:
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


def _compute_trade_excursions_from_df(df, start_idx, end_idx, direction, base_entry_price):
    if start_idx is None or end_idx is None:
        return 0.0, 0.0

    window = df.iloc[int(start_idx): int(end_idx) + 1]
    if window.empty:
        return 0.0, 0.0

    high_max = float(window["High"].max())
    low_min = float(window["Low"].min())
    entry_px = float(base_entry_price)

    if direction == "LONG":
        max_loss = max(0.0, entry_px - low_min)
        max_profit = max(0.0, high_max - entry_px)
    else:
        max_loss = max(0.0, high_max - entry_px)
        max_profit = max(0.0, entry_px - low_min)

    return float(max_loss), float(max_profit)


def calculate_masa_bottom_pullback_signals(
    df,
    min_bars=600,
    session="日盤",
    is_realtime=False,
    risk_config=None,
):
    """
    麻紗老師底部拉回 v1：日K找破底翻，5分K回測支撐後進出場。

    使用方式：
    - 請用 5m K 資料執行此策略。
    - 程式會由 5m 自動彙整日K，前一日收盤形成破底翻 setup 後，
      下一交易日用 5m 等回測與重新站回再進場。
    """
    if df is None or len(df) < min_bars or not isinstance(df.index, pd.DatetimeIndex):
        return [], []

    config = risk_config or {}
    box_lookback_bars = int(config.get("masa_box_lookback_bars", 20) or 20)
    box_min_bars = int(config.get("masa_box_min_bars", 8) or 8)
    box_low_quantile = float(config.get("masa_box_low_quantile", 0.25) or 0.25)
    box_max_range_points = float(config.get("masa_box_max_range_points", 0.0) or 0.0)
    break_buffer = float(config.get("masa_break_buffer_points", 0.0) or 0.0)
    reclaim_buffer = float(config.get("masa_reclaim_buffer_points", 0.0) or 0.0)
    open_low_tolerance = float(config.get("masa_open_low_tolerance_points", 0.0) or 0.0)
    support_touch_buffer = float(config.get("masa_support_touch_buffer_points", 5.0) or 5.0)
    confirm_buffer = float(config.get("masa_confirm_buffer_points", 0.0) or 0.0)
    stop_buffer = float(config.get("masa_stop_buffer_points", 0.0) or 0.0)
    support_fail_buffer = float(config.get("masa_support_fail_buffer_points", 0.0) or 0.0)
    take_profit_points = float(config.get("masa_take_profit_points", 0.0) or 0.0)
    max_hold_bars = int(config.get("masa_max_hold_bars", 240) or 240)
    max_entry_bars = int(config.get("masa_max_entry_bars", 60) or 60)
    allow_same_day_exit = bool(config.get("masa_allow_same_day_exit", True))

    df = df.copy()
    daily = _build_daily_bars_from_intraday(df)
    if len(daily) < box_lookback_bars + 2:
        return [], []

    intraday_dates = pd.Series(df.index.date, index=df.index)
    unique_dates = list(pd.unique(intraday_dates))
    date_to_position = {date_value: idx for idx, date_value in enumerate(unique_dates)}
    trades = []
    add_events = []
    next_available_idx = 0

    for setup_daily_idx in range(box_lookback_bars, len(daily) - 1):
        setup_date = daily.index[setup_daily_idx].date()
        if setup_date not in date_to_position:
            continue

        prior = daily.iloc[setup_daily_idx - box_lookback_bars: setup_daily_idx]
        if len(prior) < box_min_bars:
            continue

        body_lows = prior[["Open", "Close"]].min(axis=1)
        body_highs = prior[["Open", "Close"]].max(axis=1)
        box_low = float(body_lows.quantile(box_low_quantile))
        box_high = float(max(prior["High"].max(), body_highs.quantile(0.75)))
        if box_high <= box_low:
            continue
        if box_max_range_points > 0 and (box_high - box_low) > box_max_range_points:
            continue

        setup_bar = daily.iloc[setup_daily_idx]
        breakdown = float(setup_bar["Low"]) < box_low - break_buffer
        reclaim = float(setup_bar["Close"]) > box_low + reclaim_buffer
        if not (breakdown and reclaim):
            continue

        next_daily_date = daily.index[setup_daily_idx + 1].date()
        if next_daily_date not in date_to_position:
            continue

        entry_mask = intraday_dates == next_daily_date
        entry_day_df = df[entry_mask]
        if entry_day_df.empty:
            continue

        day_start_idx = df.index.get_loc(entry_day_df.index[0])
        if day_start_idx < next_available_idx:
            continue

        next_open = float(entry_day_df.iloc[0]["Open"])
        setup_close = float(setup_bar["Close"])
        if next_open < setup_close - open_low_tolerance:
            add_events.append({
                "type": "masa_skip_open_low",
                "setup_date": setup_date,
                "next_date": next_daily_date,
                "next_open": next_open,
                "setup_close": setup_close,
            })
            continue

        entry_support = max(box_low, min(next_open, setup_close))
        stop_reference = box_low
        touched_support = False
        entry_idx = None
        entry_price = None

        for local_i, (_, row) in enumerate(entry_day_df.iterrows()):
            global_i = day_start_idx + local_i
            if local_i >= max_entry_bars:
                break

            if float(row["Low"]) < stop_reference - stop_buffer:
                break
            if float(row["Low"]) <= entry_support + support_touch_buffer:
                touched_support = True
            if touched_support and float(row["Close"]) >= entry_support + confirm_buffer:
                entry_idx = global_i
                entry_price = float(row["Close"])
                break

        if entry_idx is None:
            continue

        exit_idx = None
        exit_price = None
        exit_reason = ""
        max_exit_idx = min(len(df) - 1, entry_idx + max_hold_bars)

        for i in range(entry_idx + 1, max_exit_idx + 1):
            row = df.iloc[i]
            if (not allow_same_day_exit) and df.index[i].date() == df.index[entry_idx].date():
                continue

            if float(row["Low"]) < stop_reference - stop_buffer:
                exit_idx = i
                exit_price = stop_reference - stop_buffer
                exit_reason = "麻紗破底翻停損：再次跌破整理區底"
                break
            if float(row["Close"]) < entry_support - support_fail_buffer:
                exit_idx = i
                exit_price = float(row["Close"])
                exit_reason = "麻紗破底翻停損：跌回回測支撐下方"
                break
            if take_profit_points > 0 and float(row["High"]) >= float(entry_price) + take_profit_points:
                exit_idx = i
                exit_price = float(entry_price) + take_profit_points
                exit_reason = f"麻紗破底翻停利：固定{take_profit_points:.0f}點"
                break

        if exit_idx is None:
            if is_realtime and max_exit_idx == len(df) - 1:
                continue
            exit_idx = max_exit_idx
            exit_price = float(df.iloc[exit_idx]["Close"])
            exit_reason = "麻紗破底翻出場：持有時間到"

        max_loss_points, max_profit_points = _compute_trade_excursions_from_df(
            df, entry_idx, exit_idx, "LONG", entry_price
        )
        trades.append({
            "entry_idx": entry_idx,
            "entry_ts": df.index[entry_idx],
            "entry_price": entry_price,
            "exit_idx": exit_idx,
            "exit_ts": df.index[exit_idx],
            "exit_price": exit_price,
            "direction": "LONG",
            "bars_held": int(exit_idx - entry_idx + 1),
            "pnl": float(exit_price - entry_price),
            "max_loss_points": max_loss_points,
            "max_profit_points": max_profit_points,
            "exit_reason": exit_reason,
            "box_high": box_high,
            "box_low": box_low,
            "breakdown_low": float(setup_bar["Low"]),
            "setup_close": setup_close,
            "next_open": next_open,
            "entry_support": entry_support,
            "strategy_note": "日K破底翻，隔日5分K回測支撐後站回進場",
        })
        next_available_idx = exit_idx + 1

    return trades, add_events


def run_selected_strategy(df, strategy="strategy1", session="日盤", is_realtime=False, risk_config=None):
    """執行單一或多個策略，回傳合併後交易訊號。"""
    registry = get_strategy_registry()
    selected_keys = _normalize_strategy_keys(strategy)

    all_trades = []
    all_add_events = []

    for strategy_key in selected_keys:
        strategy_info = registry.get(strategy_key)
        if strategy_info is None:
            continue

        strategy_func = strategy_info["func"]
        strategy_name = strategy_info["name"]
        trades, add_events = strategy_func(
            df,
            session=session,
            is_realtime=is_realtime,
            risk_config=risk_config,
        )

        for trade in trades:
            trade_copy = dict(trade)
            trade_copy["strategy_key"] = strategy_key
            trade_copy["strategy_name"] = strategy_name
            all_trades.append(trade_copy)
        all_add_events.extend(add_events)

    # 去除重複（多策略組合時避免相同交易重複顯示）
    dedup_map = {}
    for trade in all_trades:
        dedup_key = (
            trade.get("entry_idx"),
            trade.get("exit_idx"),
            trade.get("direction"),
            float(trade.get("entry_price", 0)),
            float(trade.get("exit_price", 0)),
        )
        if dedup_key not in dedup_map:
            dedup_map[dedup_key] = trade

    merged_trades = list(dedup_map.values())
    merged_trades.sort(key=lambda item: (item.get("entry_idx", -1), item.get("exit_idx", -1)))

    return merged_trades, all_add_events


def calculate_strategy1_signals(df, min_bars=105, session="日盤", is_realtime=False):
    """相容舊名稱，實際委派至 MA60/MA100 撐壓進場。"""
    return calculate_ma60_ma100_support_resistance_signals(
        df,
        min_bars=min_bars,
        session=session,
        is_realtime=is_realtime,
    )


def calculate_ma60_key_engulfing_signals(df, min_bars=105, session="日盤", is_realtime=False):
    """相容舊名稱，實際委派至新策略1。"""
    return calculate_ma60_ma100_support_resistance_signals(df, min_bars=min_bars, session=session, is_realtime=is_realtime)
