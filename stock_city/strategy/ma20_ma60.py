from datetime import datetime, timedelta
import pytz
import pandas as pd


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


def calculate_ma60_key_engulfing_signals(df, min_bars=105, session="日盤", is_realtime=False):
    """
    策略2：MA60/MA100 關鍵K吞噬策略。

        進場定義：
        - 關鍵K（第N根）觸及判斷採 buffer：Low-10 <= MA <= High+10
    - 多方：第N+1根 max(Open, Close) > 關鍵K close
    - 空方：第N+1根 min(Open, Close) < 關鍵K close
        - 關鍵K close 位置：
            多方需關鍵K close > 觸碰到的 MA；空方需關鍵K close < 觸碰到的 MA
    並搭配：MA60 斜率方向、跨日不計、收盤前30分鐘不開新倉。
    """
    if df is None or len(df) < min_bars:
        return [], []

    df = df.copy()
    trades = []
    add_events = []

    if "MA60" not in df.columns:
        df["MA60"] = df["Close"].rolling(window=60).mean()
    if "MA100" not in df.columns:
        df["MA100"] = df["Close"].rolling(window=100).mean()
    df["MA60_slope"] = df["MA60"].diff()

    position = None
    buffer_points = 10.0

    entry_idx = None
    entry_price = None
    bars_in_position = 0

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

        engulf_up = max(row_curr["Open"], row_curr["Close"]) > row_prev["Close"]
        engulf_down = min(row_curr["Open"], row_curr["Close"]) < row_prev["Close"]

        prev_date = df.index[i - 1].date()
        curr_date = df.index[i].date()
        same_day_signal = prev_date == curr_date

        minutes_left = minutes_to_session_close(df.index[i])
        cutoff_reached = minutes_left <= 30

        if position is None:
            if key_close_long_valid and ma60_up and engulf_up and same_day_signal and (not cutoff_reached):
                position = "LONG"
                entry_idx = i
                entry_price = row_curr["Close"]
                bars_in_position = 1
            elif key_close_short_valid and ma60_down and engulf_down and same_day_signal and (not cutoff_reached):
                position = "SHORT"
                entry_idx = i
                entry_price = row_curr["Close"]
                bars_in_position = 1
            continue

        bars_in_position += 1

        if cutoff_reached:
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

        prev_body_low = min(row_prev["Open"], row_prev["Close"])
        prev_body_high = max(row_prev["Open"], row_prev["Close"])

        # 依使用者規則：
        # 多方出場：當 N+1 收盤 < 第 N 根 min(Open, Close)
        # 空方出場：當 N+1 收盤 > 第 N 根 max(Open, Close)
        if position == "LONG" and row_curr["Close"] < prev_body_low:
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
                "exit_reason": "多方出場(N+1收盤<前一根實體低點)",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            continue

        if position == "SHORT" and row_curr["Close"] > prev_body_high:
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
                "exit_reason": "空方出場(N+1收盤>前一根實體高點)",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            continue

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


def run_selected_strategy(df, strategy="strategy1", session="日盤", is_realtime=False):
    """依策略代碼執行：strategy1 或 strategy2。"""
    strategy_key = str(strategy).strip().lower()
    if strategy_key in ("2", "strategy2", "ma60", "ma60_key"):
        return calculate_ma60_key_engulfing_signals(df, session=session, is_realtime=is_realtime)
    return calculate_ma_trend_engulfing_signals(df, session=session, is_realtime=is_realtime)
